"""Restock: dependency-free HTTP service with a persistent SQLite outbox."""
import base64
import hashlib
import hmac
import html
import json
import logging
import os
import re
import secrets
import signal
import sqlite3
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

DB_PATH = os.getenv('DB_PATH', 'data/restock.sqlite3')
STOCK_URL = os.getenv('STOCK_API_URL', '').rstrip('/')
STOCK_KEY = os.getenv('STOCK_API_KEY', '')
RESEND_URL = os.getenv('RESEND_BASE_URL', '').rstrip('/')
RESEND_KEY = os.getenv('RESEND_API_KEY', '')
WEBHOOK_SECRET = os.getenv('WEBHOOK_SECRET', '')
POLL_SECONDS = float(os.getenv('POLL_SECONDS', '10'))
STOP = threading.Event()
WAKE = threading.Event()
EMAIL_RE = re.compile(r'^[^\s@<>]+@[^\s@<>]+\.[^\s@<>]+$')
SKU_RE = re.compile(r'^[A-Za-z0-9][A-Za-z0-9_.-]{0,119}$')
log = logging.getLogger('restock')


def connect():
    db = sqlite3.connect(DB_PATH, timeout=15)
    db.row_factory = sqlite3.Row
    db.execute('PRAGMA busy_timeout=15000')
    db.execute('PRAGMA foreign_keys=ON')
    return db


def initialize():
    Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
    with connect() as db:
        db.execute('PRAGMA journal_mode=WAL')
        db.executescript('''
        CREATE TABLE IF NOT EXISTS requests (
            id TEXT PRIMARY KEY, email TEXT NOT NULL, sku TEXT NOT NULL,
            created_at REAL NOT NULL, notified_at REAL, status TEXT NOT NULL DEFAULT 'waiting',
            attempts INTEGER NOT NULL DEFAULT 0, next_attempt REAL NOT NULL DEFAULT 0,
            first_attempt REAL, lease_until REAL NOT NULL DEFAULT 0, provider_id TEXT,
            last_error TEXT);
        CREATE UNIQUE INDEX IF NOT EXISTS one_open ON requests(email, sku) WHERE notified_at IS NULL;
        CREATE INDEX IF NOT EXISTS due ON requests(next_attempt, lease_until) WHERE notified_at IS NULL;
        CREATE TABLE IF NOT EXISTS events (id TEXT PRIMARY KEY, digest TEXT NOT NULL, received_at REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT NOT NULL);
        ''')
        existing = db.execute("SELECT value FROM settings WHERE key='admin_password'").fetchone()
        if not existing:
            password = os.getenv('ADMIN_PASSWORD') or secrets.token_urlsafe(24)
            db.execute("INSERT INTO settings VALUES ('admin_password',?)", (password,))
            if not os.getenv('ADMIN_PASSWORD'):
                log.warning('Generated admin password (username admin): %s', password)


def api_json(url, key, payload=None, idem=None):
    headers = {'Authorization': 'Bearer ' + key, 'Accept': 'application/json'}
    if idem:
        headers['Idempotency-Key'] = idem
    data = None
    if payload is not None:
        data = json.dumps(payload).encode()
        headers['Content-Type'] = 'application/json'
    req = urllib.request.Request(url, data=data, headers=headers)
    with urllib.request.urlopen(req, timeout=6) as response:
        return json.loads(response.read(1_000_000))


def stock(sku):
    if not STOCK_URL or not STOCK_KEY:
        raise RuntimeError('Stock service not configured')
    result = api_json(STOCK_URL + '/' + urllib.parse.quote(sku, safe=''), STOCK_KEY)
    if not isinstance(result, dict):
        raise ValueError('Invalid stock response')
    quantity = result.get('quantity')
    if not isinstance(quantity, (int, float)) or isinstance(quantity, bool):
        raise ValueError('Missing stock quantity')
    return quantity > 0 and result.get('in_stock', True) is not False


def email_payload(row):
    sku = html.escape(row['sku'])
    return {'from': os.getenv('FROM_EMAIL', 'Restock <alerts@example.com>'), 'to': [row['email']],
            'subject': row['sku'] + ' is back in stock',
            'text': f"Good news! {row['sku']} is back in stock. Visit the store to check availability. This is your one-time stock alert.",
            'html': f'''<!doctype html><html><body style="margin:0;background:#f2f0e9;font-family:Arial,sans-serif;color:#242920;padding:28px 12px"><table role="presentation" style="max-width:520px;width:100%;margin:auto;background:white;border-collapse:collapse"><tr><td style="padding:32px;border-bottom:1px solid #e4e6df;font-size:22px;font-weight:bold">restock<span style="color:#547442">.</span></td></tr><tr><td style="padding:38px 32px"><p style="font-size:11px;letter-spacing:2px;color:#5f7654">THE WAIT IS OVER</p><h1 style="font-size:36px;line-height:1.1;font-weight:normal">A little good news<br>for your inbox.</h1><p style="font-size:16px;line-height:1.8"><strong>{sku}</strong> is available again.</p><p style="font-size:14px;line-height:1.8;color:#66705f">Visit the store to check current availability. Stock can change, so your alert is not a reservation.</p></td></tr><tr><td style="padding:24px 32px;background:#eaf0e4;font-size:12px;line-height:1.7;color:#526249">You requested this one-time stock alert. You won't receive another email for this request.</td></tr></table></body></html>'''}


def claim_request():
    now = time.time()
    with connect() as db:
        db.execute('BEGIN IMMEDIATE')
        row = db.execute("SELECT * FROM requests WHERE notified_at IS NULL AND status != 'needs_review' AND next_attempt<=? AND lease_until<=? ORDER BY next_attempt,created_at LIMIT 1", (now, now)).fetchone()
        if row:
            db.execute('UPDATE requests SET lease_until=? WHERE id=?', (now + 20, row['id']))
        return dict(row) if row else None


def process_one(row):
    now = time.time()
    try:
        # The stock API is authoritative; webhook quantities never cause an email alone.
        if not stock(row['sku']):
            with connect() as db:
                db.execute("UPDATE requests SET status='waiting',lease_until=0,next_attempt=?,last_error=NULL WHERE id=?", (time.time() + POLL_SECONDS, row['id']))
            return
        if row['first_attempt'] and now - row['first_attempt'] >= 23 * 3600:
            with connect() as db:
                db.execute("UPDATE requests SET status='needs_review',last_error='Delivery uncertain beyond safe idempotency window',lease_until=0 WHERE id=?", (row['id'],))
            return
        if not RESEND_URL or not RESEND_KEY:
            raise RuntimeError('Email service not configured')
        # Persist BEFORE network I/O, including before any uncertain provider outcome.
        with connect() as db:
            db.execute("UPDATE requests SET status='sending',attempts=attempts+1,first_attempt=COALESCE(first_attempt,?) WHERE id=?", (now, row['id']))
        url = RESEND_URL if RESEND_URL.endswith('/emails') else RESEND_URL + '/emails'
        result = api_json(url, RESEND_KEY, email_payload(row), idem='restock/' + row['id'])
        if not isinstance(result, dict) or not result.get('id'):
            raise ValueError('Email provider did not acknowledge a message id')
        with connect() as db:
            db.execute("UPDATE requests SET status='notified',notified_at=?,provider_id=?,lease_until=0,last_error=NULL WHERE id=?", (time.time(), str(result['id']), row['id']))
    except Exception as exc:
        # Do not store error bodies: they can contain credentials or personal data.
        error = 'HTTP ' + str(exc.code) if isinstance(exc, urllib.error.HTTPError) else type(exc).__name__
        delay = min(60, 2 ** min(row['attempts'] + 1, 6))
        with connect() as db:
            db.execute("UPDATE requests SET status='retrying',lease_until=0,next_attempt=?,last_error=? WHERE id=?", (time.time() + delay, error, row['id']))


def worker():
    while not STOP.is_set():
        try:
            row = claim_request()
            if row:
                process_one(row)
                continue
        except Exception:
            log.exception('Worker cycle failed')
        WAKE.wait(0.3)
        WAKE.clear()


STYLE = '''*{box-sizing:border-box}body{margin:0;background:#f3f2ec;color:#273023;font:14px system-ui,sans-serif}a{color:inherit}header{padding:24px 5%;display:flex;justify-content:space-between;border-bottom:1px solid #d7dcce;align-items:center}.brand{font-size:26px;font-weight:700;letter-spacing:-1.5px;text-decoration:none}.brand span{color:#668456}small{font-size:10px;letter-spacing:1px;color:#68745e}main{max-width:1200px;margin:auto;padding:50px 5%}h1{font:normal 58px Georgia,serif;letter-spacing:-2px;line-height:1.05;margin:20px 0}p{line-height:1.8;color:#68745e}.layout{display:grid;grid-template-columns:1fr 1fr;gap:9%;align-items:center}.art{height:360px;background:#dfe6d4;display:grid;place-items:center;position:relative;overflow:hidden}.art:before{content:"";width:230px;height:230px;border:1px solid #a4b595;border-radius:50%;position:absolute}.envelope{font:80px Georgia,serif;transform:rotate(-10deg);color:#47603b;z-index:1;background:#f6f8ef;border:1px solid #bac7af;padding:15px 35px;box-shadow:5px 15px 30px #58694722}form{margin-top:26px}label{font-size:12px;display:block;margin:18px 0 8px}input,select{background:#fff;border:1px solid #b8c3aa;border-radius:4px;padding:14px;font:inherit;width:100%;min-width:0}input:focus,select:focus{outline:2px solid #547442;outline-offset:2px}button{width:100%;padding:15px;background:#344d2a;color:white;border:0;border-radius:4px;margin-top:20px;cursor:pointer;font:inherit}button:disabled{opacity:.5}.message{padding:15px;background:#e0ead7;margin-top:18px;line-height:1.6}.error{background:#f4e0d9}.caption{font-size:11px;border-top:1px solid #d7dcce;margin-top:26px;padding-top:18px}table{width:100%;border-collapse:collapse;font-size:12px}th,td{text-align:left;padding:15px 10px;border-bottom:1px solid #d7dcce}th{color:#68745e;font-weight:500}.stats{display:grid;grid-template-columns:repeat(3,1fr);gap:12px;margin:30px 0}.stat{padding:22px;background:#e3eadb;border:1px solid #c8d4bd}.stat strong{font:30px Georgia,serif;display:block}.pill{padding:5px 8px;border:1px solid #b8c3aa;border-radius:20px;white-space:nowrap}.tablewrap{overflow:auto}footer{padding:25px 5%;font-size:10px;color:#68745e;border-top:1px solid #d7dcce}@media(max-width:650px){.layout{grid-template-columns:1fr;gap:35px}h1{font-size:44px}.art{height:220px;grid-row:1}.stats{gap:6px}.stat{padding:15px}main{padding:30px 6%}header{padding:20px 6%}}'''


def page(title, body, admin=False):
    nav = '<a href="/embed">Open signup form ↗</a>' if admin else '<small>ONE REQUEST. ONE GOOD UPDATE.</small>'
    return '<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>' + title + '</title><style>' + STYLE + '</style></head><body><header><a class="brand" href="/">restock<span>.</span></a>' + nav + '</header><main>' + body + '</main><footer>Restock · One-time availability notifications · No reservation or purchase is made.</footer></body></html>'


class Handler(BaseHTTPRequestHandler):
    server_version = 'Restock'

    def log_message(self, *_):
        pass  # Avoid recording email addresses, credentials, or request bodies.

    def send(self, code, body, kind='application/json', extra=None):
        raw = body.encode() if isinstance(body, str) else body
        self.send_response(code)
        self.send_header('Content-Type', kind + '; charset=utf-8')
        self.send_header('Content-Length', str(len(raw)))
        self.send_header('X-Content-Type-Options', 'nosniff')
        self.send_header('Referrer-Policy', 'no-referrer')
        self.send_header('Cache-Control', 'no-store')
        self.send_header('Content-Security-Policy', "default-src 'self'; style-src 'unsafe-inline'; script-src 'unsafe-inline'; object-src 'none'; base-uri 'none'" + ("; frame-ancestors 'none'" if self.path.startswith('/admin') else ''))
        for key, value in (extra or {}).items():
            self.send_header(key, value)
        self.end_headers()
        self.wfile.write(raw)

    def json(self, code, value, extra=None):
        self.send(code, json.dumps(value), extra=extra)

    def admin_ok(self):
        try:
            raw = self.headers.get('Authorization', '')
            credentials = base64.b64decode(raw.removeprefix('Basic '), validate=True).decode()
            user, password = credentials.split(':', 1)
            with connect() as db:
                stored = db.execute("SELECT value FROM settings WHERE key='admin_password'").fetchone()['value']
            expected = os.getenv('ADMIN_PASSWORD') or stored
            return hmac.compare_digest(user, 'admin') and hmac.compare_digest(password, expected)
        except Exception:
            return False

    def do_GET(self):
        path = urllib.parse.urlsplit(self.path).path
        if path == '/health':
            with connect() as db:
                db.execute('SELECT 1')
            return self.json(200, {'ok': True})
        if path in ('/', '/embed'):
            body = '''<section class="layout"><div><small>BACK IN STOCK ALERTS</small><h1>Worth the wait.<br>First to know.</h1><p>Leave your email and the item code. We'll send one notification when it's available again.</p><form id="signup"><label for="sku">Item code / SKU</label><input id="sku" name="sku" placeholder="e.g. TEE-ORG-WHT-M" maxlength="120" required pattern="[A-Za-z0-9][A-Za-z0-9_.-]*"><label for="email">Email address</label><input id="email" name="email" type="email" placeholder="you@example.com" maxlength="254" required><button id="submit">Let me know when it's back ↗</button></form><div id="message" role="status" hidden></div><p class="caption">One-time stock alert only. Availability can change; an alert does not reserve the item.</p></div><div class="art" aria-hidden="true"><span class="envelope">✉</span></div></section><script>const q=new URLSearchParams(location.search);document.querySelector('#sku').value=q.get('sku')||'';document.querySelector('#signup').onsubmit=async e=>{e.preventDefault();const b=document.querySelector('#submit'),m=document.querySelector('#message');b.disabled=true;m.hidden=true;try{const r=await fetch('/api/requests',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({email:document.querySelector('#email').value,sku:document.querySelector('#sku').value})});const d=await r.json();m.className='message'+(r.ok?'':' error');m.textContent=r.ok?(d.duplicate?'You’re already on the list for this item.':'You’re on the list. We’ll email you when it’s available.'):(d.error||'Please try again.');}catch{m.className='message error';m.textContent='We couldn’t confirm your request. Please try again; duplicate signups are safe.'}finally{m.hidden=false;b.disabled=false}};</script>'''
            return self.send(200, page('Restock — Worth the wait', body), 'text/html')
        if path == '/admin':
            if not self.admin_ok():
                return self.json(401, {'error': 'Authentication required'}, {'WWW-Authenticate': 'Basic realm="Restock admin", charset="UTF-8"'})
            with connect() as db:
                rows = db.execute('SELECT * FROM requests ORDER BY created_at DESC LIMIT 500').fetchall()
                counts = dict(db.execute('SELECT status,count(*) FROM requests GROUP BY status').fetchall())
            def date(value):
                return time.strftime('%Y-%m-%d %H:%M UTC', time.gmtime(value)) if value else '—'
            rendered = ''.join('<tr><td>' + html.escape(r['email']) + '</td><td>' + html.escape(r['sku']) + '</td><td><span class="pill">' + html.escape(r['status']) + '</span></td><td>' + date(r['created_at']) + '</td><td>' + date(r['notified_at']) + '</td><td>' + str(r['attempts']) + '</td><td>' + html.escape(r['last_error'] or '—') + '</td></tr>' for r in rows)
            body = '<small>OPERATIONS / LIVE REQUESTS</small><h1>A quieter kind of queue.</h1><p>Availability requests and their delivery history. Refresh to see the latest state.</p><div class="stats">' + ''.join('<div class="stat"><strong>' + str(n) + '</strong>' + label + '</div>' for label, n in [('Waiting / retrying', sum(v for k, v in counts.items() if k not in ('notified', 'needs_review'))), ('Notified', counts.get('notified', 0)), ('Needs review', counts.get('needs_review', 0))]) + '</div><div class="tablewrap"><table><thead><tr><th>Email</th><th>Item</th><th>Status</th><th>Requested</th><th>Notified</th><th>Attempts</th><th>Last error</th></tr></thead><tbody>' + (rendered or '<tr><td colspan="7">No requests yet. Your next customer starts the story.</td></tr>') + '</tbody></table></div><p class="caption">Showing the latest 500 requests. Durable data is retained in SQLite.</p>'
            return self.send(200, page('Restock — Admin', body, True), 'text/html')
        self.json(404, {'error': 'Not found'})

    def do_POST(self):
        path = urllib.parse.urlsplit(self.path).path
        if path not in ('/api/requests', '/webhooks/stock'):
            return self.json(404, {'error': 'Not found'})
        if path == '/webhooks/stock' and (not WEBHOOK_SECRET or not hmac.compare_digest(self.headers.get('X-Webhook-Secret', ''), WEBHOOK_SECRET)):
            return self.json(401, {'error': 'Unauthorized'})
        try:
            size = int(self.headers.get('Content-Length', '0'))
            if size <= 0 or size > 8192:
                return self.json(413, {'error': 'Body must be between 1 and 8192 bytes'})
            if self.headers.get('Content-Type', '').split(';')[0].strip() != 'application/json':
                return self.json(415, {'error': 'Use application/json'})
            data = json.loads(self.rfile.read(size))
            if not isinstance(data, dict):
                raise ValueError()
            sku = data.get('sku')
            if not isinstance(sku, str) or not SKU_RE.fullmatch(sku):
                return self.json(400, {'error': 'A valid SKU is required'})
            if path == '/api/requests':
                address = data.get('email')
                if not isinstance(address, str):
                    return self.json(400, {'error': 'A valid email address is required'})
                address = address.strip().lower()
                if len(address) > 254 or not EMAIL_RE.fullmatch(address):
                    return self.json(400, {'error': 'A valid email address is required'})
                with connect() as db:
                    cursor = db.execute('INSERT OR IGNORE INTO requests(id,email,sku,created_at) VALUES (?,?,?,?)', (secrets.token_hex(16), address, sku, time.time()))
                    duplicate = cursor.rowcount == 0
                WAKE.set()
                return self.json(200 if duplicate else 201, {'ok': True, 'duplicate': duplicate})
            event = data.get('event_id')
            quantity = data.get('quantity')
            if not isinstance(event, str) or not 1 <= len(event) <= 200 or isinstance(quantity, bool) or not isinstance(quantity, int) or quantity < 0:
                return self.json(400, {'error': 'event_id and non-negative integer quantity are required'})
            digest = hashlib.sha256(json.dumps({'sku': sku, 'quantity': quantity}, sort_keys=True).encode()).hexdigest()
            with connect() as db:
                db.execute('BEGIN IMMEDIATE')
                existing = db.execute('SELECT digest FROM events WHERE id=?', (event,)).fetchone()
                if existing and existing['digest'] != digest:
                    return self.json(409, {'error': 'Event id already used with different content'})
                if not existing:
                    db.execute('INSERT INTO events VALUES (?,?,?)', (event, digest, time.time()))
                    db.execute('UPDATE requests SET next_attempt=0 WHERE sku=? AND notified_at IS NULL', (sku,))
            WAKE.set()
            self.json(200, {'ok': True, 'duplicate': bool(existing)})
        except (ValueError, TypeError, UnicodeDecodeError):
            self.json(400, {'error': 'Invalid JSON body'})
        except sqlite3.Error:
            self.json(503, {'error': 'Storage temporarily unavailable; retry safely'})


def main():
    logging.basicConfig(level=logging.INFO, format='%(levelname)s %(message)s')
    initialize()
    threading.Thread(target=worker, daemon=True).start()
    server = ThreadingHTTPServer(('0.0.0.0', int(os.getenv('PORT', '8080'))), Handler)
    def shutdown(*_):
        STOP.set()
        WAKE.set()
        threading.Thread(target=server.shutdown, daemon=True).start()
    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)
    try:
        server.serve_forever()
    finally:
        STOP.set()
        server.server_close()


if __name__ == '__main__':
    main()
