import base64
import concurrent.futures
import json
import os
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import app


class MockProvider(BaseHTTPRequestHandler):
    stock = {}
    messages = {}
    calls = []
    failures = 0
    uncertain = False

    def log_message(self, *_):
        pass

    def reply(self, code, data):
        raw = json.dumps(data).encode()
        self.send_response(code)
        self.send_header('Content-Length', str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def do_GET(self):
        if self.headers.get('Authorization') != 'Bearer stock-test':
            return self.reply(401, {})
        sku = self.path.rsplit('/', 1)[-1]
        qty = self.stock.get(sku, 0)
        self.reply(200, {'quantity': qty, 'in_stock': qty > 0})

    def do_POST(self):
        if self.path != '/emails' or self.headers.get('Authorization') != 'Bearer mail-test':
            return self.reply(401, {})
        data = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
        key = self.headers.get('Idempotency-Key')
        self.calls.append((key, data))
        if key in self.messages:
            return self.reply(200, {'id': self.messages[key]['id']})
        if self.failures:
            type(self).failures -= 1
            return self.reply(500, {'error': 'temporary'})
        self.messages[key] = {'id': 'msg-' + str(len(self.messages) + 1), 'payload': data}
        if self.uncertain:
            type(self).uncertain = False
            # The provider accepted the email, but the response was lost/invalid.
            return self.reply(200, {})
        self.reply(200, {'id': self.messages[key]['id']})


class RestockTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory()
        app.DB_PATH = cls.temp.name + '/db.sqlite3'
        os.environ['ADMIN_PASSWORD'] = 'test-only-admin'
        app.WEBHOOK_SECRET = 'test-only-webhook'
        app.POLL_SECONDS = .01
        app.initialize()
        cls.provider = ThreadingHTTPServer(('127.0.0.1', 0), MockProvider)
        threading.Thread(target=cls.provider.serve_forever, daemon=True).start()
        base = 'http://127.0.0.1:' + str(cls.provider.server_port)
        app.STOCK_URL, app.STOCK_KEY = base + '/stock', 'stock-test'
        app.RESEND_URL, app.RESEND_KEY = base, 'mail-test'
        cls.http = ThreadingHTTPServer(('127.0.0.1', 0), app.Handler)
        threading.Thread(target=cls.http.serve_forever, daemon=True).start()
        cls.url = 'http://127.0.0.1:' + str(cls.http.server_port)

    @classmethod
    def tearDownClass(cls):
        cls.http.shutdown()
        cls.provider.shutdown()
        cls.http.server_close()
        cls.provider.server_close()
        cls.temp.cleanup()

    def setUp(self):
        with app.connect() as db:
            db.execute('DELETE FROM requests')
            db.execute('DELETE FROM events')
        MockProvider.stock = {}
        MockProvider.messages = {}
        MockProvider.calls = []
        MockProvider.failures = 0
        MockProvider.uncertain = False

    def http_call(self, path, data=None, headers=None):
        raw = json.dumps(data).encode() if data is not None else None
        req = urllib.request.Request(self.url + path, data=raw, headers={'Content-Type': 'application/json', **(headers or {})})
        try:
            with urllib.request.urlopen(req, timeout=10) as response:
                return response.status, response.read().decode()
        except urllib.error.HTTPError as exc:
            return exc.code, exc.read().decode()

    def signup(self, email='alex@example.com', sku='TEST-SKU'):
        return self.http_call('/api/requests', {'email': email, 'sku': sku})

    def hook(self, event='evt-test', sku='TEST-SKU', qty=2):
        return self.http_call('/webhooks/stock', {'event_id': event, 'sku': sku, 'quantity': qty}, {'X-Webhook-Secret': app.WEBHOOK_SECRET})

    def process(self):
        row = app.claim_request()
        self.assertIsNotNone(row)
        app.process_one(row)

    def due(self):
        with app.connect() as db:
            db.execute('UPDATE requests SET next_attempt=0,lease_until=0')

    def test_concurrent_signups_have_one_open_request(self):
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(lambda _: self.signup(), range(16)))
        self.assertEqual(sum(s == 201 for s, _ in results), 1)
        self.assertTrue(all(s in (200, 201) for s, _ in results))
        with app.connect() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM requests').fetchone()[0], 1)

    def test_email_case_normalization_deduplicates(self):
        self.assertEqual(self.signup(' Alex@Example.com ')[0], 201)
        self.assertEqual(self.signup('alex@example.com')[0], 200)

    def test_invalid_input_is_rejected(self):
        self.assertEqual(self.signup('not-an-email')[0], 400)
        self.assertEqual(self.signup(sku='../private')[0], 400)
        self.assertEqual(self.http_call('/api/requests', [1, 2])[0], 400)

    def test_bad_webhook_secret_is_rejected(self):
        self.assertEqual(self.http_call('/webhooks/stock', {'sku': 'TEST-SKU'}, {'X-Webhook-Secret': 'wrong'})[0], 401)

    def test_phantom_stock_does_not_send(self):
        self.signup()
        self.hook(qty=100)
        self.process()
        self.assertEqual(len(MockProvider.messages), 0)

    def test_concurrent_duplicate_webhooks_send_once(self):
        self.signup()
        MockProvider.stock['TEST-SKU'] = 2
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(lambda _: self.hook(), range(12)))
        self.assertTrue(all(s == 200 for s, _ in results))
        self.process()
        self.assertIsNone(app.claim_request())
        self.assertEqual(len(MockProvider.messages), 1)
        with app.connect() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM events').fetchone()[0], 1)

    def test_conflicting_event_id_rejected(self):
        self.assertEqual(self.hook(qty=1)[0], 200)
        self.assertEqual(self.hook(qty=2)[0], 409)

    def test_failed_email_retries_with_stable_key(self):
        self.signup()
        MockProvider.stock['TEST-SKU'] = 2
        MockProvider.failures = 1
        self.process()
        self.assertEqual(len(MockProvider.messages), 0)
        self.due()
        self.process()
        self.assertEqual(len(MockProvider.messages), 1)
        self.assertEqual(MockProvider.calls[0][0], MockProvider.calls[1][0])

    def test_uncertain_delivery_survives_reinitialization_without_duplicate(self):
        self.signup()
        MockProvider.stock['TEST-SKU'] = 2
        MockProvider.uncertain = True
        self.process()
        self.assertEqual(len(MockProvider.messages), 1)
        app.initialize()
        self.due()
        self.process()
        self.assertEqual(len(MockProvider.messages), 1)
        with app.connect() as db:
            row = db.execute('SELECT * FROM requests').fetchone()
            self.assertEqual(row['status'], 'notified')
            self.assertIsNotNone(row['notified_at'])

    def test_lease_prevents_concurrent_workers_claiming_same_request(self):
        self.signup()
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as pool:
            claims = list(pool.map(lambda _: app.claim_request(), range(5)))
        self.assertEqual(sum(x is not None for x in claims), 1)
        self.due()
        self.assertIsNotNone(app.claim_request())

    def test_immediately_in_stock_signup_and_repeat_lifecycle(self):
        MockProvider.stock['TEST-SKU'] = 2
        self.signup()
        self.process()
        self.assertEqual(len(MockProvider.messages), 1)
        self.assertEqual(self.signup()[0], 201)
        self.process()
        self.assertEqual(len(MockProvider.messages), 2)

    def test_admin_is_protected_and_shows_timestamps(self):
        self.assertEqual(self.http_call('/admin')[0], 401)
        self.signup()
        MockProvider.stock['TEST-SKU'] = 1
        self.process()
        auth = base64.b64encode(b'admin:test-only-admin').decode()
        code, body = self.http_call('/admin', headers={'Authorization': 'Basic ' + auth})
        self.assertEqual(code, 200)
        self.assertIn('alex@example.com', body)
        self.assertIn('notified', body)
        self.assertIn('UTC', body)

    def test_embed_page_and_health(self):
        self.assertEqual(self.http_call('/health')[0], 200)
        code, body = self.http_call('/embed')
        self.assertEqual(code, 200)
        self.assertIn('type="email"', body)
        self.assertIn('/api/requests', body)

    def test_long_uncertainty_is_flagged_for_review(self):
        self.signup()
        MockProvider.stock['TEST-SKU'] = 2
        with app.connect() as db:
            db.execute('UPDATE requests SET first_attempt=?', (time.time() - 24 * 3600,))
        self.process()
        self.assertEqual(len(MockProvider.messages), 0)
        with app.connect() as db:
            self.assertEqual(db.execute('SELECT status FROM requests').fetchone()[0], 'needs_review')


if __name__ == '__main__':
    unittest.main(verbosity=2)
