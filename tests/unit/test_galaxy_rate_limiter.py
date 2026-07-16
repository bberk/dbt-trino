import threading
import time
import unittest
from unittest.mock import MagicMock, patch

import requests

from dbt.adapters.trino.galaxy.api_client import GalaxyDiscoveryClient, _AdaptiveRateLimiter


class TestAdaptiveRateLimiter(unittest.TestCase):
    def test_acquire_returns_immediately_when_tokens_available(self):
        limiter = _AdaptiveRateLimiter(initial_rate=10.0)
        start = time.monotonic()
        limiter.acquire()
        elapsed = time.monotonic() - start
        self.assertLess(elapsed, 0.1)

    def test_acquire_throttles_when_tokens_exhausted(self):
        limiter = _AdaptiveRateLimiter(initial_rate=10.0)
        # Drain all tokens
        for _ in range(10):
            limiter.acquire()
        # The next acquire should wait ~0.1s (1 token at 10/s)
        start = time.monotonic()
        limiter.acquire()
        elapsed = time.monotonic() - start
        self.assertGreater(elapsed, 0.05)

    def test_on_success_increases_rate(self):
        limiter = _AdaptiveRateLimiter(initial_rate=5.0)
        limiter.on_success()
        with limiter._lock:
            self.assertAlmostEqual(limiter._rate, 5.1, places=5)

    def test_on_rate_limited_halves_rate(self):
        limiter = _AdaptiveRateLimiter(initial_rate=10.0)
        limiter.on_rate_limited()
        with limiter._lock:
            self.assertAlmostEqual(limiter._rate, 5.0, places=5)

    def test_on_rate_limited_with_retry_after(self):
        limiter = _AdaptiveRateLimiter(initial_rate=10.0, min_rate=0.1)
        # Retry-After: 2s → estimated safe rate = (1/2) * 0.8 = 0.4 req/s
        limiter.on_rate_limited(retry_after=2.0)
        with limiter._lock:
            self.assertAlmostEqual(limiter._rate, 0.4, places=5)

    def test_on_rate_limited_respects_min_rate(self):
        limiter = _AdaptiveRateLimiter(initial_rate=0.6, min_rate=0.5)
        limiter.on_rate_limited()
        with limiter._lock:
            self.assertGreaterEqual(limiter._rate, 0.5)

    def test_thread_safe_acquire(self):
        """Multiple threads can acquire tokens without data corruption."""
        limiter = _AdaptiveRateLimiter(initial_rate=100.0)
        errors = []

        def worker():
            try:
                limiter.acquire()
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=worker) for _ in range(20)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=5.0)

        self.assertEqual(errors, [])

    def test_rate_is_shared_across_instances(self):
        """The class-level rate limiter is shared; modifying it on one instance
        is visible from another."""
        original_rate = GalaxyDiscoveryClient._rate_limiter._rate
        try:
            GalaxyDiscoveryClient._rate_limiter.on_success()
            client1 = GalaxyDiscoveryClient("d1", "c1", "s1")
            client2 = GalaxyDiscoveryClient("d2", "c2", "s2")
            self.assertIs(client1._rate_limiter, client2._rate_limiter)
        finally:
            GalaxyDiscoveryClient._rate_limiter._rate = original_rate


class TestCatalogIdThreadSafety(unittest.TestCase):
    def test_catalog_id_resolved_once_under_concurrent_access(self):
        """_resolve_catalog_id should be called exactly once per unique catalog
        name even when multiple threads request it simultaneously."""
        client = GalaxyDiscoveryClient("domain", "cid", "sec")
        resolve_count = 0
        count_lock = threading.Lock()

        def fake_resolve(name):
            nonlocal resolve_count
            time.sleep(0.02)  # simulate network latency
            with count_lock:
                resolve_count += 1
            return "catalog-id-123"

        with patch.object(client, "_resolve_catalog_id", side_effect=fake_resolve):
            threads = [
                threading.Thread(target=client._get_catalog_id, args=("my_catalog",))
                for _ in range(10)
            ]
            for t in threads:
                t.start()
            for t in threads:
                t.join(timeout=5.0)

        self.assertEqual(resolve_count, 1)
        self.assertEqual(client._catalog_ids.get("my_catalog"), "catalog-id-123")


class TestApiRequest429Handling(unittest.TestCase):
    def setUp(self):
        self.client = GalaxyDiscoveryClient("domain", "cid", "sec")
        # Save and reset the class-level rate limiter to a known state so
        # tests don't interfere with each other.
        with GalaxyDiscoveryClient._rate_limiter._lock:
            self._saved_rate = GalaxyDiscoveryClient._rate_limiter._rate
            GalaxyDiscoveryClient._rate_limiter._rate = 10.0
            GalaxyDiscoveryClient._rate_limiter._tokens = 10.0

    def tearDown(self):
        with GalaxyDiscoveryClient._rate_limiter._lock:
            GalaxyDiscoveryClient._rate_limiter._rate = self._saved_rate

    @patch.object(GalaxyDiscoveryClient, "_ensure_token", return_value=True)
    def test_429_adjusts_rate_and_raises(self, _mock_token):
        mock_response = MagicMock()
        mock_response.status_code = 429
        mock_response.headers = {"Retry-After": "2"}
        mock_response.raise_for_status.side_effect = requests.HTTPError("429")
        self.client._session = MagicMock()
        self.client._session.get.return_value = mock_response

        rate_before = GalaxyDiscoveryClient._rate_limiter._rate
        with patch("time.sleep"):  # don't actually sleep in tests
            with self.assertRaises(requests.HTTPError):
                self.client._api_get("/catalog")
        rate_after = GalaxyDiscoveryClient._rate_limiter._rate
        self.assertLess(rate_after, rate_before)

    @patch.object(GalaxyDiscoveryClient, "_ensure_token", return_value=True)
    def test_success_increases_rate(self, _mock_token):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = []
        mock_response.raise_for_status = MagicMock()
        self.client._session = MagicMock()
        self.client._session.get.return_value = mock_response

        rate_before = GalaxyDiscoveryClient._rate_limiter._rate
        self.client._api_get("/catalog")
        rate_after = GalaxyDiscoveryClient._rate_limiter._rate
        self.assertGreater(rate_after, rate_before)


if __name__ == "__main__":
    unittest.main()
