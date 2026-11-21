from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import threading

import httpx
import pytest

from tinvest_signal_engine.adapters.gemini_analyzer import GeminiLlmAnalyzer
from tinvest_signal_engine.adapters.gemini_trader import GeminiTraderCopilotAdapter
from tinvest_signal_engine.adapters.internet_news_search import GoogleNewsRssSearchAdapter
from tinvest_signal_engine.adapters.llm_http import create_llm_http_client
from tinvest_signal_engine.adapters.news_provider import RssNewsProvider


def test_required_vpn_rejects_missing_proxy_before_creating_client(monkeypatch):
    monkeypatch.setenv("GEMINI_REQUIRE_PROXY", "true")
    monkeypatch.delenv("GEMINI_HTTPS_PROXY", raising=False)
    with pytest.raises(ValueError, match="GEMINI_HTTPS_PROXY is required"):
        create_llm_http_client(timeout_seconds=1)


def test_all_llm_clients_use_explicit_proxy_and_do_not_fall_back(monkeypatch):
    requests = []

    class ProxyHandler(BaseHTTPRequestHandler):
        def do_GET(self):
            requests.append(("GET", self.path))
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b'proxied')

        def do_CONNECT(self):
            requests.append(("CONNECT", self.path))
            self.send_response(502)
            self.end_headers()

        def log_message(self, *_args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), ProxyHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    monkeypatch.setenv("GEMINI_REQUIRE_PROXY", "true")
    monkeypatch.setenv("GEMINI_HTTPS_PROXY", f"http://127.0.0.1:{server.server_port}")
    monkeypatch.setenv("HTTPS_PROXY", "http://unrelated-proxy.invalid:1")
    adapters = [
        GeminiLlmAnalyzer(api_key="redacted"),
        GeminiTraderCopilotAdapter(api_key="redacted"),
        GoogleNewsRssSearchAdapter(),
        RssNewsProvider(),
    ]
    try:
        for adapter in adapters:
            assert adapter._client.get("http://llm-egress-check.invalid/").text == "proxied"
            with pytest.raises(httpx.ProxyError):
                adapter._client.get("https://llm-egress-check.invalid/")
        assert len(requests) == 8
        assert sum(method == "CONNECT" for method, _ in requests) == 4
    finally:
        for adapter in adapters:
            adapter.close()
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)
