import httpx

from tinvest_signal_engine.adapters.gemini_analyzer import GeminiLlmAnalyzer
from tinvest_signal_engine.domain.signal_llm import LlmSignalContext


def test_location_error_stops_request_storm_and_reports_reason(caplog):
    requests = []

    def respond(request):
        requests.append(request)
        return httpx.Response(400, json={"error": {
            "status": "FAILED_PRECONDITION",
            "message": "User location is not supported for the API use.",
        }})

    with httpx.Client(transport=httpx.MockTransport(respond)) as client:
        analyzer = GeminiLlmAnalyzer(api_key="secret-test-key", client=client)
        assert analyzer.analyze_signal(LlmSignalContext("SBER", "obi_dynamics")) is None
        assert not analyzer.is_news_relevant("SBER", "obi_dynamics", "Новость")
    assert len(requests) == 1
    assert "FAILED_PRECONDITION" in caplog.text
    assert "secret-test-key" not in caplog.text


def test_model_fallback_produces_analysis():
    requests = []

    def respond(request):
        requests.append(request)
        if len(requests) == 1:
            return httpx.Response(404, json={"error": {"status": "NOT_FOUND"}})
        return httpx.Response(200, json={"candidates": [{"content": {"parts": [{
            "text": '{"has_catalyst": false, "catalyst_summary": "Рост активности", "trade_bias": "LONG", "confidence_score": 0.8}',
        }]}}]})

    with httpx.Client(transport=httpx.MockTransport(respond)) as client:
        analyzer = GeminiLlmAnalyzer(
            api_key="test-key", analyzer_model="missing-model", client=client,
        )
        result = analyzer.analyze_signal(LlmSignalContext("SBER", "aggressive_trade_burst"))
    assert result is not None
    assert result.trade_bias == "LONG"
    assert len(requests) == 2
