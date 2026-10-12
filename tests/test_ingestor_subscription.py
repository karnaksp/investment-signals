from types import SimpleNamespace

from tinkoff.invest import SubscriptionAction

from tinvest_signal_engine.services.ingestor import subscribe_to_stream


class _Stream:
    def __init__(self) -> None:
        self.requests = []

    def subscribe(self, request) -> None:
        self.requests.append(request)


def test_trade_subscription_requests_open_interest() -> None:
    stream = _Stream()
    instrument = SimpleNamespace(
        instrument_id="uid",
        trades=True,
        last_price=False,
        info=False,
        order_book_depth=None,
        candles=False,
        candle_interval="1m",
    )

    subscribe_to_stream(stream, [instrument])

    assert len(stream.requests) == 1
    request = stream.requests[0].subscribe_trades_request
    assert request.with_open_interest is True
    assert (
        request.subscription_action == SubscriptionAction.SUBSCRIPTION_ACTION_SUBSCRIBE
    )
    assert request.instruments[0].instrument_id == "uid"
