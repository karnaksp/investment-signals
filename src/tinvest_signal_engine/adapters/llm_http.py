"""One explicit egress configuration for LLM and news HTTP clients."""

import os

import httpx


def create_llm_http_client(
    *, timeout_seconds: float, follow_redirects: bool = False,
) -> httpx.Client:
    proxy = os.getenv("GEMINI_HTTPS_PROXY", "").strip() or None
    require_proxy = os.getenv("GEMINI_REQUIRE_PROXY", "false").strip().lower() in {
        "1", "true", "yes",
    }
    if require_proxy and proxy is None:
        raise ValueError("GEMINI_HTTPS_PROXY is required when GEMINI_REQUIRE_PROXY=true")
    return httpx.Client(
        timeout=timeout_seconds,
        proxy=proxy,
        trust_env=False,
        follow_redirects=follow_redirects,
    )
