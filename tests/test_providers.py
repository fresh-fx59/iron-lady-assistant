from src.providers import Provider, ProviderManager


def test_subprocess_env_strips_gemini_keys_when_disallowed(monkeypatch) -> None:
    monkeypatch.setenv("GEMINI_API_KEY", "gem-key")
    monkeypatch.setenv("GOOGLE_API_KEY", "google-key")
    monkeypatch.setenv("GOOGLE_GENERATIVE_AI_API_KEY", "genai-key")
    monkeypatch.setenv("OPENAI_API_KEY", "openai-key")

    manager = ProviderManager(watch_config=False)
    provider = Provider(name="claude", description="Claude", cli="claude")

    env = manager.subprocess_env(provider, allow_gemini_api=False)

    assert "GEMINI_API_KEY" not in env
    assert "GOOGLE_API_KEY" not in env
    assert "GOOGLE_GENERATIVE_AI_API_KEY" not in env
    assert env.get("OPENAI_API_KEY") == "openai-key"


def test_subprocess_env_keeps_gemini_keys_when_allowed(monkeypatch) -> None:
    monkeypatch.setenv("GEMINI_API_KEY", "gem-key")
    monkeypatch.setenv("GOOGLE_API_KEY", "google-key")
    monkeypatch.setenv("GOOGLE_GENERATIVE_AI_API_KEY", "genai-key")

    manager = ProviderManager(watch_config=False)
    provider = Provider(name="claude", description="Claude", cli="claude")

    env = manager.subprocess_env(provider, allow_gemini_api=True)

    assert env.get("GEMINI_API_KEY") == "gem-key"
    assert env.get("GOOGLE_API_KEY") == "google-key"
    assert env.get("GOOGLE_GENERATIVE_AI_API_KEY") == "genai-key"
