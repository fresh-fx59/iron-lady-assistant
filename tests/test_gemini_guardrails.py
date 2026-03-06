from src.bot import _allows_gemini_api_for_request, _is_tool_allowed_by_gemini_policy


def test_gemini_allowed_for_image_request() -> None:
    assert _allows_gemini_api_for_request("Generate image of a mountain lake")


def test_gemini_blocked_for_non_image_request() -> None:
    assert not _allows_gemini_api_for_request("Summarize this article")


def test_second_pass_tool_policy_blocks_gemini_non_image() -> None:
    assert not _is_tool_allowed_by_gemini_policy("gemini", "help me with this code")


def test_second_pass_tool_policy_allows_gemini_image() -> None:
    assert _is_tool_allowed_by_gemini_policy("gemini", "create image of a fox")
