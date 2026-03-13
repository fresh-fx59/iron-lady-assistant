from src.gmail_gcp_bootstrap import build_manual_console_url


def test_build_manual_console_url_points_to_project_credentials_page() -> None:
    assert (
        build_manual_console_url("ila-demo-project")
        == "https://console.cloud.google.com/apis/credentials?project=ila-demo-project"
    )
