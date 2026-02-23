import json
from pathlib import Path
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from claude_context.cli.commands import cli
from claude_context.generation.formatter import END_MARKER, START_MARKER


@pytest.fixture
def runner():
    return CliRunner()


SAMPLE_ITEMS = [
    {
        "PK": "SERVICE#my-api",
        "SK": "CALLER#checkout#POST#/api/orders",
        "request_fields": {"user_id", "cart_id"},
        "request_headers": {"x-correlation-id"},
        "query_params": set(),
        "response_codes": {"200", "422"},
        "call_count": 1500,
        "last_seen": "2026-02-21T10:00:00Z",
        "first_seen": "2026-01-01T00:00:00Z",
    }
]


class TestSyncCommand:
    def test_dry_run_prints_section(self, runner):
        with patch("claude_context.cli.commands.fetch_service_data", return_value=SAMPLE_ITEMS):
            result = runner.invoke(cli, ["sync", "--service", "my-api", "--dry-run"])
        assert result.exit_code == 0
        assert START_MARKER in result.output
        assert "checkout" in result.output

    def test_writes_claude_md(self, runner, tmp_path):
        output = tmp_path / "CLAUDE.md"
        with patch("claude_context.cli.commands.fetch_service_data", return_value=SAMPLE_ITEMS):
            result = runner.invoke(
                cli, ["sync", "--service", "my-api", "--output", str(output)]
            )
        assert result.exit_code == 0
        assert output.exists()
        assert START_MARKER in output.read_text()

    def test_no_data_exits_cleanly(self, runner):
        with patch("claude_context.cli.commands.fetch_service_data", return_value=[]):
            result = runner.invoke(cli, ["sync", "--service", "my-api", "--dry-run"])
        assert result.exit_code == 0
        assert "No data found" in result.output


class TestHookCommand:
    def test_skips_when_no_service_configured(self, runner):
        result = runner.invoke(cli, ["hook"])
        assert result.exit_code == 0

    def test_syncs_when_cache_stale(self, runner, tmp_path):
        output = tmp_path / "CLAUDE.md"
        with runner.isolated_filesystem(temp_dir=tmp_path):
            with patch("claude_context.cli.commands.fetch_service_data", return_value=SAMPLE_ITEMS):
                result = runner.invoke(
                    cli,
                    ["hook", "--service", "my-api", "--output", str(output)],
                    env={"CLAUDE_CONTEXT_CACHE_MINUTES": "0"},
                )
        assert result.exit_code == 0

    def test_exits_zero_on_failure(self, runner, tmp_path):
        with runner.isolated_filesystem(temp_dir=tmp_path):
            with patch(
                "claude_context.cli.commands.fetch_service_data",
                side_effect=Exception("DynamoDB down"),
            ):
                result = runner.invoke(
                    cli,
                    ["hook", "--service", "my-api"],
                    env={"CLAUDE_CONTEXT_CACHE_MINUTES": "0"},
                )
        assert result.exit_code == 0


class TestInstallHookCommand:
    def test_creates_settings_file(self, runner):
        with runner.isolated_filesystem() as tmp_dir:
            result = runner.invoke(cli, ["install-hook"])
            assert result.exit_code == 0
            settings_path = Path(tmp_dir) / ".claude" / "settings.json"
            assert settings_path.exists()
            settings = json.loads(settings_path.read_text())
            hooks = settings["hooks"]["PreToolUse"]
            assert any(
                any(h.get("command") == "claude-context hook" for h in entry.get("hooks", []))
                for entry in hooks
            )

    def test_idempotent(self, runner):
        with runner.isolated_filesystem():
            runner.invoke(cli, ["install-hook"])
            result = runner.invoke(cli, ["install-hook"])
        assert result.exit_code == 0
        assert "already installed" in result.output
