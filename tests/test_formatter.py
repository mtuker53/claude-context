from pathlib import Path

import pytest

from claude_context.generation.formatter import (
    END_MARKER,
    START_MARKER,
    generate_section,
    update_claude_md,
)


SAMPLE_ENDPOINTS = {
    "POST /api/orders": [
        {
            "caller": "checkout-service",
            "request_fields": ["cart_id", "user_id"],
            "request_headers": ["x-correlation-id"],
            "query_params": [],
            "response_codes": ["200", "422"],
            "call_count": 1500,
            "last_seen": "2026-02-21T10:00:00Z",
        }
    ]
}


class TestGenerateSection:
    def test_contains_markers(self):
        section = generate_section(SAMPLE_ENDPOINTS, "my-api")
        assert START_MARKER in section
        assert END_MARKER in section

    def test_contains_service_name(self):
        section = generate_section(SAMPLE_ENDPOINTS, "my-api")
        assert "my-api" in section

    def test_contains_caller(self):
        section = generate_section(SAMPLE_ENDPOINTS, "my-api")
        assert "checkout-service" in section

    def test_contains_endpoint(self):
        section = generate_section(SAMPLE_ENDPOINTS, "my-api")
        assert "POST /api/orders" in section

    def test_contains_field_names(self):
        section = generate_section(SAMPLE_ENDPOINTS, "my-api")
        assert "cart_id" in section
        assert "user_id" in section

    def test_contains_response_codes(self):
        section = generate_section(SAMPLE_ENDPOINTS, "my-api")
        assert "200" in section
        assert "422" in section

    def test_no_endpoints(self):
        section = generate_section({}, "my-api")
        assert START_MARKER in section
        assert END_MARKER in section


class TestUpdateClaudeMd:
    def test_creates_file_if_not_exists(self, tmp_path):
        path = tmp_path / "CLAUDE.md"
        update_claude_md(path, "content")
        assert path.exists()
        assert "content" in path.read_text()

    def test_appends_to_existing_file(self, tmp_path):
        path = tmp_path / "CLAUDE.md"
        path.write_text("# Existing content\n")
        section = f"{START_MARKER}\ngenerated\n{END_MARKER}"
        update_claude_md(path, section)
        content = path.read_text()
        assert "Existing content" in content
        assert "generated" in content

    def test_replaces_existing_section(self, tmp_path):
        path = tmp_path / "CLAUDE.md"
        path.write_text(f"# Before\n\n{START_MARKER}\nold content\n{END_MARKER}\n\n# After\n")
        section = f"{START_MARKER}\nnew content\n{END_MARKER}"
        update_claude_md(path, section)
        content = path.read_text()
        assert "new content" in content
        assert "old content" not in content
        assert "# Before" in content
        assert "# After" in content

    def test_idempotent_on_repeated_sync(self, tmp_path):
        path = tmp_path / "CLAUDE.md"
        section = f"{START_MARKER}\ncontent\n{END_MARKER}"
        update_claude_md(path, section)
        update_claude_md(path, section)
        content = path.read_text()
        assert content.count(START_MARKER) == 1
