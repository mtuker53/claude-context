import json
import logging
import time
from pathlib import Path

import click

from claude_context.generation.formatter import generate_section, update_claude_md
from claude_context.generation.transformer import transform_items
from claude_context.storage.dynamo import fetch_service_data

logger = logging.getLogger(__name__)


@click.group()
@click.version_option()
def cli():
    """Keep CLAUDE.md updated with live API consumer context."""
    pass


@cli.command()
@click.option("--service", required=True, envvar="CLAUDE_CONTEXT_SERVICE", help="Service name")
@click.option("--table", envvar="CLAUDE_CONTEXT_TABLE", default="claude-context", show_default=True)
@click.option("--region", envvar="AWS_DEFAULT_REGION")
@click.option("--output", default="./CLAUDE.md", show_default=True, type=click.Path())
@click.option("--dry-run", is_flag=True, help="Print output without writing to file")
def sync(service, table, region, output, dry_run):
    """Sync API consumer data from DynamoDB into CLAUDE.md."""
    items = fetch_service_data(table_name=table, service_name=service, region=region)

    if not items:
        click.echo(f"No data found for service '{service}'")
        return

    endpoints = transform_items(items)
    section = generate_section(endpoints, service)

    if dry_run:
        click.echo(section)
        return

    update_claude_md(Path(output), section)
    click.echo(f"Updated {output} ({len(endpoints)} endpoint(s) from {len(items)} record(s))")


@cli.command()
@click.option("--cache-minutes", default=60, show_default=True, envvar="CLAUDE_CONTEXT_CACHE_MINUTES")
@click.option("--service", envvar="CLAUDE_CONTEXT_SERVICE")
@click.option("--table", envvar="CLAUDE_CONTEXT_TABLE", default="claude-context")
@click.option("--region", envvar="AWS_DEFAULT_REGION")
@click.option("--output", default="./CLAUDE.md", type=click.Path())
def hook(cache_minutes, service, table, region, output):
    """Pre-tool hook for Claude Code. Syncs context if cache is stale."""
    if not service:
        return  # Not configured for this project — pass through silently

    cache_file = Path(".claude/.cc-last-sync")

    if cache_file.exists():
        try:
            last_sync = float(cache_file.read_text().strip())
            age_minutes = (time.time() - last_sync) / 60
            if age_minutes < cache_minutes:
                return  # Cache is fresh
        except ValueError:
            pass  # Corrupt timestamp file — proceed with sync

    try:
        items = fetch_service_data(table_name=table, service_name=service, region=region)
        if items:
            endpoints = transform_items(items)
            section = generate_section(endpoints, service)
            update_claude_md(Path(output), section)
        cache_file.parent.mkdir(parents=True, exist_ok=True)
        cache_file.write_text(str(time.time()))
    except Exception as e:
        # Never block Claude from making edits due to a sync failure
        click.echo(f"claude-context hook: sync failed (non-fatal): {e}", err=True)


@cli.command("install-hook")
@click.option(
    "--global", "global_install", is_flag=True,
    help="Install in ~/.claude/settings.json instead of .claude/settings.json"
)
def install_hook(global_install):
    """Add claude-context to .claude/settings.json PreToolUse hooks."""
    if global_install:
        settings_path = Path.home() / ".claude" / "settings.json"
    else:
        settings_path = Path(".claude/settings.json")

    settings_path.parent.mkdir(parents=True, exist_ok=True)

    if settings_path.exists():
        try:
            settings = json.loads(settings_path.read_text())
        except json.JSONDecodeError:
            click.echo(f"Error: {settings_path} contains invalid JSON", err=True)
            raise click.Abort()
    else:
        settings = {}

    hook_command = "claude-context hook"
    hook_entry = {
        "matcher": "Edit|Write|NotebookEdit",
        "hooks": [{"type": "command", "command": hook_command}],
    }

    hooks_list = settings.setdefault("hooks", {}).setdefault("PreToolUse", [])

    already_installed = any(
        any(h.get("command") == hook_command for h in entry.get("hooks", []))
        for entry in hooks_list
    )

    if already_installed:
        click.echo("claude-context hook is already installed.")
        return

    hooks_list.append(hook_entry)
    settings_path.write_text(json.dumps(settings, indent=2) + "\n")
    click.echo(f"Hook installed in {settings_path}")
    click.echo("Make sure CLAUDE_CONTEXT_SERVICE is set in your environment.")
