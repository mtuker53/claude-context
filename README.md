# claude-context

Keep `CLAUDE.md` updated with live API consumer context from HTTP traffic.

## Installation

```bash
pip install claude-context
```

## Usage

### FastAPI
```python
from claude_context import ClaudeContextMiddleware
app.add_middleware(ClaudeContextMiddleware, service_name="my-api")
```

### Flask
```python
from claude_context.wsgi import ClaudeContextMiddleware
app.wsgi_app = ClaudeContextMiddleware(app.wsgi_app, service_name="my-api")
```

### Lambda
```python
from claude_context import claude_context_tracker

@claude_context_tracker(service_name="my-api")
def handler(event, context):
    ...
```

### CLI
```bash
# Sync CLAUDE.md manually
claude-context sync --service my-api

# Install Claude Code pre-tool hook
claude-context install-hook
```
