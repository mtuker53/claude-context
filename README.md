# claude-context

Keep `CLAUDE.md` updated with live API consumer context from HTTP traffic.

## Installation

```bash
# HTTP middleware (no existing OTEL setup required)
pip install claude-context

# OpenTelemetry integration (for services already using OTEL)
pip install claude-context[otel]
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

### OpenTelemetry
```python
from opentelemetry.sdk.trace import TracerProvider
from claude_context.otel import ClaudeContextSpanProcessor

provider = TracerProvider()
provider.add_span_processor(ClaudeContextSpanProcessor(service_name="my-api"))
```

For reliable caller identity, configure OTEL to capture service identity headers:
```bash
OTEL_INSTRUMENTATION_HTTP_CAPTURE_HEADERS_SERVER_REQUEST=x-service-name
```

### CLI
```bash
# Sync CLAUDE.md manually
claude-context sync --service my-api

# Install Claude Code pre-tool hook
claude-context install-hook
```
