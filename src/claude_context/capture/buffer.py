import logging
import threading
import time
from collections.abc import Callable

from claude_context.capture.observation import Observation

logger = logging.getLogger(__name__)


class ObservationBuffer:
    """Thread-safe buffer that accumulates observations and flushes them in a background thread."""

    def __init__(
        self,
        flush_fn: Callable[[list[Observation]], None],
        max_size: int = 100,
        flush_interval: float = 30.0,
    ) -> None:
        self._flush_fn = flush_fn
        self.max_size = max_size
        self.flush_interval = flush_interval
        self._observations: list[Observation] = []
        self._lock = threading.Lock()
        self._last_flush = time.monotonic()

    def add(self, obs: Observation) -> None:
        with self._lock:
            self._observations.append(obs)
            if self._should_flush():
                self._flush_async()

    def flush(self) -> None:
        """Synchronous flush — call at the end of a Lambda invocation."""
        with self._lock:
            self._flush_sync()

    def _should_flush(self) -> bool:
        return (
            len(self._observations) >= self.max_size
            or time.monotonic() - self._last_flush >= self.flush_interval
        )

    def _flush_async(self) -> None:
        batch = self._drain()
        if batch:
            threading.Thread(target=self._safe_flush, args=(batch,), daemon=True).start()

    def _flush_sync(self) -> None:
        batch = self._drain()
        if batch:
            self._safe_flush(batch)

    def _drain(self) -> list[Observation]:
        batch = self._observations[:]
        self._observations = []
        self._last_flush = time.monotonic()
        return batch

    def _safe_flush(self, batch: list[Observation]) -> None:
        try:
            self._flush_fn(batch)
        except Exception:
            logger.warning("claude-context: failed to flush observations", exc_info=True)
