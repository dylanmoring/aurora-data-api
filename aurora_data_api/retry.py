"""retry_resuming
A tiny, no-deps retry decorator for handling Aurora RDS Data API "DatabaseResuming"-style
errors with a fixed backoff schedule. Works for BOTH sync and async callables.

Usage (sync):

    from retry_resuming import retry_resuming

    class AuroraDataAPIClient:
        @retry_resuming(3, 6, 12, exceptions="DatabaseResumingException")
        def start_transaction(self):
            # ... call boto3 client.begin_transaction(...)
            ...

        @retry_resuming(3, 6, 12, exceptions="DatabaseResumingException")
        def commit(self):
            ...

        @retry_resuming(3, 6, 12, exceptions="DatabaseResumingException")
        def rollback(self):
            ...

Usage (async):

    class AsyncAuroraDataAPIClient:
        @retry_resuming(3, 6, 12, exceptions="DatabaseResumingException")
        async def start_transaction(self):
            ...

        @retry_resuming(3, 6, 12, exceptions="DatabaseResumingException")
        async def commit(self):
            ...

        @retry_resuming(3, 6, 12, exceptions="DatabaseResumingException")
        async def rollback(self):
            ...

Passing exceptions:
- You can pass a single exception class, an iterable of classes, and/or string names
  (e.g., "DatabaseResumingException"). String-names are handy when the concrete
  boto client type isn’t easily available at import time. A retry happens if the
  caught exception is an instance of any provided class OR its type name matches
  any provided string.

Backoff policy:
- First attempt runs immediately. Then for each delay (d0, d1, ... dn), a retry is attempted
  after sleeping that many seconds. With N delays you get N+1 total attempts.

Logging:
- Logs at WARNING level before each sleep. Logger name: this module’s __name__.
"""
from __future__ import annotations

import asyncio
import time
import logging
import inspect
import functools
from typing import Iterable, Tuple, Sequence, Union, Type

logger = logging.getLogger(__name__)

ExceptionType = Type[BaseException]
ExceptionsParam = Union[ExceptionType, Sequence[ExceptionType], str, Sequence[str]]


def _normalize_exceptions(exceptions: ExceptionsParam) -> Tuple[Tuple[ExceptionType, ...], Tuple[str, ...]]:
    """Return (exception_types, exception_names).

    Accepts a single exception class, an iterable of classes, a single string name,
    or an iterable of string names. Mixed inputs are allowed (e.g., [TypeA, "FooError"]).
    """
    if exceptions is None:
        raise ValueError("exceptions must be provided (type(s) or name(s))")
    elif isinstance(exceptions, (str, type)):
        exceptions = (exceptions,)

    types: list[ExceptionType] = []
    names: list[str] = []

    def _add(obj):
        if isinstance(obj, type) and issubclass(obj, BaseException):
            types.append(obj)
        elif isinstance(obj, str):
            names.append(obj)
        else:
            raise TypeError(
                "exceptions must be exception class(es) or string name(s); got %r" % (obj,)
            )

    try:
        # Try iterate; if it's a single item (e.g., class), TypeError below will kick in
        for item in exceptions:  # type: ignore[assignment]
            _add(item)
    except TypeError:
        # Not iterable -> treat as single
        _add(exceptions)  # type: ignore[arg-type]

    return tuple(types), tuple(names)


def _should_retry(exc: BaseException, exc_types: Tuple[ExceptionType, ...], exc_names: Tuple[str, ...]) -> bool:
    if exc_types and isinstance(exc, exc_types):
        return True
    if exc_names and type(exc).__name__ in exc_names or any(name in type(exc).__name__ for name in exc_names):
        return True
    return False


def retry_exceptions(*delays: Union[int, float], exceptions: ExceptionsParam):
    """Retry decorator with fixed delays, supporting sync and async callables.

    Parameters
    ----------
    *delays : float
        Sequence of seconds to sleep *before* each retry (e.g., 3, 6, 12).
        With N delays, total attempts = N + 1.
    exceptions : Exception class | iterable[Exception class] | str | iterable[str]
        Exception(s) that should trigger a retry. You may pass class objects,
        or type names like "DatabaseResumingException".
    """
    exc_types, exc_names = _normalize_exceptions(exceptions)
    delays = tuple(float(d) for d in delays)

    def _log_retry(fn_qualname: str, delay: float, attempt: int, total_retries: int, exc: BaseException):
        logger.warning(
            "Caught %s in %s; retrying in %.3fs (retry %d/%d)",
            type(exc).__name__, fn_qualname, delay, attempt, total_retries,
        )

    def decorator(fn):
        fn_name = getattr(fn, "__qualname__", getattr(fn, "__name__", str(fn)))

        if inspect.iscoroutinefunction(fn):
            @functools.wraps(fn)
            async def async_wrapper(*args, **kwargs):
                total_retries = len(delays)
                for attempt_idx in range(total_retries + 1):
                    try:
                        return await fn(*args, **kwargs)
                    except BaseException as e:  # noqa: BLE001
                        if not _should_retry(e, exc_types, exc_names):
                            raise
                        if attempt_idx == total_retries:
                            raise
                        delay = delays[attempt_idx]
                        _log_retry(fn_name, delay, attempt_idx + 1, total_retries, e)
                        await asyncio.sleep(delay)
            return async_wrapper

        @functools.wraps(fn)
        def sync_wrapper(*args, **kwargs):
            total_retries = len(delays)
            for attempt_idx in range(total_retries + 1):
                try:
                    return fn(*args, **kwargs)
                except BaseException as e:  # noqa: BLE001
                    if not _should_retry(e, exc_types, exc_names):
                        raise
                    if attempt_idx == total_retries:
                        raise
                    delay = delays[attempt_idx]
                    _log_retry(fn_name, delay, attempt_idx + 1, total_retries, e)
                    time.sleep(delay)
        return sync_wrapper

    return decorator
