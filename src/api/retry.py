"""
Retry Utilities with Exponential Backoff
========================================
Provides robust retry logic for API calls.
"""

import time
import logging
import functools
from typing import Callable, TypeVar, Optional, Tuple, Type
import random

logger = logging.getLogger(__name__)

T = TypeVar('T')


class RetryConfig:
    """Configuration for retry behavior."""

    def __init__(
        self,
        max_retries: int = 3,
        base_delay: float = 1.0,
        max_delay: float = 30.0,
        exponential_base: float = 2.0,
        jitter: bool = True,
        retryable_exceptions: Tuple[Type[Exception], ...] = (Exception,),
        retryable_status_codes: Tuple[int, ...] = (429, 500, 502, 503, 504),
    ):
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.exponential_base = exponential_base
        self.jitter = jitter
        self.retryable_exceptions = retryable_exceptions
        self.retryable_status_codes = retryable_status_codes


def calculate_backoff(
    attempt: int,
    base_delay: float = 1.0,
    max_delay: float = 30.0,
    exponential_base: float = 2.0,
    jitter: bool = True,
) -> float:
    """
    Calculate delay for exponential backoff.

    Args:
        attempt: The current attempt number (0-indexed)
        base_delay: Initial delay in seconds
        max_delay: Maximum delay cap
        exponential_base: Base for exponential calculation
        jitter: Add random jitter to prevent thundering herd

    Returns:
        Delay in seconds
    """
    delay = min(base_delay * (exponential_base ** attempt), max_delay)

    if jitter:
        # Add random jitter (0-50% of delay)
        delay = delay * (0.5 + random.random() * 0.5)

    return delay


def retry_with_backoff(
    func: Callable[..., T],
    config: Optional[RetryConfig] = None,
    on_retry: Optional[Callable[[int, Exception], None]] = None,
) -> T:
    """
    Execute a function with retry and exponential backoff.

    Args:
        func: The function to execute
        config: Retry configuration
        on_retry: Optional callback called on each retry with (attempt, exception)

    Returns:
        The result of the function

    Raises:
        The last exception if all retries fail
    """
    config = config or RetryConfig()
    last_exception = None

    for attempt in range(config.max_retries + 1):
        try:
            return func()
        except config.retryable_exceptions as e:
            last_exception = e

            if attempt >= config.max_retries:
                logger.error(f"All {config.max_retries + 1} attempts failed: {e}")
                raise

            delay = calculate_backoff(
                attempt,
                config.base_delay,
                config.max_delay,
                config.exponential_base,
                config.jitter,
            )

            logger.warning(
                f"Attempt {attempt + 1}/{config.max_retries + 1} failed: {e}. "
                f"Retrying in {delay:.2f}s..."
            )

            if on_retry:
                on_retry(attempt, e)

            time.sleep(delay)

    raise last_exception


def retry_decorator(
    max_retries: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 30.0,
    retryable_exceptions: Tuple[Type[Exception], ...] = (Exception,),
):
    """
    Decorator for automatic retry with exponential backoff.

    Usage:
        @retry_decorator(max_retries=3, base_delay=1.0)
        def my_api_call():
            ...
    """
    config = RetryConfig(
        max_retries=max_retries,
        base_delay=base_delay,
        max_delay=max_delay,
        retryable_exceptions=retryable_exceptions,
    )

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> T:
            return retry_with_backoff(
                lambda: func(*args, **kwargs),
                config=config,
            )
        return wrapper

    return decorator


class RetryableRequest:
    """
    Context manager for retryable HTTP requests.

    Usage:
        with RetryableRequest(config) as retry:
            response = retry.execute(lambda: requests.get(url))
    """

    def __init__(self, config: Optional[RetryConfig] = None):
        self.config = config or RetryConfig()
        self.attempts = 0
        self.last_error = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def execute(self, request_func: Callable) -> any:
        """Execute a request with retry logic."""
        return retry_with_backoff(request_func, self.config)

    def should_retry_response(self, response) -> bool:
        """Check if a response should trigger a retry."""
        if hasattr(response, 'status_code'):
            return response.status_code in self.config.retryable_status_codes
        return False


# Convenience function for requests library
def requests_retry(
    method: str,
    url: str,
    max_retries: int = 3,
    **kwargs
):
    """
    Make an HTTP request with automatic retry.

    Args:
        method: HTTP method (get, post, etc.)
        url: Request URL
        max_retries: Maximum retry attempts
        **kwargs: Additional arguments to pass to requests

    Returns:
        Response object
    """
    import requests

    config = RetryConfig(
        max_retries=max_retries,
        retryable_exceptions=(
            requests.exceptions.RequestException,
            requests.exceptions.Timeout,
            requests.exceptions.ConnectionError,
        ),
    )

    def make_request():
        response = getattr(requests, method.lower())(url, **kwargs)

        # Raise for retryable status codes
        if response.status_code in config.retryable_status_codes:
            raise requests.exceptions.RequestException(
                f"Retryable status code: {response.status_code}"
            )

        return response

    return retry_with_backoff(make_request, config)
