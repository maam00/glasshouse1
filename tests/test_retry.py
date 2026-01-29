"""
Tests for retry logic with exponential backoff.
"""

import pytest
import time
from src.api.retry import (
    RetryConfig,
    calculate_backoff,
    retry_with_backoff,
    retry_decorator,
)


class TestCalculateBackoff:
    """Test backoff calculation."""

    def test_initial_delay(self):
        """Test that first attempt uses base delay."""
        delay = calculate_backoff(0, base_delay=1.0, jitter=False)
        assert delay == 1.0

    def test_exponential_increase(self):
        """Test that delay increases exponentially."""
        delay_0 = calculate_backoff(0, base_delay=1.0, jitter=False)
        delay_1 = calculate_backoff(1, base_delay=1.0, jitter=False)
        delay_2 = calculate_backoff(2, base_delay=1.0, jitter=False)

        assert delay_0 == 1.0
        assert delay_1 == 2.0
        assert delay_2 == 4.0

    def test_max_delay_cap(self):
        """Test that delay is capped at max_delay."""
        delay = calculate_backoff(
            10,  # Would be 1024 without cap
            base_delay=1.0,
            max_delay=30.0,
            jitter=False
        )
        assert delay == 30.0

    def test_jitter_varies_delay(self):
        """Test that jitter causes variation in delay."""
        # With jitter, delays should vary
        delays = [
            calculate_backoff(1, base_delay=1.0, jitter=True)
            for _ in range(10)
        ]

        # Not all delays should be exactly equal
        # (statistically very unlikely with jitter)
        assert len(set(delays)) > 1


class TestRetryWithBackoff:
    """Test retry_with_backoff function."""

    def test_success_on_first_try(self):
        """Test that successful function returns immediately."""
        call_count = 0

        def success_func():
            nonlocal call_count
            call_count += 1
            return "success"

        result = retry_with_backoff(success_func)

        assert result == "success"
        assert call_count == 1

    def test_retry_on_failure(self):
        """Test that function retries on failure."""
        call_count = 0

        def fail_then_succeed():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ValueError("Temporary error")
            return "success"

        config = RetryConfig(
            max_retries=3,
            base_delay=0.01,  # Fast for testing
            retryable_exceptions=(ValueError,),
        )

        result = retry_with_backoff(fail_then_succeed, config)

        assert result == "success"
        assert call_count == 3

    def test_raises_after_max_retries(self):
        """Test that exception is raised after max retries."""
        call_count = 0

        def always_fail():
            nonlocal call_count
            call_count += 1
            raise ValueError("Permanent error")

        config = RetryConfig(
            max_retries=2,
            base_delay=0.01,
            retryable_exceptions=(ValueError,),
        )

        with pytest.raises(ValueError):
            retry_with_backoff(always_fail, config)

        # Should have tried 3 times (initial + 2 retries)
        assert call_count == 3

    def test_non_retryable_exception_raises_immediately(self):
        """Test that non-retryable exceptions are raised immediately."""
        call_count = 0

        def raise_type_error():
            nonlocal call_count
            call_count += 1
            raise TypeError("Not retryable")

        config = RetryConfig(
            max_retries=3,
            retryable_exceptions=(ValueError,),  # Only ValueError is retryable
        )

        with pytest.raises(TypeError):
            retry_with_backoff(raise_type_error, config)

        # Should only be called once
        assert call_count == 1

    def test_on_retry_callback(self):
        """Test that on_retry callback is called."""
        retry_attempts = []

        def fail_twice():
            if len(retry_attempts) < 2:
                raise ValueError("Error")
            return "success"

        def on_retry(attempt, exception):
            retry_attempts.append((attempt, str(exception)))

        config = RetryConfig(
            max_retries=3,
            base_delay=0.01,
            retryable_exceptions=(ValueError,),
        )

        result = retry_with_backoff(fail_twice, config, on_retry=on_retry)

        assert result == "success"
        assert len(retry_attempts) == 2
        assert retry_attempts[0][0] == 0  # First retry attempt
        assert retry_attempts[1][0] == 1  # Second retry attempt


class TestRetryDecorator:
    """Test retry_decorator."""

    def test_decorator_retries_function(self):
        """Test that decorator enables retries."""
        call_count = 0

        @retry_decorator(max_retries=2, base_delay=0.01)
        def flaky_function():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise Exception("Temporary error")
            return "success"

        result = flaky_function()

        assert result == "success"
        assert call_count == 2

    def test_decorator_preserves_function_metadata(self):
        """Test that decorator preserves function name and docstring."""

        @retry_decorator(max_retries=2)
        def documented_function():
            """This is a docstring."""
            return "result"

        assert documented_function.__name__ == "documented_function"
        assert "docstring" in documented_function.__doc__


class TestRetryConfig:
    """Test RetryConfig dataclass."""

    def test_default_values(self):
        """Test default configuration values."""
        config = RetryConfig()

        assert config.max_retries == 3
        assert config.base_delay == 1.0
        assert config.max_delay == 30.0
        assert config.jitter == True

    def test_custom_values(self):
        """Test custom configuration values."""
        config = RetryConfig(
            max_retries=5,
            base_delay=2.0,
            max_delay=60.0,
            jitter=False,
        )

        assert config.max_retries == 5
        assert config.base_delay == 2.0
        assert config.max_delay == 60.0
        assert config.jitter == False

    def test_retryable_status_codes(self):
        """Test default retryable status codes."""
        config = RetryConfig()

        # Should retry on rate limit and server errors
        assert 429 in config.retryable_status_codes  # Too Many Requests
        assert 500 in config.retryable_status_codes  # Internal Server Error
        assert 502 in config.retryable_status_codes  # Bad Gateway
        assert 503 in config.retryable_status_codes  # Service Unavailable
        assert 504 in config.retryable_status_codes  # Gateway Timeout
