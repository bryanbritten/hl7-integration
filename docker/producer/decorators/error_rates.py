import functools
import random


def with_error_rate(error_rate: float):
    """
    Decorator to introduce an error rate for functions that generate patient data.
    If the random number is less than the error rate, the function will return an empty string.
    Otherwise, it will execute the function normally.

    error_rate: float - The probability of the function returning an empty string.
    Returns: function - The wrapped function with error rate logic.
    """

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            rate = kwargs.pop("error_rate", error_rate)
            if random.random() <= rate:
                return ""
            return func(*args, **kwargs)

        return wrapper

    return decorator
