"""Exception hierarchy of the public API."""


class SmallageError(Exception):
    """Base of every error this library raises."""


class ConfigurationError(SmallageError):
    """Invalid configuration. Raised at construction, never from the worker loop."""


class MalformedEnvelope(SmallageError):
    """A stream entry is missing a field the envelope requires."""


class PayloadTooLarge(SmallageError):
    """Encoded arguments exceed the configured limit for an inline payload."""
