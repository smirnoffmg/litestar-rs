"""Exception hierarchy of the public API."""


class LitestarRsError(Exception):
    """Base of every error this library raises."""


class ConfigurationError(LitestarRsError):
    """Invalid configuration. Raised at construction, never from the worker loop."""


class MalformedEnvelope(LitestarRsError):
    """A stream entry is missing a field the envelope requires."""


class PayloadTooLarge(LitestarRsError):
    """Encoded arguments exceed the configured limit for an inline payload."""
