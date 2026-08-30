class FabMQError(Exception):
    """Base exception for FabMQ errors."""


class ConfigurationError(FabMQError):
    """Raised when required configuration is missing or invalid."""


class TopicError(FabMQError):
    """Raised for topic administration failures."""
