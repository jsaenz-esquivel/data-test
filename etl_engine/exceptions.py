"""Custom exceptions for the ETL engine."""

class ETLError(Exception):
    """Base exception for ETL errors."""
    pass


class MetadataError(ETLError):
    """Raised when metadata JSON is invalid."""
    pass


class SourceLoadError(ETLError):
    """Raised when source data can't be loaded."""
    pass


class ValidationError(ETLError):
    """Raised during data validation."""
    pass


class TransformationError(ETLError):
    """Raised when a transformation fails."""
    pass


class SinkWriteError(ETLError):
    """Raised when writing output fails."""
    pass