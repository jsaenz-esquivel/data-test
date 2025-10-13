"""
Custom exceptions - easier to debug than catching generic Exception everywhere.
"""


class ETLException(Exception):
    """Base class for ETL errors."""
    pass


class MetadataValidationError(ETLException):
    """When metadata JSON is malformed or missing required fields."""
    pass


class SourceLoadError(ETLException):
    """Can't read input files (missing, wrong format, etc)."""
    pass


class TransformationError(ETLException):
    """Something went wrong during data transformation."""
    pass


class SinkWriteError(ETLException):
    """Can't write output files (permissions, disk space, etc)."""
    pass


class ValidationRuleNotFound(ETLException):
    """Tried to use a validation rule that doesn't exist."""
    pass