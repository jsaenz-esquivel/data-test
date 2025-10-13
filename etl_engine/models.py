"""
Pydantic models for ETL metadata structure.
Maps the JSON metadata to type-safe Python objects.
"""

from pydantic import BaseModel, Field
from typing import Any


# =============================================================================
# SOURCE MODELS
# =============================================================================

class Source(BaseModel):
    """Defines where to read input data from."""
    name: str
    path: str  # Supports wildcards like /data/input/events/person/*
    format: str  # JSON, CSV, PARQUET, etc


# =============================================================================
# TRANSFORMATION MODELS
# =============================================================================

class ValidationRule(BaseModel):
    """Single field validation rule."""
    field: str
    validations: list[str]  # ["notEmpty", "notNull", etc]


class AddFieldRule(BaseModel):
    """Rule for adding a new field."""
    name: str
    function: str  # "current_timestamp", "uuid", etc


class TransformationParams(BaseModel):
    """
    Parameters for transformations.
    Flexible structure - fields depend on transformation type.
    """
    input: str  # Which dataset to transform (source name or previous transformation)
    
    # For validate_fields transformation
    validations: list[ValidationRule] | None = None
    
    # For add_fields transformation
    addFields: list[AddFieldRule] | None = None  # Note: camelCase to match JSON
    
    # Could add more params for other transformation types
    # filter_condition: str | None = None
    # join_config: dict | None = None


class Transformation(BaseModel):
    """
    A transformation step in the pipeline.
    Type determines which params are used.
    """
    name: str  # Unique name for this transformation
    type: str  # "validate_fields", "add_fields", "filter", etc
    params: TransformationParams


# =============================================================================
# SINK MODELS
# =============================================================================

class Sink(BaseModel):
    """Defines where to write output data."""
    input: str  # Which dataset to write (transformation name)
    name: str  # Sink identifier
    paths: list[str]  # Can write to multiple paths
    format: str  # JSON, CSV, PARQUET, etc
    saveMode: str  # OVERWRITE, APPEND


# =============================================================================
# DATAFLOW MODEL (Top-level pipeline definition)
# =============================================================================

class Dataflow(BaseModel):
    """
    Complete ETL pipeline definition.
    Contains sources, transformations, and sinks.
    """
    name: str
    sources: list[Source]
    transformations: list[Transformation]
    sinks: list[Sink]


# =============================================================================
# ROOT METADATA MODEL
# =============================================================================

class MetadataConfig(BaseModel):
    """
    Root metadata structure.
    Can contain multiple dataflows.
    """
    dataflows: list[Dataflow]


# =============================================================================
# DATA RECORD MODELS (for processing)
# =============================================================================

class ErrorDetail(BaseModel):
    """Single validation error for a field."""
    field: str
    validation: str
    error_code: str
    message: str


class ProcessedRecord(BaseModel):
    """
    Wrapper for a data record with validation status.
    Used internally during processing.
    """
    data: dict[str, Any]  # The actual record data
    is_valid: bool = True
    errors: list[ErrorDetail] = Field(default_factory=list)
    
    def add_error(self, field: str, validation: str, error_code: str, message: str):
        """Helper to add validation errors."""
        self.is_valid = False
        self.errors.append(ErrorDetail(
            field=field,
            validation=validation,
            error_code=error_code,
            message=message
        ))