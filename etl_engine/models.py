"""Pydantic models for metadata validation."""

from typing import List, Dict, Any
from pydantic import BaseModel, Field


class MetadataConfig(BaseModel):
    """Root metadata configuration."""
    dataflows: List['DataflowConfig']


class DataflowConfig(BaseModel):
    """Complete dataflow definition."""
    name: str
    sources: List['SourceConfig']
    transformations: List['TransformationConfig']
    sinks: List['SinkConfig']


class SourceConfig(BaseModel):
    """Source data configuration."""
    name: str
    path: str
    format: str


class TransformationConfig(BaseModel):
    """Transformation step configuration."""
    name: str
    type: str
    params: Dict[str, Any]


class SinkConfig(BaseModel):
    """Output sink configuration."""
    input: str
    name: str
    paths: List[str]
    format: str
    saveMode: str


# Validation-specific models
class ValidationRule(BaseModel):
    """Field validation rule."""
    field: str
    validations: List[str]


class ValidateFieldsParams(BaseModel):
    """Parameters for validate_fields transformation."""
    input: str
    validations: List[ValidationRule]


class AddFieldParams(BaseModel):
    """Parameters for add_fields transformation."""
    name: str
    function: str


class AddFieldsParams(BaseModel):
    """Parameters for add_fields transformation."""
    input: str
    addFields: List[AddFieldParams] = Field(alias="addFields")

    class Config:
        populate_by_name = True