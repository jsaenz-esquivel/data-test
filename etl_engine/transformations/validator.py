"""
Validator transformation - validates fields based on rules.
"""

from typing import Any, Callable

from etl_engine.transformations.base import BaseTransformation
from etl_engine.models import ProcessedRecord, TransformationParams
from etl_engine.exceptions import ValidationRuleNotFound


class ValidatorTransformation(BaseTransformation):
    """
    Validates records based on field rules.
    Splits records into OK (passed validation) and KO (failed validation).
    """
    
    # Validation functions registry
    # Maps validation rule name to validation function
    VALIDATION_RULES: dict[str, Callable[[Any], bool]] = {
        'notNull': lambda value: value is not None,
        'notEmpty': lambda value: value is not None and str(value).strip() != '',
    }
    
    # Error messages for each validation rule
    ERROR_MESSAGES = {
        'notNull': 'Field cannot be null',
        'notEmpty': 'Field cannot be empty',
    }
    
    def __init__(self, name: str, params: TransformationParams):
        super().__init__(name, params)
        
        # Validate that all rules exist
        if params.validations:
            for validation_rule in params.validations:
                for rule_name in validation_rule.validations:
                    if rule_name not in self.VALIDATION_RULES:
                        raise ValidationRuleNotFound(
                            f"Validation rule '{rule_name}' not found. "
                            f"Available: {list(self.VALIDATION_RULES.keys())}"
                        )
    
    def execute(self, records: list[ProcessedRecord]) -> dict[str, list[ProcessedRecord]]:
        """
        Validate all records and split into OK/KO.
        
        Args:
            records: Records to validate
            
        Returns:
            {
                "validation_ok": [valid records],
                "validation_ko": [invalid records]
            }
        """
        ok_records = []
        ko_records = []
        
        for record in records:
            # Skip already invalid records
            if not record.is_valid:
                ko_records.append(record)
                continue
            
            # Apply all validation rules
            self._validate_record(record)
            
            # Separate based on result
            if record.is_valid:
                ok_records.append(record)
            else:
                ko_records.append(record)
        
        return {
            self._get_output_name("_ok"): ok_records,
            self._get_output_name("_ko"): ko_records,
        }
    
    def _validate_record(self, record: ProcessedRecord) -> None:
        """
        Apply all validation rules to a record.
        Modifies record in place by adding errors.
        
        Args:
            record: Record to validate
        """
        if not self.params.validations:
            return
        
        for validation_rule in self.params.validations:
            field_name = validation_rule.field
            
            # Get field value from record data
            field_value = record.data.get(field_name)
            
            # Apply each validation rule
            for rule_name in validation_rule.validations:
                validator_func = self.VALIDATION_RULES[rule_name]
                
                if not validator_func(field_value):
                    # Validation failed - add error
                    error_code = self._get_error_code(rule_name)
                    error_message = self.ERROR_MESSAGES.get(
                        rule_name,
                        f"Validation '{rule_name}' failed"
                    )
                    
                    record.add_error(
                        field=field_name,
                        validation=rule_name,
                        error_code=error_code,
                        message=error_message
                    )
    
    @staticmethod
    def _get_error_code(rule_name: str) -> str:
        """
        Generate error code from rule name.
        
        Args:
            rule_name: Validation rule name (e.g., "notEmpty")
            
        Returns:
            Error code (e.g., "NOT_EMPTY")
        """
        # Convert camelCase to UPPER_SNAKE_CASE
        # notEmpty -> NOT_EMPTY
        import re
        s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', rule_name)
        return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).upper()
    
    @classmethod
    def add_validation_rule(cls, rule_name: str, validator_func: Callable[[Any], bool], 
                           error_message: str) -> None:
        """
        Add a custom validation rule dynamically.
        
        Args:
            rule_name: Name of the validation rule
            validator_func: Function that returns True if valid
            error_message: Error message for this rule
            
        Example:
            ValidatorTransformation.add_validation_rule(
                'isPositive',
                lambda x: isinstance(x, (int, float)) and x > 0,
                'Value must be positive'
            )
        """
        cls.VALIDATION_RULES[rule_name] = validator_func
        cls.ERROR_MESSAGES[rule_name] = error_message