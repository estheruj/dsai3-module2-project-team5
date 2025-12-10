from great_expectations.data_context import DataContext
from great_expectations.core.batch import BatchRequest
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.exceptions import GreatExpectationsError
import pandas as pd

class ValidityCheck:
    def __init__(self, data_context_path, threshold=0.98):
        self.data_context = DataContext(data_context_path)
        self.threshold = threshold

    def run_validity_check(self, table_name, column_name, expected_values):
        try:
            batch_request = BatchRequest(
                datasource_name="your_datasource_name",
                data_connector_name="your_data_connector_name",
                data_asset_name=table_name,
            )
            suite_name = "validity_suite"
            validator = self.data_context.get_validator(batch_request=batch_request, expectation_suite_name=suite_name)

            # Check for validity
            results = validator.expect_column_values_to_be_in_set(column_name, expected_values)
            return self.calculate_validity_percentage(results)

        except GreatExpectationsError as e:
            print(f"Error running validity check: {e}")
            return None

    def calculate_validity_percentage(self, results):
        total_values = results["result"]["observed_value_count"]
        valid_values = results["result"]["success"] * total_values
        validity_percentage = (valid_values / total_values) * 100 if total_values > 0 else 0
        return validity_percentage

    def check_all_columns(self, table_name, column_expectations):
        results = {}
        for column_name, expected_values in column_expectations.items():
            validity_percentage = self.run_validity_check(table_name, column_name, expected_values)
            results[column_name] = validity_percentage
        return results

    def check_table_validity(self, table_name, column_expectations):
        column_results = self.check_all_columns(table_name, column_expectations)
        overall_validity = sum(column_results.values()) / len(column_results) if column_results else 0
        return overall_validity, column_results
