from great_expectations.data_context import DataContext
from great_expectations.core.batch import BatchRequest
from great_expectations.core.batch import RuntimeBatchRequest
import pandas as pd

class CompletenessCheck:
    def __init__(self, data_context_path, threshold=0.98):
        self.data_context = DataContext(data_context_path)
        self.threshold = threshold

    def run_completeness_check(self, table_name, column_name):
        batch_request = BatchRequest(
            datasource="your_datasource_name",
            data_connector_name="your_data_connector_name",
            data_asset_name=table_name,
        )
        
        # Load the data
        batch = self.data_context.get_batch(batch_request)
        df = batch.to_dataframe()

        # Calculate completeness
        total_rows = df.shape[0]
        non_null_rows = df[column_name].notnull().sum()
        completeness_percentage = non_null_rows / total_rows

        return completeness_percentage

    def check_all_columns(self, table_name):
        batch_request = BatchRequest(
            datasource="your_datasource_name",
            data_connector_name="your_data_connector_name",
            data_asset_name=table_name,
        )
        
        # Load the data
        batch = self.data_context.get_batch(batch_request)
        df = batch.to_dataframe()

        results = {}
        for column in df.columns:
            completeness_percentage = self.run_completeness_check(table_name, column)
            results[column] = completeness_percentage

        return results

    def evaluate_completeness(self, table_name):
        completeness_results = self.check_all_columns(table_name)
        evaluation = {column: (percentage >= self.threshold) for column, percentage in completeness_results.items()}
        return evaluation

# Example usage
# completeness_check = CompletenessCheck(data_context_path="path/to/great_expectations/directory")
# results = completeness_check.evaluate_completeness("your_table_name")
# print(results)