from great_expectations.data_context import DataContext
from great_expectations.core.batch import BatchRequest
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.exceptions import GreatExpectationsError
import pandas as pd

class UniquenessCheck:
    def __init__(self, data_context_path, threshold=0.98):
        self.data_context = DataContext(data_context_path)
        self.threshold = threshold

    def run_uniqueness_check(self, table_name, column_name):
        try:
            # Create a batch request for the specified table and column
            batch_request = BatchRequest(
                datasource_name="your_datasource_name",
                data_connector_name="your_data_connector_name",
                data_asset_name=table_name,
                # Assuming you are using a SQLAlchemy datasource
                # You can also use RuntimeBatchRequest for in-memory data
            )

            # Load the data
            batch = self.data_context.get_batch(batch_request)
            df = batch.to_pandas()

            # Check uniqueness
            unique_count = df[column_name].nunique()
            total_count = df[column_name].count()
            uniqueness_percentage = unique_count / total_count

            return uniqueness_percentage >= self.threshold, uniqueness_percentage

        except GreatExpectationsError as e:
            print(f"Error running uniqueness check: {e}")
            return False, None

    def check_all_columns(self, table_name):
        results = {}
        try:
            # Create a batch request for the specified table
            batch_request = BatchRequest(
                datasource_name="your_datasource_name",
                data_connector_name="your_data_connector_name",
                data_asset_name=table_name,
            )

            # Load the data
            batch = self.data_context.get_batch(batch_request)
            df = batch.to_pandas()

            for column in df.columns:
                is_unique, uniqueness_percentage = self.run_uniqueness_check(table_name, column)
                results[column] = {
                    "is_unique": is_unique,
                    "uniqueness_percentage": uniqueness_percentage
                }

            return results

        except GreatExpectationsError as e:
            print(f"Error checking all columns for uniqueness: {e}")
            return None

# Example usage
if __name__ == "__main__":
    uniqueness_check = UniquenessCheck(data_context_path="path/to/great_expectations/directory")
    table_results = uniqueness_check.check_all_columns("your_table_name")
    print(table_results)