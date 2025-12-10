from great_expectations.data_context import DataContext
import pandas as pd

class DataQualityCheck:
    def __init__(self, context_path, threshold=0.98):
        self.context = DataContext(context_path)
        self.threshold = threshold

    def check_accuracy(self, table_name, column_name):
        suite = self.context.get_expectation_suite("accuracy_suite")
        batch = self.context.get_batch({
            "datasource": "your_datasource_name",
            "data_asset_name": table_name,
            "expectation_suite_name": suite.expectation_suite_name
        })

        results = batch.validate(expectation_suite=suite)
        accuracy_score = self.calculate_accuracy(results, column_name)
        return accuracy_score

    def calculate_accuracy(self, results, column_name):
        total_records = len(results['results'])
        accurate_records = sum(1 for result in results['results'] if result['success'])
        accuracy_percentage = accurate_records / total_records if total_records > 0 else 0
        return accuracy_percentage

    def run_checks(self, tables):
        quality_results = {}
        for table in tables:
            quality_results[table] = {}
            for column in tables[table]:
                accuracy = self.check_accuracy(table, column)
                quality_results[table][column] = accuracy >= self.threshold
        return quality_results

# Example usage
if __name__ == "__main__":
    context_path = "path/to/your/great_expectations/directory"
    dq_check = DataQualityCheck(context_path)
    tables = {
        "table_name_1": ["column1", "column2"],
        "table_name_2": ["column1", "column2"]
    }
    results = dq_check.run_checks(tables)
    print(results)