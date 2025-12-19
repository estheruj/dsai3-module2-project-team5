from datetime import datetime
import pandas as pd
from great_expectations import DataContext

class TimelinessCheck:
    def __init__(self, data_context: DataContext, threshold: float = 0.98):
        self.data_context = data_context
        self.threshold = threshold

    def check_timeliness(self, df: pd.DataFrame, timestamp_column: str, expected_frequency: str) -> float:
        current_time = datetime.now()
        df[timestamp_column] = pd.to_datetime(df[timestamp_column])
        
        # Calculate the time difference between the current time and the latest timestamp in the data
        latest_timestamp = df[timestamp_column].max()
        time_difference = current_time - latest_timestamp
        
        # Determine if the data is timely based on the expected frequency
        if expected_frequency == 'daily':
            is_timely = time_difference.days <= 1
        elif expected_frequency == 'hourly':
            is_timely = time_difference.total_seconds() <= 3600
        else:
            raise ValueError("Unsupported frequency. Use 'daily' or 'hourly'.")

        return 1.0 if is_timely else 0.0

    def run_checks(self, table_name: str, timestamp_column: str, expected_frequency: str) -> dict:
        # Load the data from the data context
        batch = self.data_context.get_batch(table_name)
        df = batch.data

        # Perform the timeliness check
        timeliness_score = self.check_timeliness(df, timestamp_column, expected_frequency)

        # Prepare the result
        result = {
            "table_name": table_name,
            "timestamp_column": timestamp_column,
            "timeliness_score": timeliness_score,
            "is_above_threshold": timeliness_score >= self.threshold
        }

        return result