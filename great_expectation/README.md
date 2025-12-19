# Data Quality Project

This project implements a comprehensive data quality checking framework using Great Expectations. It focuses on ensuring the integrity and reliability of data across various tables and columns by performing checks on completeness, accuracy, uniqueness, validity, consistency, and timeliness.

## Project Structure

- **src/**: Contains the main source code for the project.
  - **great_expectations/**: Holds the configurations and expectations for data quality checks.
    - **expectations/**: JSON files defining expectations for different data quality checks.
    - **checkpoints/**: Configuration files for running data quality checkpoints.
    - **great_expectations.yml**: Main configuration file for Great Expectations.
  - **dashboard/**: Contains the web application for visualizing data quality metrics.
    - **app.py**: Entry point for the dashboard application.
    - **templates/**: HTML templates for the dashboard.
    - **static/**: Static files including CSS and JavaScript for the dashboard.
  - **data_sources/**: Configuration for connecting to the database.
  - **dq_checks/**: Implementation of various data quality checks.
  - **utils/**: Utility functions for score calculation and threshold validation.
  - **main.py**: Main entry point for executing data quality checks and generating the dashboard.
  
- **tests/**: Contains unit tests for the data quality check implementations.
  
- **config/**: Configuration settings for the data quality checks.
  
- **requirements.txt**: Lists the dependencies required for the project.

## Data Quality Checks

The project implements the following data quality checks:

- **Completeness**: Ensures that all required data is present.
- **Accuracy**: Validates that the data is correct and reliable.
- **Uniqueness**: Checks for duplicate records in the data.
- **Validity**: Ensures that the data conforms to defined formats and standards.
- **Consistency**: Validates that the data is consistent across different datasets.
- **Timeliness**: Checks that the data is up-to-date and relevant.

## Dashboard

The dashboard provides a visual representation of data quality metrics, displaying the percentage of data quality for each column and at the table level. A threshold of 98% is set for data quality, and any metrics falling below this threshold will be highlighted for further investigation.

## Setup Instructions

1. Clone the repository.
2. Install the required dependencies using `pip install -r requirements.txt`.
3. Configure the database connection in `src/data_sources/database_config.py`.
4. Run the main application using `python src/main.py`.
5. Access the dashboard at `http://localhost:5000`.

## Usage Guidelines

- Ensure that the database is accessible and properly configured before running the checks.
- Review the expectations defined in the `great_expectations/expectations` directory to customize the data quality checks as needed.
- Monitor the dashboard for real-time updates on data quality metrics.

## License

This project is licensed under the MIT License.