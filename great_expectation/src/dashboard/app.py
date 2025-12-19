from flask import Flask, render_template
import json
import os

app = Flask(__name__)

def load_data_quality_metrics():
    metrics = {}
    # Assuming the metrics are stored in a JSON file or similar structure
    # Here we would load the metrics from a source, for example:
    # metrics = json.load(open('path_to_metrics.json'))
    
    # For demonstration, we will create a mock structure
    metrics = {
        "table1": {
            "column1": 99.5,
            "column2": 97.0,
            "column3": 98.5
        },
        "table2": {
            "column1": 98.0,
            "column2": 99.0,
            "column3": 95.0
        }
    }
    
    return metrics

@app.route('/')
def index():
    metrics = load_data_quality_metrics()
    return render_template('index.html', metrics=metrics)

if __name__ == '__main__':
    app.run(debug=True)