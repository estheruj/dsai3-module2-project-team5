import os
import json
from flask import Flask, render_template
from data_sources.database_config import get_database_connection
from dq_checks.completeness import check_completeness
from dq_checks.accuracy import check_accuracy
from dq_checks.uniqueness import check_uniqueness
from dq_checks.validity import check_validity
from dq_checks.consistency import check_consistency
from dq_checks.timeliness import check_timeliness
from utils.score_calculator import calculate_scores
from utils.threshold_validator import validate_threshold

app = Flask(__name__)

@app.route('/')
def index():
    connection = get_database_connection()
    
    completeness_results = check_completeness(connection)
    accuracy_results = check_accuracy(connection)
    uniqueness_results = check_uniqueness(connection)
    validity_results = check_validity(connection)
    consistency_results = check_consistency(connection)
    timeliness_results = check_timeliness(connection)

    scores = calculate_scores(completeness_results, accuracy_results, uniqueness_results, validity_results, consistency_results, timeliness_results)
    
    data_quality_metrics = {
        "completeness": scores['completeness'],
        "accuracy": scores['accuracy'],
        "uniqueness": scores['uniqueness'],
        "validity": scores['validity'],
        "consistency": scores['consistency'],
        "timeliness": scores['timeliness'],
    }

    overall_quality = sum(data_quality_metrics.values()) / len(data_quality_metrics)

    return render_template('index.html', data_quality_metrics=data_quality_metrics, overall_quality=overall_quality)

if __name__ == '__main__':
    app.run(debug=True)