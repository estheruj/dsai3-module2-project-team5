def calculate_quality_score(check_results):
    total_checks = len(check_results)
    passed_checks = sum(1 for result in check_results if result['status'] == 'pass')
    return (passed_checks / total_checks) * 100 if total_checks > 0 else 0

def evaluate_data_quality(data_quality_metrics):
    quality_scores = {}
    for table, metrics in data_quality_metrics.items():
        quality_scores[table] = {
            'completeness': calculate_quality_score(metrics['completeness']),
            'accuracy': calculate_quality_score(metrics['accuracy']),
            'uniqueness': calculate_quality_score(metrics['uniqueness']),
            'validity': calculate_quality_score(metrics['validity']),
            'consistency': calculate_quality_score(metrics['consistency']),
            'timeliness': calculate_quality_score(metrics['timeliness']),
        }
    return quality_scores

def check_threshold(quality_scores, threshold=98):
    threshold_results = {}
    for table, scores in quality_scores.items():
        threshold_results[table] = {metric: score >= threshold for metric, score in scores.items()}
    return threshold_results