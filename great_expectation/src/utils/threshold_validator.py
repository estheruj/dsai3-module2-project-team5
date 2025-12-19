def validate_threshold(score, threshold=98):
    """
    Validate if the given score meets the specified threshold.

    Parameters:
    score (float): The data quality score to validate.
    threshold (float): The threshold percentage to compare against (default is 98).

    Returns:
    bool: True if the score meets or exceeds the threshold, False otherwise.
    """
    return score >= threshold

def validate_scores(scores, threshold=98):
    """
    Validate a list of scores against the specified threshold.

    Parameters:
    scores (list): A list of data quality scores to validate.
    threshold (float): The threshold percentage to compare against (default is 98).

    Returns:
    dict: A dictionary with scores and their validation results.
    """
    return {score: validate_threshold(score, threshold) for score in scores}