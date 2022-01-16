"""Exceptions raised by SparkJob class"""

class SparkSubmitError(Exception):
    """Raised if spark-submit command fails"""


class SparkJobKillError(Exception):
    """Raised if spark-submit command fails"""
