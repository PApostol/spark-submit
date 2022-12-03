"""Exceptions raised by SparkJob class"""


class SparkException(Exception):
    """Generic spark-submit exception"""


class SparkSubmitError(SparkException):
    """Raised if spark-submit command fails"""


class SparkJobKillError(SparkException):
    """Raised if spark-submit command fails"""
