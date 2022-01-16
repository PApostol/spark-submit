"""spark-submit module"""

from .__info__ import (__author__, __author_email__, __bugtrack_url__,
                       __description__, __license__, __maintainer__, __title__,
                       __url__, __version__)
from .sparkjob import SparkJob
from .system import system_info

__all__ = ['SparkJob', 'system_info']
