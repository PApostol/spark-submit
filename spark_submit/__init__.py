"""spark-submit module"""

from .sparkjob import SparkJob
from .system import system_info
from .__info__ import (
    __title__,
    __author__,
    __author_email__,
    __maintainer__,
    __license__,
    __version__,
    __description__,
    __url__,
    __bugtrack_url__
)

__all__ = ['SparkJob', 'system_info']
