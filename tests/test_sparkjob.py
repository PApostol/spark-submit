"""Unit tests for spark_submit/sparkjob.py"""
import os

import pyspark
import pytest

from spark_submit.sparkjob import SparkJob

MAIN_FILE = 'resources/pyspark_example.py'
SPARK_HOME = os.path.dirname(pyspark.__file__).replace(os.path.sep, '/')

MOCK_JOB = SparkJob(
    main_file=MAIN_FILE,
    spark_home=SPARK_HOME,
    verbose=True,
    total_executor_cores=4,
    conf=["'foo'='bar'"],
    main_file_args='conf.json',
)


def test_missing_main_file():
    file = 'missing.py'
    with pytest.raises(FileNotFoundError) as err:
        SparkJob(main_file=file)

    assert str(err.value) == f'File {file} does not exist.'


def test_missing_spark_home():
    spark_home = '.'
    with pytest.raises(FileNotFoundError) as err:
        SparkJob(main_file=MAIN_FILE, spark_home=spark_home)

    assert f'bin/spark-submit was not found in "{spark_home}"' in str(err.value)


def test_check_spark_submit_cmd():
    expected_cmd = f"{SPARK_HOME}/bin/spark-submit --master local[*] --name spark-submit-task --deploy-mode client --driver-memory 1g --executor-memory 1g --executor-cores 1 --total-executor-cores 4 --verbose --conf 'foo'='bar' resources/pyspark_example.py conf.json"
    assert expected_cmd == MOCK_JOB.get_submit_cmd()


def test_intial_get_state():
    assert MOCK_JOB.get_state() == ''


def test_intial_get_output():
    assert MOCK_JOB.get_output() == ''


def get_intial_code():
    assert MOCK_JOB.get_code() == -1


def test_intial_get_id():
    assert MOCK_JOB.get_id() == ''
