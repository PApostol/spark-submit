"""Unit tests for spark_submit/system.py"""
import getpass

from spark_submit.system import _execute_cmd, system_info


def test_execute_cmd():
    res, code = _execute_cmd('whoami')
    assert isinstance(res, str)
    assert isinstance(code, int)
    assert getpass.getuser() in res


def test_system_info():
    keys = ['Spark version', 'Scala version', 'Java version', 'PySpark version']
    ans = system_info()
    assert isinstance(ans, str)
    assert all(k in ans for k in keys)
