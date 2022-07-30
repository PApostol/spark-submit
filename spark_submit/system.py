"""System related functionalities"""
import logging
import os
import platform
import re
import subprocess
import sys
from typing import Any, Dict, Optional, Tuple


def _execute_cmd(cmd: str, timeout: Optional[int] = None, silent: bool = True, **kwargs: Any) -> Tuple[str, int]:

    with subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        shell=True,
        **kwargs,
    ) as process:
        output, _ = process.communicate(timeout=timeout)
        res = output.decode() if isinstance(output, bytes) else output
        code = process.returncode

    if code != 0 and not silent:
        logging.warning(res)
    return res, code


def system_info() -> str:
    """Collects Spark related system information, such as versions of
       spark-submit, Scala, Java, PySpark, Python and OS

    Returns:
        str: system information
    """

    def _quote_spaces(text: str) -> str:
        return f'"{text}"' if ' ' in text else text

    spark_home = os.environ.get('SPARK_HOME', os.path.expanduser('~/spark_home')).replace(os.path.sep, '/')
    info_cmd = f'{_quote_spaces(spark_home)}/bin/spark-submit --version'

    java_home = os.environ.get('JAVA_HOME', '').replace(os.path.sep, '/')
    if java_home:
        java_bin = f'{java_home}/bin/java'
        info_cmd += f' ; {_quote_spaces(java_bin)} -version'
    else:
        info_cmd += ' ; java -version'

    info_cmd += f' ; {_quote_spaces(sys.executable)} -m pip show pyspark'
    if platform.system() == 'Windows':
        info_cmd = info_cmd.replace(' ; ', ' & ')

    info_stdout, _ = _execute_cmd(info_cmd, silent=False)
    info_re = {
        'Spark version': '  version (.+)',
        'Scala version': 'scala version (.+?),',
        'Java version': 'version "(.+)"',
        'PySpark version': 'Version: (.+)',
    }

    sys_info: Dict[str, str] = {}
    for key, val in info_re.items():
        i = re.findall(val, info_stdout, re.IGNORECASE)
        if i:
            sys_info[key] = i[0].strip()

    sys_info['Python version'] = sys.version.split(' ', maxsplit=1)[0]
    sys_info['OS'] = platform.platform()
    return '\n'.join([f'{key}: {val}' for key, val in sys_info.items()])
