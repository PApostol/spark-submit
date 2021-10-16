import logging
import os
import platform
import re
import subprocess
import sys

def _get_env_vars() -> dict:
    return {'JAVA_HOME': os.environ.get('JAVA_HOME', ''),
            'PYSPARK_PYTHON': os.environ.get('PYSPARK_PYTHON', sys.executable),
            'PYSPARK_DRIVER_PYTHON': os.environ.get('PYSPARK_DRIVER_PYTHON', sys.executable)
            }


def _execute_cmd(cmd: str) -> str:
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True)
    o, _ = p.communicate()
    o = o.decode()

    if p.returncode != 0:
        logging.warning(o)
    else:
        return o


def system_info() -> str:
    """Collects Spark related system information, such as versions of spark-submit, Scala, Java, Python and OS

    Returns:
        str: system information
   """
    spark_bin = os.environ.get('SPARK_HOME', os.path.expanduser('~/spark_home')) + '/bin/spark-submit'
    cmd = spark_bin + ' --version'

    java_home = os.environ.get('JAVA_HOME', '')
    if java_home:
        cmd += ' ; ' + java_home + '/bin/java -version'

    info_stdout = _execute_cmd(cmd)
    info_re = {'Spark version': 'version (.+)',
               'Scala version': 'scala version (.+?),',
               'Java version': 'openjdk version \"(.+)\"'
              }

    info = {}
    for k, v in info_re.items():
        i = re.findall(v, info_stdout, re.IGNORECASE)
        if i:
            info[k] = i[0]

    info['Python version'] = sys.version.split(' ')[0]
    info['OS'] = platform.platform()
    return '\n'.join([f'{k}: {v}' for k, v in info.items()])
