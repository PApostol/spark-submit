## Spark-submit

[![PyPI version](https://badge.fury.io/py/spark-submit.svg)](https://badge.fury.io/py/spark-submit)
[![Downloads](https://static.pepy.tech/personalized-badge/spark-submit?period=month&units=international_system&left_color=grey&right_color=green&left_text=total%20downloads)](https://pepy.tech/project/spark-submit)
[![PyPI - Downloads](https://img.shields.io/pypi/dm/spark-submit)](https://pypi.org/project/spark-submit/)
[![](https://img.shields.io/badge/python-3.6+-blue.svg)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/License-MIT-blue)](#license "Go to license section")
[![contributions welcome](https://img.shields.io/badge/contributions-welcome-brightgreen.svg?style=flat)](https://github.com/PApostol/spark-submit/issues)

#### TL;DR: Python manager for spark-submit jobs

### Description
This package allows for submission and management of Spark jobs in Python scripts via [Apache Spark's](https://spark.apache.org/) `spark-submit` functionality.

### Installation
The easiest way to install is using `pip`:

`pip install spark-submit`

To install from source:
```
git clone https://github.com/PApostol/spark-submit.git
cd spark-submit
python setup.py install
```

For usage details check `help(spark_submit)`.

### Usage Examples
Spark arguments can either be provided as keyword arguments or as an unpacked dictionary.

##### Simple example:
```
from spark_submit import SparkJob

app = SparkJob('/path/some_file.py', master='local', name='simple-test')
app.submit()

print(app.get_state())
```
##### Another example:
```
from spark_submit import SparkJob

spark_args = {
    'master': 'spark://some.spark.master:6066',
    'deploy_mode': 'cluster',
    'name': 'spark-submit-app',
    'class': 'main.Class',
    'executor_memory': '2G',
    'executor_cores': '1',
    'total_executor_cores': '2',
    'verbose': True,
    'conf': ["spark.foo.bar='baz'", "spark.x.y='z'"],
    'main_file_args': '--foo arg1 --bar arg2'
    }

app = SparkJob('s3a://bucket/path/some_file.jar', **spark_args)
print(app.get_submit_cmd(multiline=True))
print(app.env_vars)

# monitor state in the background every x seconds with `await_result=x`
app.submit(await_result=10, use_env_vars=True)

print(app.get_state()) # 'SUBMITTED'

while not app.concluded:
    # do other stuff...
    print(app.get_state()) # 'RUNNING'

print(app.get_state()) # 'FINISHED'
```

#### Examples of `spark-submit` to `spark_args` dictionary:
##### A `client` example:
```
~/spark_home/bin/spark-submit \
--master spark://some.spark.master:7077 \
--name spark-submit-job \
--total-executor-cores 8 \
--executor-cores 4 \
--executor-memory 4G \
--driver-memory 2G \
--py-files /some/utils.zip \
--files /some/file.json \
/path/to/pyspark/file.py --data /path/to/data.csv
```
##### becomes
```
spark_args = {
    'master': 'spark://some.spark.master:7077',
    'name': 'spark_job_client',
    'total_executor_cores: '8',
    'executor_cores': '4',
    'executor_memory': '4G',
    'driver_memory': '2G',
    'py_files': '/some/utils.zip',
    'files': '/some/file.json',
    'main_file_args': '--data /path/to/data.csv'
    }
main_file = '/path/to/pyspark/file.py'
app = SparkJob(main_file, **spark_args)
```
##### A `cluster` example:
```
~/spark_home/bin/spark-submit \
--master spark://some.spark.master:6066 \
--deploy-mode cluster \
--name spark_job_cluster \
--jars "s3a://mybucket/some/file.jar" \
--conf "spark.some.conf=foo" \
--conf "spark.some.other.conf=bar" \
--total-executor-cores 16 \
--executor-cores 4 \
--executor-memory 4G \
--driver-memory 2G \
--class my.main.Class \
--verbose \
s3a://mybucket/file.jar "positional_arg1" "positional_arg2"
```
##### becomes
```
spark_args = {
    'master': 'spark://some.spark.master:6066',
    'deploy_mode': 'cluster',
    'name': 'spark_job_cluster',
    'jars': 's3a://mybucket/some/file.jar',
    'conf': ["spark.some.conf='foo'", "spark.some.other.conf='bar'"], # note the use of quotes
    'total_executor_cores: '16',
    'executor_cores': '4',
    'executor_memory': '4G',
    'driver_memory': '2G',
    'class': 'my.main.Class',
    'verbose': True,
    'main_file_args': '"positional_arg1" "positional_arg2"'
    }
main_file = 's3a://mybucket/file.jar'
app = SparkJob(main_file, **spark_args)
```

#### Testing

You can do some simple testing with local mode Spark after cloning the repo:

`python tests/run_test.py`

Note any additional requirements for running the tests: `pip install -r tests/requirements.txt`

#### Additional methods

`spark_submit.system_info()`: Collects Spark related system information, such as versions of spark-submit, Scala, Java, PySpark, Python and OS

`spark_submit.SparkJob.kill()`: Kills the running Spark job (cluster mode only)

`spark_submit.SparkJob.get_code()`: Gets the spark-submit return code

`spark_submit.SparkJob.get_output()`: Gets the spark-submit stdout

### License

Released under [MIT](/LICENSE) by [@PApostol](https://github.com/PApostol).

- You can freely modify and reuse.
- The original license must be included with copies of this software.
- Please link back to this repo if you use a significant portion the source code.
