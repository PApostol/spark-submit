### Spark-submit

#### 1.4.0 (under development)
Features:
- Report if framework not detected by `spark_submit.system_info()`

Misc/Internal
- Better way to construct the spark-submit command

#### 1.3.0 (2022-07-30)
Features:
- Addition of `spark_submit.SparkJob.get_id()` method to obtain the Spark submission ID
- Use all environment variables from `os.environ` if `use_env_vars=True` in `spark_submit.SparkJob.submit()`
- Additional environment variables can be used in `spark_submit.SparkJob.submit()` with `extra_env_vars` argument

Misc/Internal
- `spark_submit.SparkJob.submit()` now uses keyword arguments only
- Argument `await_result` is changed to `poll_time` in `spark_submit.SparkJob.submit()`
- No default environment variables to be used with `spark_submit.SparkJob.submit()`
- Code standards improvements and general refactoring

#### 1.2.1 (2022-01-16)
Misc/Internal
- Refactor to comply with `pylint`, `isort` and `mypy` checks
- Minor logic improvements

#### 1.2.0 (2021-12-09)
Features
- Added optional `timeout` argument for `spark_submit.SparkJob.submit()`

Improvements
- Improve support for Yarn and Kubernetes clusters on polling and killing submitted tasks

Misc/Internal
- Use dedicated file for default arguments and Spark states
- Use consistent path separators where possible

#### 1.1.0 (2021-11-13)
Features
- Addition of `spark_submit.SparkJob.get_submit_cmd()` method
- Introduction of `spark_submit.SparkJob.concluded` attribute to keep track of submitted job

Improvements
- Improve `spark_submit.system_info()` functionality
- More generic status polling
- Minor refactoring

#### 1.0.1 (2021-10-22)
Bug Fixes
- Fix return code bug in `spark_submit.SparkJob.get_code()`

Improvements
- Change argument order in `spark_submit.SparkJob.submit()`
- Minor improvements

#### 1.0.0 (2021-10-16)
- First release
