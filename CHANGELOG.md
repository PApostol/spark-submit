### Spark-submit

##### 1.2.0 (2021-12-09)
- Improve support for Yarn and Kubernetes clusters on polling and killing submitted tasks
- Use dedicated file for default arguments and Spark states
- Added optional `timeout` argument for `submit()`
- Use consistent path separators where possible

##### 1.1.0 (2021-11-13)
- Use of `concluded` attribute to keep track of submitted job
- Addition of `get_submit_cmd()` method
- Improve `system_info()` functionality
- More generic status polling
- Various improvements & bug fixes

##### 1.0.1 (2021-10-22)
- Fix return code bug in `get_code()`
- Change argument order in `submit()`
- Minor improvements

##### 1.0.0 (2021-10-16)
- First release
