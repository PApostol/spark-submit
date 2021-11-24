from spark_submit import SparkJob

print('---Running example on local mode---')
job = SparkJob('tests/resources/pyspark_example.py', main_file_args ='100000')

print(f'spark-submit command:\n {job.get_submit_cmd(multiline=True)}')
print('\nJob running...\n')
job.submit()

print(f'Job output:\n{job.get_output()}\n')
print(f'Job state: {job.get_state()}\n')
print(f'Job return code: {job.get_code()}\n')
