from spark_submit import SparkJob

print('---Running example on local mode---')
job = SparkJob('tests/resources/pyspark_example.py', main_file_args ='100000')

print('spark-submit command:\n' + job.get_submit_cmd(multiline=True))
print('\nJob running...\n')
job.submit()

print('Job output:\n{0}\n'.format(job.get_output()))
print('Job state: {0}\n'.format(job.get_state()))
print('Job return code: {0}\n'.format(job.get_code()))
