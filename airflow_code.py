from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

# set up concurrent dags
default_args = {
    'owner': 'airflow',
    'start_date': datetime.now() - timedelta(minutes=1),
    'email': [],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# create a DAG to run the setup for the tasks
setup_template = """
sh /home/cloudera/Documents/practise_exercise_1/mysql_to_hive.sh --setup
"""

dag_setup = DAG('setup',
                start_date=datetime.now() - timedelta(minutes=1),
                default_args=default_args,
                schedule_interval='')

task_setup = BashOperator(task_id='setup_task', bash_command=setup_template, dag=dag_setup)

# create a Dag to schedule loading data to hive and report generation
# report generation must only happen if loading data to hive is successful
dag = DAG('schedule',
          start_date=datetime.now() - timedelta(minutes=1),
          default_args=default_args,
          schedule_interval='@daily',
          max_active_runs=1)


mysql_to_hive = BashOperator(task_id='mysql_to_hive',
                             bash_command="""sh /home/cloudera/Documents/practise_exercise_1/mysql_to_hive.sh --exec """,
                             dag=dag)

csv_to_hive = BashOperator(task_id='csv_to_hive',
                           bash_command="""sh /home/cloudera/Documents/practise_exercise_1/csv_to_hive.sh """, dag=dag)

user_total = BashOperator(task_id='user_total',
                                 bash_command="""impala-shell -f /home/cloudera/Documents/practise_exercise_1/user_total.hql """,
                                 dag=dag)

user_report = BashOperator(task_id='user_report',
                           bash_command="""impala-shell -f /home/cloudera/Documents/practise_exercise_1/user_report.hql """,
                           dag=dag)

user_total.set_upstream(mysql_to_hive)
user_total.set_upstream(csv_to_hive)
user_report.set_upstream(mysql_to_hive)
user_report.set_upstream(csv_to_hive)

# create a dag to clean up data is hdfs, hive and mysql
cleanup_template = """
# clean up hive tables
hive -e 'drop table practical_exercise_1.user'
hive -e 'drop table practical_exercise_1.activitylog'
hive -e 'drop table practical_exercise_1.user_report'
hive -e 'drop table practical_exercise_1.user_total'
hive -e 'drop table practical_exercise_1.user_upload_dump'
hive -e 'drop database if exists practical_exercise_1'

# clean up hdfs 
hdfs dfs -rm -r /user/cloudera/practical_exercise_1
hdfs dfs -ls /user/cloudera

# clean up mysql
mysql -u root -e "drop table practical_exercise_1.activitylog"
mysql -u root -e "drop table practical_exercise_1.user"

# remove sqoop jobs 
sh /home/cloudera/Documents/practise_exercise_1/mysql_to_hive.sh --del
"""

dag_cleanup = DAG('cleanup',
                  start_date=datetime.now() - timedelta(minutes=1),
                  default_args=default_args,
                  schedule_interval='')

task_cleanup = BashOperator(task_id='cleanup_task', bash_command=cleanup_template, dag=dag_cleanup)