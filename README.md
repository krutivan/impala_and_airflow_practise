# impala_and_airflow_practise

This exercise has an airflow job - airflow_code.py which creates three possible dags as follows:
</b>setup</b> - used to setup sqoop jobs and database. 
```sh
airflow trigger_dag setup
```

<b>schedule</b>b> - used to schedule data transfer to hive and report generation. This DAG is scheduled to run daily and does not have to be manually triggered. However the setup dag must be run before this runs.

<b>cleanup </b>b>- used to clean up sqoop jobs, databse tables and hdfs folders.
```sh
airflow trigger_dag cleanup
```

The dag 'schedule' has 4 tasks:
<ol>
	<li> mysql_to_hive - copies user and activity_log tables from mysql to hive </li>
	<li> csv_to_hive - loads user upload dump csv to hdfs and creates an external table referenceing the hdfs location</li>
	<li> user_total - generates user_total report which includes total users and new users added. </li>
	<li> user_report - generates a report containing user related metrics such as activity, number of uploads/deletes etc. </li>
</ol>

The dags are ordered as follows:
```python
user_total.set_upstream(mysql_to_hive)
user_total.set_upstream(csv_to_hive)
user_report.set_upstream(mysql_to_hive)
user_report.set_upstream(csv_to_hive)
```
This ensures that both tasks 'mysql_to_hive' and 'csv_to_hive' complete before reports generations happen.
Both report generation tasks may happen in parallel once the loading tasks are completed.

