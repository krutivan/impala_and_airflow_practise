#create sqoop jobs
setup()
{
	# create a sqoop job to copy activitylog table
	nohup sqoop metastore & 

	sqoop job \
	--meta-connect jdbc:hsqldb:hsql://localhost:16000/sqoop \
	--create practical_exercise_1.activitylog \
	-- import \
	--connect jdbc:mysql://localhost/practical_exercise_1 \
	--username root \
	--password-file /user/cloudera/root_pwd.txt \
	--table activitylog \
	--m 2 \
	--hive-import \
	--hive-database practical_exercise_1 \
	--hive-table activitylog \
	--incremental append \
	--check-column id \
	--last-value 0

	echo "sqoop jobs created"

	sqoop job \
	--meta-connect jdbc:hsqldb:hsql://localhost:16000/sqoop \
	--list;

	#create hive database if not exists
	hive -e 'create database if not exists practical_exercise_1'
	hive -e 'CREATE TABLE if NOT EXISTS practical_exercise_1.user_total(time_ran timestamp, total_users INT, users_added INT)'
}


#deletes exisiting sqoop jobs
delete_jobs()
{
	sqoop job \
	--meta-connect jdbc:hsqldb:hsql://localhost:16000/sqoop \
	--delete practical_exercise_1.activitylog
}

exec_activitylog()
{
	sqoop job \
	--meta-connect jdbc:hsqldb:hsql://localhost:16000/sqoop \
	--exec practical_exercise_1.activitylog
}

exec_user()
{
	sqoop import \
	--connect jdbc:mysql://localhost/practical_exercise_1 \
	--username root \
	--password-file /user/cloudera/root_pwd.txt \
	--table user \
	--m 2 \
	--hive-overwrite \
	--hive-import \
	--hive-database practical_exercise_1 \
	--hive-table user
}

#execute sqoop jobs
exec_jobs()
{
	nohup sqoop metastore & 
	
	exec_activitylog
	exec_user

	echo "Executed the sqoop jobs to copy user and activitylog to hive"
}


if [[ $1 = '--setup' ]]
	then setup
elif [[ $1 = '--exec' ]]; then
	exec_jobs
elif [[ $1 = '--del' ]]; then
	delete_jobs	
else 
	echo "sh mysql_to_hive.sh [--setup or --exec or --del]"
fi
