# takes the csv having user upload dumps and puts it into hdfs
# note that when users deleted the historical dump is still preserved

csv_source_dir='/home/cloudera/Documents/practise_exercise_1/'
csv_processed_dir='/home/cloudera/Documents/processed_csv/'
hdfs_path='/user/cloudera/practical_exercise_1/user_upload_dump/'

mkdir -p $csv_processed_dir
#if hdfs path does exist create it
hdfs dfs -test -d $hdfs_path
	if [ $? != 0 ]
		then 
			hadoop fs -mkdir -p $hdfs_path
		fi 

#get csv files present in directory
array=(`find $csv_source_dir -type f -name '*.csv'`)

for i in "${array[@]}"
do :
	echo $i
done
#copy csv files to hdfs
for i in "${array[@]}"
do :
	#replace ':' in filename to'_'
	filename=$(echo $(basename $i .csv) | sed 's/:/_/g').csv
	
	#rename file
	mv $i $csv_source_dir$filename

	#copy to hdfs
	hadoop fs -copyFromLocal $csv_source_dir$filename $hdfs_path

	#move file to processed folder
	mv $csv_source_dir$filename $csv_processed_dir$filename
done

echo "----------------------------------------------------"
echo "updated hdfs..."
hdfs dfs -ls $hdfs_path

# create external table pointing to the hdfs location
hive -e "CREATE EXTERNAL TABLE IF NOT EXISTS practical_exercise_1.user_upload_dump (user_id INT,filename STRING, timestamp INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE location '/user/cloudera/practical_exercise_1/user_upload_dump'"