#!/bin/bash

parallelism = `wc -l tables.lst`

function popStack(){
	: ${1?'Missing stack name'}

	# check if array not void
	local pointerStack=$(<$pointerStackFile)
	[ $pointerStack -ge $initialSizeStack ] && return 1

	eval "$1"=${listOfTables[$pointerStack]}

	(( pointerStack=$pointerStack+1))
	echo $pointerStack > $pointerStackFile
	return 0
}

function doSqoop(){
	local id=$1	# this id should be unique betwween all the instances of doSqoop
	local sqoopCmd=`sed '$id!d' tables.lst`
	local logFileSummary=$databaseBaseDir/process_$id-summary.log
	local logFileRaw=$databaseBaseDir/process_$id-raw.log
	
	echo -e "\n\n############################# `date` Starting new execution  #############################" >> $logFileSummary

	while true; do
		# get the name of the next table to sqoop
		popStack $sqoopCmd
		[ $? -ne 0 ] && break

		echo "[`date`] Executing command -- $sqoopCmd" | tee -a $logFileSummary $logFileRaw

		#sqoop import -D mapreduce.job.queuename=$queue -D mapreduce.job.ubertask.enable=true --connect "jdbc:sqlserver://$origServer:1433; database=$origDatabase; username=myUser; password=myPassword" --hive-import --hive-database $hiveDatabase --fields-terminated-by '\t' --null-string '' --null-non-string '' -m 1 --outdir $dirJavaGeneratedCode --query "select a.* from $origTable a where \$CONDITIONS" --target-dir $targetTmpHdfsDir/$myTable --hive-table $myTable >> $logFileRaw 2>> $logFileRaw
		exec $sqoopCmd
		echo "Tail of : $logFileRaw" >> $logFileSummary
		tail -6 $logFileRaw  >> $logFileSummary
	done

	echo -e "\n############################# `date` Ending execution  #############################" >> $logFileSummary
}

for i in `seq $parallelism`; do
	sleep 0.1 # to avoid subprocesses to pop the stack at the same time
	(doSqoop $i) &
	if i > 10 
		sleep 2m
done

wait