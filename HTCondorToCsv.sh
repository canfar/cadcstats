preOS=$1
condHis=$2

col="QDate Requirements Owner VMLoc VMInstanceType RequestMemory RequestDisk MemoryUsage DiskUsage LastRemoteHost DiskUsage ImageSize ResidentSetSize \
JobStatus CommittedTime CommittedSuspensionTime CumulativeSlotTime NumJobStarts RequestCpus JobStartDate VMName RemoveReason CompletionDate"

extra="VMCPUCores VMMem VMStorage"

if [ "$preOS" == "-pre" ]; then
	condor_history -file $condHis -af:,rh $col $extra > $condHis".csv"
elif [ "$preOS" == "-post" ]; then
	condor_history -file $condHis -af:,rh $col > $condHis".csv"
else
	echo "usage: $ condToCsv.sh -pre/-post $HTCONDOR_HISTORY_FILE"
fi	
