#!/bin/bash

files=(Controller.java Dstore.java Controller.class Dstore.class)
cport=12345
r=3
dport1=12346
dport2=12347
dport3=12348
timeout=100
rebalance_period=10000

script_error() {
	echo "Error, script terminated"
	exit 1
}

check_status() {
	if [ ! $? -eq 0 ]
	then
		script_error
	fi
}

silently_kill() {
	{ kill -9 $1 && wait $1; } 2>/dev/null
}

echo
echo "This script will validate your COMP2207 2020/21 coursework submission by checking whether:"
echo -e "\t - the zip file can be unzipped (this requires unzip command to be installed)"
echo -e "\t - all required files are there"
echo -e "\t - all the included Java source files can be compiled"
echo -e "\t - Controller and Dstore processes can be started"
echo -e "\t - required log files are created"
echo
echo "Please make sure that your zip file is successfully validated with this script before submitting it."
echo
echo "This script will unzip files in ./tmp/ directory, and compile and execute your software in ./run/ directory. Both folders will be created in the current working directory. If those directories already exist, they will first be deleted, together with all their content."
echo
echo "This script will use the logger classes included in the ./loggers/ directory, and expects to find this folder in the current working directory."
echo
read -p "Are you sure you want to continue (y/n)" -n 1 -r
echo
echo

if [[ $REPLY =~ ^[Yy]$ ]]
then
	echo "Validation started"
else
	echo "Script not executed"
	exit 0
fi

if [ -z $1 ]
  then
    echo "submission zip file expected"
	script_error
fi

if [ -d tmp/ ]; then
	echo -n "tmp directory already exists, deleting it..."
	rm -fdr tmp/
	check_status
	echo "ok"
fi

echo -n "Creating tmp directory..."
mkdir tmp
check_status
echo "ok"

echo -n "Unzipping submission file..."
unzip $1 -d tmp/ > /dev/null
check_status
echo "ok"

echo -n "Checking all required files exist..."
cd tmp/
for f in ${files[@]}; do
	if [ ! -f "$f" ]; then
		echo "$f does not exist"
		exit 1
	fi
done
echo "ok"

cd ..

if [ -d run/ ]; then
	echo -n "run directory already exists, deleting it..."
	rm -fdr run/
	check_status
	echo "ok"
fi

echo -n "Creating run directory..."
mkdir run
check_status
echo "ok"

echo -n "Copying loggers Java sources..."
cp loggers/*.java tmp/
check_status
echo "ok"

cd run/

echo -n "Compiling Java sources..."
javac ../tmp/*.java -d .
check_status
echo "ok"

echo "Starting Controller on port ${cport} with replication factor ${r}, timout ${timeout}ms and rebalance period ${rebalance_period}ms ..."
java Controller $cport $r $timeout $rebalance_period &
controllerId=$!
check_status
sleep 1s
echo "Ok, Controller started"

echo "Starting 3 Dstore processes on ports ${dport1}, ${dport2} and ${dport3} with timeout ${timeout}ms..."
mkdir file_folder_$dport1
java Dstore $dport1 $cport $timeout file_folder_$dport1 &
d1Id=$!
check_status
mkdir file_folder_$dport2
java Dstore $dport2 $cport $timeout file_folder_$dport2 &
d2Id=$!
check_status
mkdir file_folder_$dport3
java Dstore $dport3 $cport $timeout file_folder_$dport3 &
d3Id=$!
check_status
echo "Ok, Dstores started"

wait_time=$((rebalance_period * 3 / 2000))
echo "Wait ${wait_time} seconds (i.e., 1.5 * rebalance period)..."
sleep $wait_time
check_status

echo "Kill Controller and Dstore processes still running..."
silently_kill $d1Id 
silently_kill $d2Id
silently_kill $d3Id
silently_kill $controllerId
echo "Ok, Controller and Dstore processes killed"

echo -n "Checking all required log files exist..."
if [ ! -n "$(find . -name 'controller_*.log' | head -1)" ]; then
	echo "log file of Controller not found"
	script_error
fi

d1log="dstore_${dport1}_*.log"
if [ ! -n "$(find . -name ${d1log} | head -1)" ]; then
	echo "log file of Dstore ${dport1} not found"
	script_error
fi

d2log="dstore_${dport2}_*.log"
if [ ! -n "$(find . -name ${d2log} | head -1)" ]; then
	echo "log file of Dstore ${dport2} not found"
	script_error
fi

d3log="dstore_${dport3}_*.log"
if [ ! -n "$(find . -name ${d3log} | head -1)" ]; then
	echo "log file of participant ${dport3} not found"
	script_error
fi
echo "ok"

echo "Validation successfully completed."
