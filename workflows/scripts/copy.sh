#$1 ssh key
#$2 <user>@<ip>
#$3 <src file path>
#$4 <dest hdfs path>
echo 'scp -i '$1' '$2':'$3' /tmp'>/tmp/exec.log
whoami>>/tmp/exec.log
chmod 700 $1
filename=`basename $3`
rm -rf /tmp/$filename
scp -o StrictHostKeyChecking=no -i $1 -r $2:$3 /tmp>>/tmp/exec.log 2>&1
filename=`basename $3`
hdfs dfs -rm -r $4
hdfs dfs -copyFromLocal /tmp/$filename $4>>/tmp/exec.log 2>&1
