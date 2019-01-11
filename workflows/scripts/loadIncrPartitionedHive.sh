set -ex
if [ $(($# % 2)) = 0 ] && [ ! $# -eq 0 ]; then
  cnt=1
  while [ $cnt -le $# ]
  do
    hive_incr_table=${!cnt}
    ((cnt++))
    hive_final_table=${!cnt}
    ((cnt++))
    beeline -u 'jdbc:hive2://10.85.9.138:10000/' -n hive --verbose=true -e "set hive.exec.dynamic.partition.mode=nonstrict;set hive.mv.files.thread=0;insert overwrite table ${hive_final_table} partition(partition_date) select *,gadate as partition_date from ${hive_incr_table};"
    beeline -u 'jdbc:hive2://10.85.9.138:10000/' -n hive --verbose=true -e "truncate table ${hive_incr_table} ;"
  done
else
  echo "odd arguments or missing arguments"
  exit 1 
fi
