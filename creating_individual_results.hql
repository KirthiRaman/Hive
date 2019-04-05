#!/bin/sh
for i in 0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31
do
  echo "Generating CSV File $i  ..."
  hive -e "select * from scanbuy_finalresult_$i" | sed 's/[\t]/,/g' | sed 's/NULL//g' | grep -v WARN | sed 's/scanbuy_finalresult_$i.//g'  > /mnt/sec_hdfs/user/mobiletl/scanbuy
_results/Scanbuy_AdAdvisor_results_20190401_$i.csv
  echo "Done Generating CSV File $i"
done

# sed 's/scanbuy_finalresult_$i.//g' is done to remove the prefix in the table-column header
