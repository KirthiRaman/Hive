#!/bin/sh

echo "Upload Started"
for i in 0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31
do
  aws s3 cp s3://neustar-scanbuy/Scanbuy-20190404-$i-adid.gz /mnt/sec_hdfs/user/mobiletl/scanbuy/$i
done
