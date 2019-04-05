-- more 05_create_indiv_results.sh 
-- Shell script
#!/bin/sh
#for i in 0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27
for i in 28 29 30 31
do
  echo "Loading Table $i in HIVE ..."
  hive -d num=$i -f create_indiv_results.hql
done

-- more create_indiv_results.hql 
drop table scanbuy_finalresult_${num};
create table scanbuy_finalresult_${num} as
select a.device_id,b.e1,b.agegroups,b.iseglist from scanbuy_data_${num} a LEFT OUTER JOIN scanbuy_finalresult b
ON lower(a.device_id)=lower(b.device_id)
where (e1 is not null and e1 !='') OR (agegroups is not null and agegroups !='' ) OR (iseglist is not null and iseglist != '');

-- ------------------------------------------------
--- WHERE THE individual hql is created as
#!/bin/sh
for i in 0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31
do
  echo "Loading Table $i in HIVE ..."
  hive -d num=$i -f load_indiv.hql
done

-- more load_indiv.hql 
drop table if exists scanbuy_data_${num};
create external table scanbuy_data_${num} 
(
device_id string,
ip string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/user/mobiletl/scanbuy/${num}';
