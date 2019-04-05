drop table experian_external_${drop_dt};
create external table experian_external_${drop_dt}
(
  device_id string,
  client_id string,
  hash_type string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/nis/prod/data/mobiletl/testfeeds/experian/${drop_dt}'
;
-- Fields could also be terminated by , OR | (instead of \t )
