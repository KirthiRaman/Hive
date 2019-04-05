set hivevar:inputTable=experian_external_${drop_dt};
set hivevar:outputTable=experian_did_${drop_dt}_clid_nstrpid;
set hivevar:deviceID_col=device_id;
set hivevar:dgraphTable=prod_dgraph_linked;

add jar /opt/mobiletl/prod_workflow_dir/lib/hiveudf_ip.jar;
add jar /opt/mobiletl/prod_workflow_dir/lib/hiveudf.jar;
create temporary function udf_ip_to_index as 'biz.neustar.nis.hiveudf.IPtoIndex';

add jar /opt/mobiletl/prod_workflow_dir/lib/hiveudf_hash.jar;
create temporary function hash_string as 'HashString';

set hive.auto.convert.join=false;

drop table ${outputTable}_inter_1;
create table ${outputTable}_inter_1 as
SELECT a.device_id,a.client_id,b.pid
FROM
(select device_id, client_id,hash
    FROM ${inputTable}
    LATERAL VIEW explode(split(concat_ws('\|',
    lower(trim(${deviceID_col})),
    upper(trim(${deviceID_col})),
    hash_string(lower(trim(${deviceID_col})),"MD5"),
    hash_string(upper(trim(${deviceID_col})),"MD5"),
    hash_string(lower(trim(${deviceID_col})),"SHA-1"),
    hash_string(upper(trim(${deviceID_col})),"SHA-1")
    ), "\\|")) ${inputTable} as hash
    where device_id is not null and device_id != ''
    ) A
LEFT OUTER JOIN
    (select hash, pid
    FROM ${dgraphTable}
    LATERAL VIEW explode(split(concat_ws('\|',
    md5,
    sha1
    ), "\\|")) ${dgraphTable} as hash
    ) B
ON A.hash = B.hash
;

drop table ${drop_dt}_exper_clid_luid;
create table ${drop_dt}_exper_clid_luid as 
select distinct a.client_id,b.luid from experian_did_${drop_dt}_clid_nstrpid_inter_1 a LEFT OUTER JOIN experian_luid_pid_distinct b 
ON cast(a.pid as bigint)=cast(b.pid as bigint);

drop table ${drop_dt}_exper_clid_epid;
create table ${drop_dt}_exper_clid_epid as 
select distinct a.client_id,b.epid 
from experian_did_${drop_dt}_clid_nstrpid_inter_1 a LEFT OUTER JOIN experian_epid_pid_distinct b 
ON cast(a.pid as bigint)=cast(b.pid as bigint);

drop table ${drop_dt}_exper_partial;
create table ${drop_dt}_exper_partial as
select a.client_id,a.luid,b.epid 
from ${drop_dt}_exper_clid_luid a LEFT OUTER JOIN ${drop_dt}_exper_clid_epid b 
ON a.client_id=b.client_id where a.client_id is not null and a.client_id != '';

drop table ${drop_dt}_exper_partial_2;
create table ${drop_dt}_exper_partial_2 as
select client_id,epid as person_id,max(luid) as luid
from ${drop_dt}_exper_partial 
where (luid is not null and luid != '') OR (epid is not null and epid != '') 
GROUP BY client_id,epid;

set hivevar:sort_order=(partition by client_id order by luid desc);

drop table ${drop_dt}_experian_full;
create table ${drop_dt}_experian_full as

select client_id, luid,person_id
from
(
    select client_id, luid,person_id,
           row_number() over ${sort_order} as row_number
    from ${drop_dt}_exper_partial_2
    GROUP BY client_id,luid,person_id
) a
where a.row_number = 1
;

drop table ${drop_dt}_exper_partial;
drop table ${drop_dt}_exper_partial_2;

set hive.auto.convert.join=true;
