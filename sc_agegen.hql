drop table scanbuy_agegen;
create table scanbuy_agegen as 
select a.device_id,a.e1,a.pid,b.agerange,b.gender 
from scanbuy_results a LEFT OUTER JOIN deviceid_agegen_20180929 b
ON lower(a.device_id)=lower(b.device_id);

drop table scanbuy_agegen_distinct; 
create table scanbuy_agegen_distinct as
select distinct device_id,e1,pid,agerange,gender from scanbuy_agegen;

ADD FILE /opt/mobiletl/cdmti/scanbuy/get_agegender_segments.py;

drop table if exists scanbuy_agegen_res;
create table scanbuy_agegen_res as
select device_id,e1,pid,agegroups
FROM
(
    select transform(device_id,e1,pid,agerange,gender)
    using "python get_agegender_segments.py"
    as device_id,e1,pid,agegroups
    from ( SELECT device_id,e1,pid,agerange,gender from scanbuy_agegen_distinct ) A
) B
;
