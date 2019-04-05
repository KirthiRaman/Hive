set hivevar:dgt=prod_dgraph_linked;
!echo "TOTAL RECORDS";
select count(*) from ${dgt};

!echo "TOTAL RECORDS WITH PID";
select count(*) from ${dgt}
where (pid is not null and pid != '');

!echo "TOTAL DISTINCT PID";
select count(distinct pid) from ${dgt}
where (pid is not null and pid != '');

!echo "TOTAL RECORDS WITH E1";
select count(*) from ${dgt}
where (e1seg is not null and e1seg != '');

!echo "MD5 Devices";
select count(single_md5) from ${dgt}                       
lateral view explode(split(md5,"\\|")) ${dgt} as single_md5
where length(udid)=32 and (md5 is not null and md5 != '') ;

!echo "SHA1 Devices";
select count(single_sha1) from ${dgt}                       
lateral view explode(split(sha1,"\\|")) ${dgt} as single_sha1
where length(udid)=40 and (sha1 is not null and sha1 != ''); 

!echo "Raw Devices";
select count(*) from ${dgt}                       
where length(udid) not in (32,40);

!echo "AD-ID";
select count(udid) from prod_did_map
where length(did)=36;

!echo "DID_OS";
select count(*) from ${dgt}
where (lower(did_os)='ios' or lower(did_os)='android');

!echo "LAT,LON";
select count(*) from ${dgt}
where (lat is not null and lat != '') and (lon is not null and lon != '');

!echo "IP";
select count(*) from ${dgt}
where (ip is not null and ip != '');

!echo "EMAIL";
select count(distinct single_email) from ${dgt}       
lateral view explode(split(email, "\\|")) ${dgt} as single_email
where (email is not null and email != '');

!echo "PHONE";
select count(distinct single_phone) from ${dgt}                          
lateral view explode(split(phone, "\\|")) ${dgt} as single_phone
where (phone is not null and phone != '');

!echo "DID_OS AND PID";
select count(*) from ${dgt}
where (lower(did_os)='ios' or lower(did_os)='android')
and (pid is not null and pid != '');

!echo "LAT,LON AND PID";
select count(*) from ${dgt}
where (lat is not null and lat != '') and (lon is not null and lon != '')
and (pid is not null and pid != '');

!echo "IP AND PID";
select count(*) from ${dgt}
where (ip is not null and ip != '')
and (pid is not null and pid != '');

!echo "EMAIL AND PID";
select count(distinct single_email) from ${dgt}       
lateral view explode(split(email, "\\|")) ${dgt} as single_email
where (email is not null and email != '')
and (pid is not null and pid != '');

!echo "PHONE AND PID";
select count(distinct single_phone) from ${dgt}                          
lateral view explode(split(phone, "\\|")) ${dgt} as single_phone
where (phone is not null and phone != '') 
and (pid is not null and pid != '');

!echo "DID_OS AND E1";
select count(*) from ${dgt}
where (lower(did_os)='ios' or lower(did_os)='android')
and (e1seg is not null and e1seg != '');

!echo "LAT,LON AND E1";
select count(*) from ${dgt}
where (lat is not null and lat != '') and (lon is not null and lon != '')
and (e1seg is not null and e1seg != '');

!echo "IP AND E1";
select count(*) from ${dgt}
where (ip is not null and ip != '')
and (e1seg is not null and e1seg != '');

!echo "EMAIL AND E1";
select count(distinct single_email) from ${dgt}       
lateral view explode(split(email, "\\|")) ${dgt} as single_email
where (email is not null and email != '')
and (e1seg is not null and e1seg != '');

!echo "PHONE AND E1";
select count(distinct single_phone) from ${dgt}                          
lateral view explode(split(phone, "\\|")) ${dgt} as single_phone
where (phone is not null and phone != '')
and (e1seg is not null and e1seg != '');
