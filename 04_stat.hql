!echo "experian_${drop_dt} pid";
select count(distinct device_id) from experian_did_${drop_dt}_clid_nstrpid_inter_1 where (pid is not null and pid != '');
!echo "experian_${drop_dt} luid";
select count(distinct client_id) from ${drop_dt}_experian_full where (luid is not null and luid != '');
!echo "experian_${drop_dt} person_id";
select count(distinct client_id) from ${drop_dt}_experian_full where (person_id is not null and person_id != '');
