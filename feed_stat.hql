set hivevar:dgt=prod_dgraph_linked;
!echo 'DGRAPH PVDR SPREAD';
select count(partner) as counts,partner from ${dgt}
LATERAL VIEW explode(split(pvdr,"\\|")) t as partner       
GROUP BY partner ORDER BY counts desc;

!echo 'DGRAPH PVDR SPREAD WITH E1';
select count(partner) as counts,partner from ${dgt}
LATERAL VIEW explode(split(pvdr,"\\|")) t as partner       
where (e1seg is not null and e1seg != '') GROUP BY partner ORDER BY counts desc;
