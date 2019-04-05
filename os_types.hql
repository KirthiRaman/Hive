-- Total 1,617,310,011 
set hivevar:dgtable=prod_dgraph_linked;
!echo "Apple current"; 
select count(*) from ${dgtable} where INSTR(lower(did_os),'iphone')>0 OR INSTR(lower(did_os),'apple')>0 OR INSTR(lower(did_os),'ios')>0 OR INSTR(lower(did_os),'mac os')>0 OR IN
STR(lower(did_os),'ios')>0 OR INSTR(Lower(did_os),'osx')>0 OR INSTR(lower(did_os),'os x')>0 OR INSTR(lower(did_os),'macosx')>0;
--select count(*) from ${dgtable} where did_os='iphone os' OR did_os='apple' OR did_os='macosx' OR did_os='os x' OR did_os='ios' OR did_os='iphone%20os';
!echo "Android current";  
select count(*) from ${dgtable} where INSTR(lower(did_os),'android')>0;
--select count(*) from ${dgtable} where did_os='android' OR did_os='browser-android' OR did_os='hardware_md5androidid' OR did_os='hardware_android_ad_id' OR did_os='hardware_sh
a1androidphoneid';
!echo "To Find Others and Unknown current" ; 
select count(*) from ${dgtable} where did_os != '' AND lower(did_os) != 'unknown' AND did_os is not null;
-- If the above result is 647,316,653 and total is 938,120,207 and apple and android together is 553,236,236
-- Then 647,316,653 - 553,236,236 = 94,080,417 (Other Devices)
-- And 938,120,207 - 647,316,653 = 290,803,472 (Unknown)
-- Other 399252356
-- Unknown 276740142
