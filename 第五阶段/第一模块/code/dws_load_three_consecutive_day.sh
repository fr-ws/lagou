#加载dws层数据
#! /bin/bash
source /etc/profile
if [ -n "$1" ] ;then
do_date=$1
else
do_date=`date -d "-1 day" +%F`
fi
sql="
insert overwrite table dws.dws_member_three_consecutive_day
partition (dt='$do_date')

select t1.* from (
(select *
from dws.dws_member_start_day where dt = '$do_date')  t1
join (select device_id from dws.dws_member_start_day where dt = date_sub('$do_date',1)) t2
on t1.device_id = t2.device_id
join (select device_id from dws.dws_member_start_day where dt = date_sub('$do_date',2)) t3
on t2.device_id = t3.device_id
) 
"
hive -e "$sql"