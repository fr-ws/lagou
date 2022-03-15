#加载ads层数据
#! /bin/bash
source /etc/profile
if [ -n "$1" ] ;then
do_date=$1
else
do_date=`date -d "-1 day" +%F`
fi
sql="
insert overwrite table ads.ads_member_three_consecutive_last_seven_days
partition (dt='$do_date')
select count(distinct device_id) cnt 
from 
dws.dws_member_three_consecutive_day 
where dt between date_sub('$do_date',5) and '$do_date'
"
hive -e "$sql"
"
) 
hive -e "$sql"