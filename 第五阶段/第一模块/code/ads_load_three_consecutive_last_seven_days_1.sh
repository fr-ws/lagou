#!/bin/bash
source /etc/profile
if [ -n "$1" ]
then
    do_date=$1
else
    do_date=`date -d "-1 day" +%F`
fi

sql="
WITH tmp as(
            SELECT device_id,dt,
			#分组排序后通过日期减去排名得到分组
                date_sub(dt,row_number() over(partition by device_id order by dt)) groupId
              FROM dws_member_start_day
			  #限定时间为最近七天
            WHERE dt between date_sub(current_date,7) and current_date),
     tmp2 as(SELECT device_id,count(1) cnt 
              FROM tmp
            GROUP BY device_id,groupId
            having cnt >= 3)
insert into table ads.ads_member_three_consecutive_last_seven_days
#统计值cnt大于等于3的记录数，即为最近7天中连续3天活跃会员数
SELECT count(distinct device_id) cnt 
  FROM tmp2;
" 

hive -e "$sql"