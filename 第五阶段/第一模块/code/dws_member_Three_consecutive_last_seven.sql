--ads建表语句，每日的最近七天连续三天活跃用户信息
drop table if exists ads.dws_member_three_consecutive_last_seven_days;
create table ads.dws_member_three_consecutive_last_seven_days(
`cnt` string
)  COMMENT '连续三天活跃会员'
partitioned by (`dt` string)
stored as parquet;

