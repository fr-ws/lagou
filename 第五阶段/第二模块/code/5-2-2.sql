2. 在会员分析中计算沉默会员数和流失会员数
沉默会员的定义：只在安装当天启动过App，而且安装时间是在7天前
流失会员的定义：最近30天未登录的会员


--沉默会员
select count(device_id) silence_user_num from 
(
SELECT device_id,count(1) as start_cnt 
FROM `dws`.`dws_member_start_day` 
where dt < date_add('2020-08-01',-7)
group by device_id 
having  start_cnt = 1
) t1



set hive.strict.checks.cartesian.product=false
--流失会员
select count(distinct device_id) 
from  `dws`.`dws_member_start_day`
where 
device_id not in 
(select device_id 
 FROM `dws`.`dws_member_start_day` 
where dt > date_add('2020-07-30',-30)) 
