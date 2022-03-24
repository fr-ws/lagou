3. 在核心交易分析中完成如下指标的计算

--dws计算每日订单情况
drop table if exists dws.dws_trade_orders_day;
create table dws.dws_trade_orders_day(
`orders_num_day` int,
`orders_money_day` decimal,
`dt` string ,
`year` string
) comment '每日订单统计表'
--partitioned by (`dt` string) 
;

insert overwrite table dws.dws_trade_orders_day
select  count(orderid) orders_num_day,sum(totalmoney) orders_money_day,dt,year 
from (select distinct orderid,totalmoney,dt,substr(dt,0,4) year
from  dwd.dwd_trade_orders
where status > 0 and dataflag = 1
) t1 
group by dt,year

--季度
select sum(orders_num_day) orders_num_quarter,
sum(orders_money_day) orders_money_week,
ceiling(substr(dt,6,2)/3) quarter 
from dws.dws_trade_orders_day 
where year = '2020'
group by ceiling(substr(dt,6,2)/3)



--月度
select sum(orders_num_day) orders_num_month,
sum(orders_money_day) orders_money_month,
month(dt) month 
from dws.dws_trade_orders_day 
where year = '2020'
group by 
month(dt)

--周度
select sum(orders_num_day) orders_num_week,
sum(orders_money_day) orders_money_week,
weekofyear(dt) month 
from dws.dws_trade_orders_day 
where year = '2020'
group by 
weekofyear(dt)


节假日,休息日,工作日
--创建日期维表
drop table if exists dim.dim_date 
create table dim.dim_date(
`dt` string,
`day_info` int comment '日期属性，-1：其他，0：工作日，1：节假日'
)
--造几条数
insert overwrite table dim.dim_date 
select '2020-06-28',1 
union
select '2020-06-29',1 
union
select '2020-06-30',1 
union
select '2020-07-01',0 
union
select '2020-07-02',0 
union
select '2020-07-03',0 
union
select '2020-07-04',-1 
union
select '2020-07-05',-1 




计算订单笔数，订单总额
select 
--节假日
sum(case when t2.day_info = 1 then orders_num_day else 0  end) as orders_num_holiday,
sum(case when t2.day_info = 1 then orders_money_day else 0 end)  as orders_money_holiday,
--休息日
sum(case when t2.day_info <> 0 then orders_num_day else 0 end) as orders_num_restday,
sum(case when t2.day_info <> 0 then orders_money_day else 0  end) as orders_money_restday,
--工作日
sum(case when t2.day_info = -1 then orders_num_day else 0  end) as orders_num_workday,
sum(case when t2.day_info = -1 then orders_money_day else 0  end) as orders_money_workday
from (select * from dws.dws_trade_orders_day where year = '2020')  t1
join dim.dim_date t2
on  t1.dt = t2.dt
