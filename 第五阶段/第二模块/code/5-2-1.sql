一、构造数据如下
100050,1,100225,WSxxx营超市,1,1,2022-03-20,2022-03-20 20:32:22
100052,2,100236,新鲜xxx旗舰店,1,1,2022-03-20,2022-03-20 17:12:59
100053,3,100011,华为xxx旗舰店,1,1,2022-03-20,2022-03-20 14:24:22
100054,4,100159,小米xxx旗舰店,1,1,2022-03-20,2022-03-20 11:35:28
100055,5,100211,苹果xxx旗舰店,1,1,2022-03-20,2022-03-20 13:22:12

100057,7,100311,三只xxx鼠零食,1,1,2022-03-20,2022-03-21 17:12:59
100058,8,100329,良子xxx铺美食,1,1,2022-03-21,2022-03-21 14:24:22
100054,4,100159,小米xxx旗舰店,2,1,2022-03-21,2022-03-21 11:35:28
100055,5,100211,苹果xxx旗舰店,2,1,2022-03-21,2022-03-21 13:22:12

100059,9,100225,乐居xxx日用品,1,1,2022-03-22,2022-03-22 18:27:45
100060,10,100211,同仁xxx大健康,1,1,2022-03-22,2022-03-22 11:35:28
100052,2,100236,新鲜xxx旗舰店,1,2,2022-03-22,2022-03-22 17:12:59

二、ODS层商家维表数据加载
1.修改DATAX脚本增量加载数据
{
	"job": {
		"setting": {
			"speed": {
				"channel": 1
			},
			"errorLimit": {
				"record": 0
			}
		},
		"content": [
			{
				"reader": {
					"name": "mysqlreader",
					"parameter": {
						"username": "hive",
						"password": "12345678",
				"connection": [{
                 "querySql": [
"shopid,userid,areaid,shopname,shoplevel,status,createtime,modifytime 
from lagou_shops where date_format(modifiedTime, '%Y-%m-%d')='$do_date' "
],
"jdbcUrl": [
"jdbc:mysql://hadoop1:3306/ebiz"
]
}]

						"connection": [
							{
								"table": [
									"lagou_shops"
								],
								"jdbcUrl": [
									"jdbc:mysql://linux123:3306/ebiz"
								]
							}
						]
					}
				},
				"writer": {
					"name": "hdfswriter",
					"parameter": {
						"defaultFS": "hdfs://linux121:9000",
						"fileType": "text",
						"path": "/user/data/trade.db/shops/dt=$do_date",
						"fileName": "shops_$do_date",
						"column": [
							{
								"name": "shopId",
								"type": "INT"
							},
							{
								"name": "userId",
								"type": "INT"
							},
							{
								"name": "areaId",
								"type": "INT"
							},
							{
								"name": "shopName",
								"type": "STRING"
							},
							{
								"name": "shopLevel",
								"type": "TINYINT"
							},
							{
								"name": "status",
								"type": "TINYINT"
							},
							{
								"name": "createTime",
								"type": "STRING"
							},
							{
								"name": "modifyTime",
								"type": "STRING"
							}
						],
						"writeMode": "append",
						"fieldDelimiter": ","
					}
				}
			}
		]
	}
}


2.ods加载每日增量数据
hive -e "alter table ods.ods_trade_orders add partition(dt='$do_date')"


三、创建和加载dim层商家拉链表数据
1.创建商家维表
create external table dim.dim_trade_shops(
`shopid` int,
`userid` int,
`areaid` int,
`shopname` string,
`shoplevel` tinyint,
`status` tinyint,
`createtime` string,
`modifytime` string,
`startdate` string,
`enddate` string
) comment '商家信息拉链表'

2.拉链表初始化，加载第一天数据
insert overwrite  table dim.dim_trade_shops
select 
shopid,userid,areaid,shopname,shoplevel,status,createtime,modifytime,
case when  modifytime is not null then substr(modifytime,0,10)  
else substr(createtime,0,10) end AS startdate , '9999-12-31' AS enddate 
from ods.ods_trade_shops 
where dt = '2022-03-20'

3.更新拉链表数据
insert overwrite table dim.dim_trade_shops
--新增数据，分区已经为每日新增数据，直接加载ods层分区全量数据，设置enddate为'9999-12-31'
select 
shopid,userid,areaid,shopname,shoplevel,status,createtime,modifytime,
case when  modifytime is not null then 
substr(modifytime,0,10)  else substr(createtime,0,10) end AS startdate , 
'9999-12-31' AS enddate from ods.ods_trade_shops
 where dt = '2022-03-22'
union all
--历史数据，原表当作左表保留全部数据，新增数据中能关联上shopid的代表有变化的数据，设置enddate为执行日期-1
select 
t1.shopid,t1.userid,t1.areaid,t1.shopname,t1.shoplevel,t1.status,t1.createtime,t1.modifytime,t1.startdate,
case when t1.enddate='9999-12-31' and t2.shopid is not null
then date_add('2022-03-23', -1)
else t1.enddate end as enddate
from dim.dim_trade_shops t1 
left join  
(select * from ods.ods_trade_shops where dt = '2022-03-22') t2
on t1.shopid = t2.shopid

四、拉链表回滚
end_date < rollback_date，即结束日期 < 回滚日期，表示该行数据在rollback_date 之前产生，这些数据需要原样保留；
start_date <= rollback_date <= end_date，即开始日期 <= 回滚日期 <= 结束日期，这些数据是回滚日期之后产生的，但是需要修改，将end_date 改为 9999-12-31；
其他数据即为之后产生的数据
--回滚到21日数据
drop table dim.tmp;
--创建临时表存放回归数据
create table dim.tmp as
select shopid,userid,areaid,shopname,shoplevel,status,createtime,modifytime,startdate,enddate 
from dim.dim_trade_shops
where enddate<='2022-03-21' 
union
select shopid,userid,areaid,shopname,shoplevel,status,createtime,modifytime,startdate, enddate 
from dim.dim_trade_shops
where startdate<='2022-03-21' and enddate>='2022-03-21'

insert overwrite table dim.dim_trade_shops
select *  from dim.tmp_shops;