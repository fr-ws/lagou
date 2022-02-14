-- 思路：与第二模块最后一题类似
-- 1、将数据按照user_id分区click_time排序
-- 2、使用lag()函数求出click_time前一行的值，nvl处理第一行空值为当前行time
-- 3、将每一行click_time与前一行click_time转为时间戳进行相减，然后除以60转换为分钟数
-- 4、使用case when 语句，大于30的则为1 小于30 的则为0，打上flag标识
-- 5、使用sum() 窗口函数，给所有数据打上分组标识gid
-- 6、根据user_id,groupType重新排序，得到结果

select user_id,click_time,
row_number() over (partition by user_id,gid order by click_time) rank
from
  (select user_id,click_time,sum(flag) over (partition by user_id order by click_time rows
    between unbounded preceding and current row) as gid 
      from(
      select user_id,click_time,
        case when (unix_timestamp(click_time) -unix_timestamp(last_1_dt ))/60 >=30 then 1 else 0 end as flag  
          from(
            select user_id,click_time,
              row_number() over (partition by user_id order by click_time) rank,
              nvl(lag(click_time) over (partition by user_id order by click_time),click_time) as last_1_dt 
                from homework.user_clicklog)a1
          )a2
     )a3;