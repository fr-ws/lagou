-- ˼·����ڶ�ģ�����һ������
-- 1�������ݰ���user_id����click_time����
-- 2��ʹ��lag()�������click_timeǰһ�е�ֵ��nvl�����һ�п�ֵΪ��ǰ��time
-- 3����ÿһ��click_time��ǰһ��click_timeתΪʱ������������Ȼ�����60ת��Ϊ������
-- 4��ʹ��case when ��䣬����30����Ϊ1 С��30 ����Ϊ0������flag��ʶ
-- 5��ʹ��sum() ���ں��������������ݴ��Ϸ����ʶgid
-- 6������user_id,groupType�������򣬵õ����

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