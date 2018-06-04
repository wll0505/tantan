#!/bin/sh
Y_M_D_1D=`date -d "$1 1 day ago" +%Y-%m-%d` 

hive<<EOF
set hive.execution.engine=MR;
create table if not exists data_analytics.boost_pay_day_d
(id int comment '',
stat_date string comment '日期',
group_name string comment '组名',
active_users int comment '活跃用户数',
boost_revenue decimal(10,2) comment '单次超级曝光收入',
boostBadge_revenue decimal(10,2) comment '包月曝光收入',
boostVip_revenue decimal(10,2) comment '超级曝光会员收入',
boost_total_revenue decimal(10,2) comment '曝光总收入',
vip_revenue decimal(10,2) comment 'vip收入',
boost_user int comment '单次超级曝光购买用户数',
boostBadge_user int comment '包月曝光购买用户数',
boostVip_user int comment '超级曝光会员购买用户数',
boost_total_user int comment '曝光购买用户数',
vip_user int comment 'vip购买用户数',
load_time string comment '加载时间')
partitioned by (dt string);

insert overwrite table data_analytics.boost_pay_day_d partition(dt='${Y_M_D_1D}')
select 
        substr(t2.name,-1) as id,
        '${Y_M_D_1D}' as stat_date,
        t2.name as group_name,
        count(distinct t3.user_id) as active_users,
        sum(case when product_type='boost' then amount else 0 end) as boost_revenue,
        sum(case when product_type='boostBadge' then amount else 0 end) as boostBadge_revenue,
        sum(case when product_type='boostVip' then amount else 0 end) as boostVip_revenue,
        sum(case when product_type in ('boost','boostVip','boostBadge') then amount else 0 end) as boost_total_revenue,
        sum(case when product_type='vip' then amount else 0 end) as vip_revenue,
        count(distinct case when product_type='boost' then t4.user_id end) as boost_user,
        count(distinct case when product_type='boostBadge' then t4.user_id end) as boostBadge_user,
        count(distinct case when product_type='boostVip' then t4.user_id end) as boostVip_user,
        count(distinct case when product_type in ('boost','boostVip','boostBadge') then t4.user_id end) as boost_total_user,
        count(distinct case when product_type='vip' then t4.user_id end) as vip_user,
        current_timestamp() as load_time
from
(select * from dwd.dwd_putong_abtest_public_ab_users_a_d where dt='${Y_M_D_1D}' and status = 'default') t1
join
(select * from dwd.dwd_putong_abtest_public_ab_groups_a_d where dt='${Y_M_D_1D}'
and name in ('boost:201805111',
'longTermBoost:201805112',
'longTermBoost:201805113',
'mixedBoost:201805114',
'mixedBoost:201805115',
'boostMembershipIndependent:201805116',
'boostMembershipIndependent:201805117',
'boostMembership:201805118',
'boostMembership:201805119')) t2
on t1.ab_group_id=t2.id
left join 
(select * from dwd.dwd_daily_user_activities where dt='${Y_M_D_1D}') t3
on t1.user_id=t3.user_id
left join
(select * from dwd.dwd_agt_tantan_user_order_info_i_d where dt='${Y_M_D_1D}' and product_type in ('boost','boostVip','boostBadge','vip')) t4
on t1.user_id=t4.user_id
group by t2.name,substr(t2.name,-1)
order by id
limit 100;
EOF