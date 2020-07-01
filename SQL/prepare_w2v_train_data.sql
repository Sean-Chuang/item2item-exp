delete from z_seanchuang.i2i_offline_w2v_train_data where dt='2020-05-30';

insert into z_seanchuang.i2i_offline_w2v_train_data
with user_events as (
    select 
        ad_id, 
        array_agg(concat(behavior_types, ':', content_id) order by ts) as events
    from z_seanchuang.i2i_offline_train_raw
    where dt = '2020-05-30-2week'
    group by 1
)
select 
    array_join(events, ' '),
    '2020-05-30-2week' as dt
from user_events