delete from z_seanchuang.i2i_offline_w2v_train_data where dt='2020-06-30';

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

#==============================================#


set session distributed_join=true;

-- next --
delete from z_seanchuang.i2i_offline_w2v_train_data where dt='2020-06-30-all';
insert into z_seanchuang.i2i_offline_w2v_train_data
with data as ( 
    select 
        ad_id,
        replace(regexp_replace(content_id, '^([0-9]+):([0-9a-zA-Z\-_]+):([0-9]+)$', '$2:$3'), ' ') as content_id,
        ts
    from (
        select 
            split(j(j(ROW, '$._d.PartnerParameters'), '$.content_id'), ',') as content_ids, 
            j(ROW, '$._u') as ad_id,
            cast(j(ROW, '$.timestamp') as bigint) as ts
        from hive_ad.default.ad_tracker_plain atp
        where atp.dt > date_format(date_add('day', -10, date('2020-06-30')),'%Y-%m-%d')
            and atp.dt <= '2020-06-30'
            and j(ROW, '$._d.StoreId') in ('jp.co.rakuten.android', '419267350')
            and j(ROW, '$._m') in ('adjust:callback:ViewContent',
                                'adjust:callback:AddToCart',
                                'adjust:callback:revenue')
    )
    cross join UNNEST(content_ids) as t(content_id)
),
user_events as (
    select 
        ad_id, 
        array_agg(content_id order by ts) as events
    from data
    where ad_id not in ('',
                       '00000000-0000-0000-0000-000000000000',
                       'b809a3a1-c846-41db-b0d4-8910a3fb21c0',
                       'DEFACE00-0000-0000-0000-000000000000')
    group by 1
)
select 
    array_join(events, ' '),
    '2020-06-30-all' as dt
from user_events
;
