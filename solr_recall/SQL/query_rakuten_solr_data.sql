# Query for test data
delete from z_seanchuang.i2i_offline_user_behavior where tag='rakuten_app' and dt='2020-08-01';

insert into z_seanchuang.i2i_offline_user_behavior
with data as ( 
    select 
        ad_id,
        regexp_replace(replace(content_id, ' '), '^([0-9]+):([0-9a-zA-Z\-_]+):([0-9]+)$', '$2:$3') as content_id,
        CASE behavior_types
            WHEN 'ViewContent' THEN 'view'
            WHEN 'AddToCart' THEN 'add_cart'
            WHEN 'revenue' THEN 'purchase'
            ELSE ''
        END as behavior_types,
        ts
    from (
        select 
            split(j(element_at(atp.data, 'partnerparameters')[1], '$.content_id'), ',') as content_ids, 
            element_at(atp.data, 'adid')[1] as ad_id,
            element_at(atp.data, 'event')[1] as behavior_types,
            element_at(atp.data, 'createdat')[1] as ts
        from hive_ad.default.ad_pixel atp
        -- where ((atp.dt = date_format(date_add('day', -1, date('2020-08-01')), '%Y-%m-%d') and atp.hh > '19') or (atp.dt = '2020-08-01' and atp.hh <= '19' ))
        where atp.dt > date_format(date_add('day', -5, date('2020-08-01')),'%Y-%m-%d')
            and atp.dt <= '2020-08-01' 
            and element_at(atp.data, 'storeid')[1] in ('jp.co.rakuten.android', '419267350')
            and j(element_at(atp.data, 'partnerparameters')[1], '$.content_id') is not null
            and atp.log_type in (
                                'adjust:callback:ViewContent',
                                'adjust:callback:AddToCart',
                                'adjust:callback:revenue')
    )
    cross join UNNEST(content_ids) as t(content_id)
),
g_data as (
    select 
        ad_id,
        array_agg((content_id || '@' || behavior_types || '@' || ts) order by ts) as events,
        'rakuten_app' as tag,
        '2020-08-01' as dt 
    from data
    where ad_id is not null 
        and ad_id not in ('',
                       '00000000-0000-0000-0000-000000000000',
                       'b809a3a1-c846-41db-b0d4-8910a3fb21c0',
                       'DEFACE00-0000-0000-0000-000000000000')
        and behavior_types != ''
    group by ad_id
)
select 
    *
from g_data
;
