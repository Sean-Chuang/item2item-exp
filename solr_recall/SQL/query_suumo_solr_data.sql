# Query for test data
delete from z_seanchuang.i2i_offline_user_behavior where tag='suumo_web' and dt='2020-08-01';

insert into z_seanchuang.i2i_offline_user_behavior
with data as ( 
    select 
        ad_id,
        regexp_replace(replace(content_id, ' '), '^([0-9]+):([0-9a-zA-Z\-_]+):([0-9]+)$', '$2:$3') as content_id,
        CASE behavior_types
            WHEN 'ViewContent' THEN 'view'
            WHEN 'AddToCart' THEN 'add_cart'
            WHEN 'Purchase' THEN 'purchase'
            ELSE ''
        END as behavior_types,
        ts
    from (
        select 
            split(element_at(atp.data, 'content_ids')[1], ',') as content_ids, 
            element_at(atp.data, 'exid')[1] as ad_id,
            element_at(atp.data, 'e')[1] as behavior_types,
            ts
        from hive_ad.default.ad_pixel atp
        where atp.dt > date_format(date_add('day', -10, date('2020-08-01')),'%Y-%m-%d')
            and atp.dt <= '2020-08-01'
            and atp.log_type = 'pixel:event'
            and element_at(atp.data, 'id')[1] = '2e484315cc77a9cb5ddbf09a'
            and element_at(atp.data, 'content_ids')[1] is not null
    )
    cross join UNNEST(content_ids) as t(content_id)
),
g_data as (
    select 
        ad_id,
        array_agg((content_id || '@' || behavior_types || '@' || ts) order by ts) as events,
        'suumo_web' as tag,
        '2020-08-01' as dt 
    from data
    where ad_id not in ('',
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
