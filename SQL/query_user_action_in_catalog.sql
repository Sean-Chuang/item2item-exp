with catlog_items as (
    select 
        replace(regexp_replace(id,'^([0-9]+):([0-9a-zA-Z\-_]+):([0-9]+)$','$2:$3'), ' ') as content_id, 
        title, 
        google_product_category, 
        try_cast(regexp_replace(price, 'JPY', '') as double) as price 
    from hive.maeda.rakuten_rpp_datafeed 
),
data as ( 
  select 
      ad_id,
      replace(regexp_replace(content_id, '^([0-9]+):([0-9a-zA-Z\-_]+):([0-9]+)$', '$2:$3'), ' ') as content_id,
      replace(behavior_types, 'adjust:callback:','') as behavior_types,
      ts
  from 
  (
     select 
         split(j(j(ROW, '$._d.PartnerParameters'), '$.content_id'), ',') as content_ids, 
         j(ROW, '$._u') as ad_id,
         j(ROW, '$._m') as behavior_types,
         cast(j(ROW, '$.timestamp') as bigint) as ts
     from hive_ad.default.ad_tracker_plain atp
     where atp.dt > date_format(date_add('day', -14, date('2020-06-30')),'%Y-%m-%d')
       and atp.dt <= '2020-06-30'
       and (j(ROW, '$._d.StoreId') in ('jp.co.rakuten.android', '419267350')
        or j(ROW, '$._d.url[0]')='https://grp15.ias.rakuten.co.jp/gw.js?v=2')
  )
  cross join UNNEST(content_ids) as t(content_id)
  where ad_id not in ('',
                     '00000000-0000-0000-0000-000000000000',
                     'b809a3a1-c846-41db-b0d4-8910a3fb21c0',
                     'DEFACE00-0000-0000-0000-000000000000')
    and behavior_types = 'revenue'
)
select 
    count(distinct ad_id) as user_count, 
    count(distinct content_id) as item_count,
    count(*) as total_behaviors
from data
inner join catlog_items using(content_id)
;


# ============================================= #
select 
    count(*)
from hive_ad.default.ad_tracker_plain atp
where atp.dt > date_format(date_add('day', -1, date('2020-06-30')),'%Y-%m-%d')
  and atp.dt <= '2020-06-30'
  and (j(ROW, '$._d.StoreId') in ('jp.co.rakuten.android', '419267350')
    or j(ROW, '$._d.url[0]')='https://grp15.ias.rakuten.co.jp/gw.js?v=2')





select 
    element_at(data, '')
from 
where dt = '2020-06-30'
