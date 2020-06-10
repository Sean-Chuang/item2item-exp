delete from z_seanchuang.i2i_offline_train_raw where dt='${dt}';
insert into z_seanchuang.i2i_offline_train_raw
with data as ( 
  select 
      ad_id,
      regexp_replace(content_id, '^([0-9]+):([0-9a-zA-Z\-_]+):([0-9]+)$', '$2:$3') as content_id,
      replace(behavior_types, 'adjust:callback:','') as behavior_types,
      ts,
      '${dt}' as dt
  from 
  (
     select 
         split(j(j(ROW, '$._d.PartnerParameters'), '$.content_id'), ',') as content_ids, 
         j(ROW, '$._u') as ad_id,
         j(ROW, '$._m') as behavior_types,
         cast(j(ROW, '$.timestamp') as bigint) as ts
     from hive_ad.default.ad_tracker_plain atp
     where atp.dt > date_format(date_add('day', -7, date('${dt}')),'%Y-%m-%d')
       and atp.dt <= '${dt}'
       and j(ROW, '$._d.StoreId') in ('jp.co.rakuten.android', '419267350')
  )
  cross join UNNEST(content_ids) as t(content_id)
)
select * from data where ad_id not in ('',
                       '00000000-0000-0000-0000-000000000000',
                       'b809a3a1-c846-41db-b0d4-8910a3fb21c0',
                       'DEFACE00-0000-0000-0000-000000000000')
;


delete from z_seanchuang.i2i_offline_test_raw where dt='${dt}';
insert into z_seanchuang.i2i_offline_test_raw
with data as (
  select 
      ad_id,
      regexp_replace(content_id, '^([0-9]+):([0-9a-zA-Z\-_]+):([0-9]+)$', '$2:$3') as content_id,
      replace(behavior_types, 'adjust:callback:','') as behavior_types,
      ts,
      '${dt}' as dt
  from 
  (
     select 
         split(j(j(ROW, '$._d.PartnerParameters'), '$.content_id'), ',') as content_ids, 
         j(ROW, '$._u') as ad_id,
         j(ROW, '$._m') as behavior_types,
         cast(j(ROW, '$.timestamp') as bigint) as ts
     from hive_ad.default.ad_tracker_plain atp
     where atp.dt > '${dt}'
       and atp.dt <= date_format(date_add('day', 1, date('${dt}')),'%Y-%m-%d')
       and j(ROW, '$._d.StoreId') in ('jp.co.rakuten.android', '419267350')
  )
  cross join UNNEST(content_ids) as t(content_id)
)
select * from data where ad_id not in ('',
                       '00000000-0000-0000-0000-000000000000',
                       'b809a3a1-c846-41db-b0d4-8910a3fb21c0',
                       'DEFACE00-0000-0000-0000-000000000000')
;