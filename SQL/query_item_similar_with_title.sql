with catlog_items as (
    select 
        content_id,  
        count(*) AS click 
    from hive_ad.default.action_click ac 
    inner join hive.maeda.rakuten_catalog c 
        on ac.mc_item_id=c.content_id and c.included_in_rpp=1
    where ac.dt between  '2020-05-24' and '2020-05-30'
    group by 1
    order by 2 desc 
    limit 4000
),


