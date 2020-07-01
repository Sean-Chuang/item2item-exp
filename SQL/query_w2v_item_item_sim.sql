WITH item_emb as (
    SELECT 
        element_at(split(item, ':',2),1) as event,  
        element_at(split(item, ':',2),2) as content_id, 
        vec 
    FROM hive.maeda.item_embeddings_v2
    WHERE dt='2020-05-20' 
        AND feature_id='rakuten_i2i'   
),
item_emb1 as (
    SELECT 
        content_id,
        MAX_BY(vec, CASE WHEN event='purchase' THEN 2 WHEN event='add_to_cart' THEN 1 ELSE 0 END ) as vec
    FROM item_emb
    GROUP BY 1
),
item_emb2 as (
    SELECT 
        content_id, 
        vec
    FROM item_emb
    where event='view_content'
), 
i2i as (
    SELECT * 
    FROM (
        SELECT 
            *,
            DENSE_RANK() OVER(PARTITION BY target_content_id ORDER BY similarity_score DESC) as rnk
        FROM (
            SELECT 
                i2.content_id as source_content_id ,
                i1.content_id as target_content_id,
                reduce(zip_with(i1.vec, i2.vec, (x,y)->x*y), 0, (s,x)->s+x,s->s)
        --/  pow(reduce(i1.vec,0,(s,x)->s+x*x,s->s),0.5) /pow(reduce(i2.vec, 0,(s, x)->s+x*x, s->s), 0.5)
                as similarity_score
            FROM  item_emb1 as i1  
            INNER JOIN ( 
                    SELECT 
                        content_id,  
                        COUNT(*) AS click 
                    FROM hive_ad.default.action_click ac 
                    INNER JOIN hive.maeda.rakuten_catalog c 
                        ON ac.mc_item_id=c.content_id 
                            and c.included_in_rpp=1
                    WHERE ac.dt BETWEEN  '2020-05-14'  AND '2020-05-20'
                    GROUP BY 1
                    ORDER BY 2 desc 
                    limit 4000
                ) c ON c.content_id=i1.content_id 
            CROSS JOIN item_emb2 as i2     
        )
    ) WHERE rnk<=31
)
    
    SELECT a.*,c.title as original_title FROM (
    SELECT
    COALESCE(c.content_id,i2i.target_content_id) as content_id,
    APPROX_DISTINCT(IF(c.content_id IS NOT NULL, aud.ad_id_plus,NULL)) as matched_users ,
    APPROX_DISTINCT(IF(c.content_id IS NOT NULL OR target_content_id IS NOT NULL,aud.ad_id_plus,NULL)) as relevant_click_users,
    MIN(IF(c.content_id IS NULL   ,i2i.similarity_score,NULL)) as min_score,
    AVG(IF(c.content_id IS NULL   ,i2i.similarity_score,NULL)) AS mean_score,
    MIN(IF(c.content_id IS NULL AND  c2.content_id IS NOT NULL, i2i.similarity_score,NULL)) as sample_low_score,
    MIN_BY(title, IF(c.content_id IS NULL AND c2.content_id IS NOT NULL, i2i.similarity_score,NULL)) as  sample_title_with_low_score,
    MIN_BY(rp.content_id, IF(c.content_id IS NULL AND c2.content_id IS NOT NULL, i2i.similarity_score,NULL)) as  sample_content_id_with_low_score

    FROM 
  (SELECT ad_id,
          regexp_replace(content_id, '^([0-9]+):([0-9a-zA-Z\-_]+):([0-9]+)$', '$2:$3') AS content_id,
          ts
   FROM
     (SELECT split(j(j(ROW, '$._d.PartnerParameters'), '$.content_id'), ',') as content_ids, 
             j(ROW, '$._u') AS ad_id,
             cast(j(ROW, '$.timestamp') AS bigint) AS ts
      FROM hive_ad.DEFAULT.ad_tracker_plain atp
      WHERE atp.dt BETWEEN '2020-05-14' AND '2020-05-20'
        AND j(ROW, '$._d.StoreId') IN ('jp.co.rakuten.android',
                                       '419267350')
        AND j(ROW, '$._m')='adjust:callback:ViewContent' )
   CROSS JOIN UNNEST(content_ids) AS t(content_id))
 rp
    INNER JOIN hive_ad.default.ad_audience_v2 aud ON rp.ad_id=aud.ad_id_plus AND aud.dt='2020-05-20'
    LEFT JOIN i2i ON rp.content_id=i2i.source_content_id
    LEFT JOIN 
   ( SELECT content_id,  COUNT(*) AS click FROM hive_ad.default.action_click ac 
   INNER JOIN hive.maeda.rakuten_catalog c ON ac.mc_item_id=c.content_id and c.included_in_rpp=1
   WHERE ac.dt BETWEEN  '2020-05-14'  AND '2020-05-20'
   GROUP BY 1
   ORDER BY 2 desc limit 4000
   )
 c ON c.content_id=rp.content_id
    LEFT JOIN hive.maeda.rakuten_catalog  c2 ON rp.content_id=c2.content_id
    GROUP BY 1
    ) a INNER JOIN hive.maeda.rakuten_catalog  c ON a.content_id=c.content_id
    
#==========================================================================#

insert into z_seanchuang.i2i_offline_item_topk_items
with item_emb as (
    select 
        element_at(split(item, ':',2),1) as event,  
        element_at(split(item, ':',2),2) as content_id, 
        vec 
    from z_seanchuang.i2i_w2v_features
    where dt='2020-05-30' 
        and feature_id='w20_n2_ft'   
),
item_emb1 as (
    select 
        content_id,
        MAX_BY(vec, 
            case 
                when event='purchase' then 2 
                when event='add_to_cart' then 1 
                else 0 
            end) as vec
    from item_emb
    group by 1
),
item_emb2 as (
    select 
        content_id, 
        vec
    from item_emb
    where event='view_content'
),
item_item_similarity as (
    select 
        a1.content_id as item_a,
        a2.content_id as item_b,
        reduce(zip_with(a1.vec, a2.vec, (x,y)->x*y), 0, (s,x)->s+x, s->s) 
                        / pow(reduce(a1.vec, 0,(s,x)->s+x*x,s->s),0.5) 
                        / pow(reduce(a2.vec, 0,(s,x)->s+x*x,s->s), 0.5) as score
    from item_emb1 a1
    inner join hive.maeda.rakuten_catalog c
        on c.content_id = a1.content_id and c.included_in_rpp = 1
    cross join item_emb1 a2
    where a1.content_id != a2.content_id
)
select 
    item_a as item,
    slice(array_agg(concat(item_b, '=', cast(score AS VARCHAR)) order by score desc), 1, 20) as similar_item,
    '2020-05-30-w20_n2_ft' as dt
from item_item_similarity
group by 1
;




insert into z_seanchuang.i2i_offline_item_topk_items
with item_emb as (
    select 
        element_at(split(item, ':',2),1) as event,  
        element_at(split(item, ':',2),2) as content_id, 
        vec 
    from z_seanchuang.i2i_w2v_features
    where dt='2020-05-30' 
        and feature_id='w20_n2_ft'   
),
item_emb1 as (
    select 
        content_id,
        MAX_BY(vec, 
            case 
                when event='purchase' then 2 
                when event='add_to_cart' then 1 
                else 0 
            end) as vec
    from item_emb
    group by 1
),
item_emb2 as (
    select 
        content_id, 
        vec
    from item_emb
    where event='view_content'
),
item_item_similarity as (
    select 
        a1.content_id as item_a,
        a2.content_id as item_b,
        reduce(zip_with(a1.vec, a2.vec, (x,y)->x*y), 0, (s,x)->s+x, s->s) 
                        / pow(reduce(a1.vec, 0,(s,x)->s+x*x,s->s),0.5) 
                        / pow(reduce(a2.vec, 0,(s,x)->s+x*x,s->s), 0.5) as score
    from item_emb1 a1
    inner join hive.maeda.rakuten_catalog c
        on c.content_id = a1.content_id and c.included_in_rpp = 1
    cross join item_emb1 a2
    where a1.content_id != a2.content_id
)
select 
    item_a as item,
    slice(array_agg(concat(item_b, '=', cast(score AS VARCHAR)) order by score desc), 1, 20) as similar_item,
    '2020-05-30-w20_n2_ft' as dt
from item_item_similarity
group by 1
;

# ============ Final Version ============#
delete from z_seanchuang.tmp_i2i_offline_item_topk_items_info where dt='2020-05-30-w20_n2_ft';

insert into z_seanchuang.i2i_offline_item_topk_items
with catlog_items as (
    select 
        regexp_replace(id,'^([0-9]+):([0-9a-zA-Z\-_]+):([0-9]+)$','$2:$3') as content_id
    from hive.maeda.rakuten_rpp_datafeed 
), 
item_emb as (
    select
        idx,
        item,
        element_at(split(item, ':',2),1) as action,
        element_at(split(item, ':',2),2) as content_id,
        vec
    from z_seanchuang.i2i_w2v_features
    where dt='2020-05-30' 
        and feature_id='w15_256_cbow'
),
item_item_similarity as (
    select 
        a1.content_id as item_a,
        a1.action as item_a_action,
        a2.content_id as item_b,
        max(reduce(zip_with(a1.vec, a2.vec, (x,y)->x*y), 0, (s,x)->s+x, s->s) 
                        / pow(reduce(a1.vec, 0,(s,x)->s+x*x,s->s),0.5) 
                        / pow(reduce(a2.vec, 0,(s,x)->s+x*x,s->s), 0.5)) as score
    from (
            select
                content_id,
                action,
                vec 
            from item_emb
            where idx >= 0 and idx < 100000
        ) a1 
    cross join (select
                    content_id,
                    vec 
                from item_emb
                inner join catlog_items using(content_id)) a2
    group by 1, 2, 3
)
select 
    item_a as item,
    item_a_action as action,
    slice(array_agg((item_b, score) order by score desc), 1, 20) as similar_item,
    '2020-05-30-w20_n2_ft' as dt
from item_item_similarity
group by 1,2 limit 100
;
