delete from z_seanchuang.i2i_offline_item_cooccurrence where dt='2020-05-30';

insert into z_seanchuang.i2i_offline_item_cooccurrence
with user_item as (
    select 
        ad_id, 
        content_id
    from z_seanchuang.i2i_offline_train_raw 
),
cooccurrence_table as (
    select 
        ad_id,
        a1.content_id as item1,
        a2.content_id as item2
    from user_item a1
    join user_item a2 using(ad_id)
)
select 
    item1,
    item2,
    count(*) as cnt,
    '2020-05-30' as dt
from cooccurrence_table
group by 1, 2; 

# Given v: 
#   sim(u, v) = freq(u & v) / (freq(v) * freq(u)^0.5)
delete from z_seanchuang.i2i_offline_item_item_similarity where dt='2020-05-30';

insert into z_seanchuang.i2i_offline_item_item_similarity
with item_self_count as (
    select 
        item_a as item,
        cnt
    from z_seanchuang.i2i_offline_item_cooccurrence
    where item_a = item_b
)
select 
    item_a,
    item_b,
    c.cnt,
    s1.cnt as a_cnt,
    s2.cnt as b_cnt,
    c.cnt / (s1.cnt * pow(s2.cnt, 0.5)) as score,
    '2020-05-30' as dt
from z_seanchuang.i2i_offline_item_cooccurrence c
left join item_self_count s1 on c.item_a = s1.item
left join item_self_count s2 on c.item_b = s2.item
where item_a != item_b
;



delete from z_seanchuang.i2i_offline_item_topk_items where dt='2020-05-30';

insert into z_seanchuang.i2i_offline_item_topk_items
select 
    item_a as item,
    slice(array_agg(concat(item_b, '=', cast(score AS VARCHAR)) order by score desc), 1, 20) as similar_item,
    '2020-05-30' as dt
from z_seanchuang.i2i_offline_item_item_similarity
group by 1
;



-- insert into z_seanchuang.i2i_offline_item_topk_items
-- select 
--     item_a as item,
--     CAST(MAP_FROM_ENTRIES(slice(array_agg(row(item_b, score) order by score desc), 1, 20)) AS JSON) as similar_item,
--     '2020-05-30' as dt
-- from z_seanchuang.i2i_offline_item_item_similarity
-- group by 1
-- ;


#=========== Final version ===========#
### Similarity 0 :
#  Given v: 
#    sim(u, v) = freq(u & v) / (freq(v) * freq(u)^0.5)
insert into z_seanchuang.i2i_offline_item_topk_items
with user_item as (
    select 
        ad_id, 
        content_id
    from z_seanchuang.i2i_offline_train_raw
    where dt='2020-05-30'
),
cooccurrence_table as (
    select 
        ad_id,
        user_weight,
        a1.content_id as item1,
        a2.content_id as item2
    from user_item a1
    join user_item a2 using(ad_id)
    left join user_weight using(ad_id)
),
item_cooccurrence as (
    select 
        item1 as item_a,
        item2 as item_b,
        count(*) as cnt
    from cooccurrence_table
    group by 1, 2
),
item_self_count as (
    select 
        item_a as item,
        cnt
    from item_cooccurrence
    where item_a = item_b
),
item_item_similarity as (
    select 
        item_a,
        item_b,
        c.cnt,
        s1.cnt as a_cnt,
        s2.cnt as b_cnt,
        c.cnt / (s1.cnt * pow(s2.cnt, 0.5)) as score
    from item_cooccurrence c
    left join item_self_count s1 on c.item_a = s1.item
    left join item_self_count s2 on c.item_b = s2.item
    where item_a != item_b
)
select 
    item_a as item,
    slice(array_agg(concat(item_b, '=', cast(score AS VARCHAR)) order by score desc), 1, 20) as similar_item,
    '2020-05-30-user-org' as dt
from item_item_similarity
group by 1
;



### Similarity 1 : 
#  Given v: 
#    sim(u, v) = w_freq(u & v) / (freq(v) * freq(u)^0.5)
delete from z_seanchuang.i2i_offline_item_topk_items where dt='2020-06-30';

insert into z_seanchuang.i2i_offline_item_topk_items
with user_weight as (
    select 
        ad_id, 
        1.0 / sqrt(count(*)) as user_weight
    from z_seanchuang.i2i_offline_train_raw
    where dt='2020-06-30' 
    group by 1
),
user_item as (
    select 
        ad_id, 
        content_id
    from z_seanchuang.i2i_offline_train_raw 
    where dt='2020-06-30' 
),
cooccurrence_table as (
    select 
        ad_id,
        user_weight,
        a1.content_id as item1,
        a2.content_id as item2
    from user_item a1
    join user_item a2 using(ad_id)
    left join user_weight using(ad_id)
),
item_cooccurrence as (
    select 
        item1 as item_a,
        item2 as item_b,
        sum(user_weight) as weight,
        count(*) as cnt
    from cooccurrence_table
    group by 1, 2
),
item_self_count as (
    select 
        item_a as item,
        cnt
    from item_cooccurrence
    where item_a = item_b
        and cnt > 3
),
item_item_similarity as (
    select 
        item_a,
        item_b,
        c.weight,
        s1.cnt as a_cnt,
        s2.cnt as b_cnt,
        c.weight / (s1.cnt * pow(s2.cnt, 0.1)) as score
    from item_cooccurrence c
    inner join item_self_count s1 on c.item_a = s1.item
    inner join item_self_count s2 on c.item_b = s2.item
    -- where item_a != item_b
)
select 
    item_a as item,
    slice(array_agg(concat(item_b, '=', cast(score AS VARCHAR)) order by score desc), 1, 20) as similar_item,
    '2020-06-30' as dt
from item_item_similarity
group by 1
;


# ========================================= #
### Similarity 2 : 
#  Given v: 
#    u_weight = 1/ log2(3 + count(*))
#    sim(u, v) = w_freq(u & v) / (w_freq(v)^0.5 * w_freq(u)^0.5)
insert into z_seanchuang.i2i_offline_item_topk_items
with user_weight as (
    select 
        ad_id, 
        1.0 / log2(3 + count(*)) as user_weight
    from z_seanchuang.i2i_offline_train_raw
    where dt='2020-05-30'
    group by 1
),
user_item as (
    select 
        ad_id, 
        content_id
    from z_seanchuang.i2i_offline_train_raw 
    where dt='2020-05-30'
),
cooccurrence_table as (
    select 
        ad_id,
        user_weight,
        a1.content_id as item1,
        a2.content_id as item2
    from user_item a1
    join user_item a2 using(ad_id)
    left join user_weight using(ad_id)
),
item_cooccurrence as (
    select 
        item1 as item_a,
        item2 as item_b,
        sum(pow(user_weight, 2)) as weight
    from cooccurrence_table
    group by 1, 2
),
item_self_count as (
    select 
        item_a as item,
        weight as w
    from item_cooccurrence
    where item_a = item_b
),
item_item_similarity as (
    select 
        item_a,
        item_b,
        c.weight,
        s1.w as a_w,
        s2.w as b_w,
        c.weight / (pow(s1.w, 0.5) * pow(s2.w, 0.5)) as score
    from item_cooccurrence c
    left join item_self_count s1 on c.item_a = s1.item
    left join item_self_count s2 on c.item_b = s2.item
    where item_a != item_b
)
select 
    item_a as item,
    slice(array_agg(concat(item_b, '=', cast(score AS VARCHAR)) order by score desc), 1, 20) as similar_item,
    '2020-05-30-user-w2' as dt
from item_item_similarity
group by 1
;
