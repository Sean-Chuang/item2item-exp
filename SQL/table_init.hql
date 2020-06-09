create table if not exists z_seanchuang.i2i_offline_train_raw (
    ad_id string,
    content_id string,
    behavior_types string,
    ts bigint
)
partitioned by (dt string)
row format delimited fields terminated by '\t'
lines terminated by '\n'
stored as textfile
location 's3://smartad-dmp/warehouse/user/seanchuang/i2i_offline_train_raw';

create table if not exists z_seanchuang.i2i_offline_test_raw (
    ad_id string,
    content_id string,
    behavior_types string,
    ts bigint
)
partitioned by (dt string)
row format delimited fields terminated by '\t'
lines terminated by '\n'
stored as textfile
location 's3://smartad-dmp/warehouse/user/seanchuang/i2i_offline_test_raw';