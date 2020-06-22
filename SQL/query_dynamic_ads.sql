SELECT 
	v.creative_id, 
	v.dt, 
	count(distinct uuid) as uu, 
	count(DISTINCT v.id) AS campaigns, 
	count(v.ots) AS vimp, 
	round(SUM(click)*1.0/COUNT(*), 5) AS vctr, 
	ROUND(SUM(sales_e6)/1e6*1000/COUNT(*)) AS vrpm, 
	SUM(sales_e6/1e6) AS sales, 
	ROUND(SUM(postback)/SUM(click),5) AS cvr, 
	SUM(postback) AS pb, 
	ROUND(SUM(sales_e6/1e6)/SUM(postback)) AS cpa, 
	SUM(least(social_welfare, 1E5)) as sw 
FROM hive_ad.ml.ad_result_v3 v 
inner join hive_ad.common.ad_result_ext ext 
	on v.ots = ext.ots 
JOIN rds_ad.smartad.campaign_creative cc 
	ON cc.creative_id=v.creative_id 
WHERE v.dt BETWEEN '2020-05-24' AND '2020-05-30' 
	AND ext.dt BETWEEN '2020-05-24' AND '2020-05-30' 
	-- AND cc.merchandise_catalog_id in (70006,70007,70015,70016) 
GROUP BY 1,2 ORDER BY 1,2;
#=================#
SELECT 
	v.dt, 
	count(distinct uuid) as uu, 
	count(DISTINCT v.id) AS campaigns, 
	count(v.ots) AS vimp, 
	round(SUM(click)*1.0/COUNT(*), 5) AS vctr, 
	ROUND(SUM(sales_e6)/1e6*1000/COUNT(*)) AS vrpm, 
	SUM(sales_e6/1e6) AS sales, 
	ROUND(SUM(postback)/SUM(click),5) AS cvr, 
	SUM(postback) AS pb, 
	ROUND(SUM(sales_e6/1e6)/SUM(postback)) AS cpa, 
	SUM(least(social_welfare, 1E5)) as sw 
FROM hive_ad.ml.ad_result_v3 v 
inner join hive_ad.common.ad_result_ext ext 
	on v.ots = ext.ots 
JOIN rds_ad.smartad.campaign_creative cc 
	ON cc.creative_id=v.creative_id 
WHERE v.dt >= '2020-05-24'
	AND ext.dt >= '2020-05-24'
	AND cc.merchandise_catalog_id in (70006,70007,70015,70016) 
GROUP BY 1 
ORDER BY 1;