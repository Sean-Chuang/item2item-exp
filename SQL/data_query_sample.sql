SELECT 
	cast(Json_extract(row, '$._d.content_ids') AS ARRAY<varchar>) AS content_ids, 
	cast(j(row, '$.timestamp') AS bigint) AS ts, j(row,'$._d.exid[0]') as exid, 
	j(row, '$._u') as guid, try_cast(j(row,'$._d.uplift_rate[0]') as double) as uplift_rate , 
	row 
FROM hive_ad.DEFAULT.ad_tracker_plain atp 
WHERE atp.dt BETWEEN '2020-05-01' AND '2020-05-01' 
	AND j(row, '$._d.e[0]')='ViewContent' 
	AND j(row, '$._d.url[0]')='https://grp15.ias.rakuten.co.jp/gw.js?v=2' 
limit 2