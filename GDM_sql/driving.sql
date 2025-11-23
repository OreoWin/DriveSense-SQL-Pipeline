-- 1. 统计每个州的司机数量
SELECT 
state,
count(*) as driver_count  
FROM drivers d 
GROUP BY d.state 

-- 2. 统计男女司机数量 & 占比
SELECT 
gender,
count(*) as cnt, 
count(*)*1.0 /(select count(*) from drivers) as pct
FROM drivers d 
GROUP BY d.gender

--3. 计算每个 risk_segment 的平均年龄
SELECT 
d.risk_segment,
round(avg(d.age),3) as avg_age
FROM drivers d 
GROUP BY d.risk_segment; 

-- 4. 按州统计总里程和平均单次行程公里数，输出：state, total_km, avg_trip_km
SELECT 
d. state,
sum(t.distance_km)as total_km, --最开始用的是count()
avg(t.distance_km)as avg_trip_km
FROM drivers d
JOIN trips t ON d.driver_id  = t.driver_id
GROUP BY d.state
ORDER BY total_km;

-- 5. 用 CASE WHEN 给行程打标签：distance_km < 10 → short， 10–50 → medium， 50 → long
--输出：trip_id, distance_km, distance_bucket
SELECT trip_id,distance_km,
CASE WHEN  distance_km < 10 THEN "short"
WHEN distance_km BETWEEN 10 AND 50 THEN "medium"
ELSE "long" END as distance_bucket 
FROM trips -- 不需要group by 因为根本没有聚合（avg/sum)

-- 6. 统计夜间行程比例，输出：total_trips, night_trips, night_ratio
