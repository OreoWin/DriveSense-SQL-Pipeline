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

-- 3. 计算每个 risk_segment 的平均年龄
SELECT 
d.risk_segment,
round(avg(d.age),3) as avg_age
FROM drivers d 
GROUP BY d.risk_segment; 

-- 4. 按州统计总里程 & 平均单次行程距离
SELECT
    d.state,
    count(t.distance_km) as total_km,
    avg(t.distance_km) as avg_trip_km
FROM drivers d
JOIN trips t ON d.driver_id = t.driver_id
GROUP BY d.state;

-- 5. 给行程按距离分桶：short / medium / long
SELECT
trip_id,
distance_km,
    CASE
        WHEN distance_km < 10 THEN 'short'
        WHEN distance_km BETWEEN 10 AND 50 THEN 'medium'
        ELSE 'long'
    END AS distance_bucket
FROM trips;

-- 6.统计夜间行程比例，输出：total_trips, night_trips, night_ratio
SELECT 
		count(trip_id) as total_trips, 
		sum(case when is_night_trip = 1 THEN 1 END) as night_trips, 
		round(sum(case when is_night_trip = 1 THEN 1 END)*1.0/count(trip_id),2) as night_ratio 
FROM trips

-- 7.找出每个司机的总行程数 & 总里程,输出：driver_id, total_trips, total_km
SELECT 
	d.driver_id, 
	count(t.trip_id)as total_trips,
	sum(distance_km) as total_km
FROM drivers d 
JOIN trips t ON d.driver_id = t.driver_id
GROUP BY d.driver_id;

-- 8.找出发生过理赔的司机基本信息, join drivers + claims, 输出：driver_id, age, state, risk_segment, claim_count, total_claim_amount
SELECT 
	d.driver_id, 
	d.age, 
	d.state, 
	d.risk_segment, 
	count(c.claim_id) as claim_count, 
	sum(c.claim_amount) as total_claim_amount
FROM drivers d 
JOIN claims c ON d.driver_id  = c.driver_id 
GROUP BY d.driver_id;

-- 9.分州看理赔率, 理赔率 = 有至少一笔 claim 的司机数 / 该州总司机数, 输出：state, drivers_with_claim, total_drivers, claim_rate
SELECT 
	d.state,
	COUNT(distinct c.driver_id) as drivers_with_claims,
	COUNT(*) as total_drivers,
	round(COUNT(distinct c.driver_id)*1.0/COUNT(*),3) as claim_rate
FROM drivers d 
LEFT JOIN  claims c on d.driver_id  = c.driver_id 
GROUP BY d.state;

-- 10. 脱敏 phone_number，只保留后 4 位，前面用 ***-***- 如 +1-310-555-1234 → ***-***-1234,输出：driver_id, phone_masked
SELECT 
	driver_id,
	concat("***-***- "||substr(phone_number,-4)) as phone_masked
FROM drivers;

-- dynamic data masking 目前用的sqlite不支持，以后可以在BI层mask,sql view, 应用层或者data api里mask


-- 11. license_number 做 hash 模拟（简单版），输出 hash_like = MD5(license_number) 或 concat 'LIC_' || RIGHT(license_number,4)
SELECT 
	d.driver_id,
	concat('******' ||substr(d.license_number,-4)) as hash_like
FROM drivers d 

-- 12. 找出平均单次 claim amount > 5000 的司机,输出：driver_id, avg_claim_amount, claim_count
select 
	driver_id,  
	round(avg(claim_amount),3) as avg_claim_amount,
	count(claim_id) as claim_count
from claims 
group by driver_id
having claim_amount > 5000;

-- 13. 统计按月份的理赔金额,使用 DATE_TRUNC('month', claim_date),输出：month, total_claim_amount, claim_count
select 
	strftime('%Y-%m', claim_date) as month,
	sum(claim_amount) as total_claim_amount, 
	count(claim_id) as claim_count
from claims 
group by "month"
order by "month" DESC

-- 14.用窗口函数计算每个司机累计行程次数,按 trip.start_time 排序, 输出：driver_id, trip_id, start_time, cumulative_trip_no
select 
	driver_id, 
	trip_id, 
	start_time,
	row_number() over (partition by driver_id order by start_time) as cumulative_trip_no
from trips
order by start_time;

-- 15. 计算每个司机的最近一次行程信息，用 ROW_NUMBER() over partition by driver_id order by start_time desc
-- 输出：driver_id, last_trip_id, last_trip_date, last_trip_distance_km
with latest as (
	SELECT 
		driver_id, 
		trip_id, 
		start_time, 
		distance_km, 
		row_number() over (partition by driver_id order by start_time desc) as rn
	from trips
)

select 
	driver_id, 
	trip_id as last_trip_id,
	strftime('%Y-%m-%d',start_time) as last_trip_date,
	distance_km as last_trip_distance_km
from latest 
where rn = 1
order by driver_id;

-- ========================= 普通聚合：每司机一行 =========================
SELECT driver_id,
       SUM(distance_km) AS total_km
FROM trips
GROUP BY driver_id;

--  =========================窗口函数：每次行程一行，但多一个“司机总里程” =========================
SELECT driver_id,
       trip_id,
       distance_km,
       SUM(distance_km) OVER(PARTITION BY driver_id) AS total_km
FROM trips;

-- 16.统计每位司机在所有 trips 中的平均 harsh_event_count，输出：driver_id, avg_harsh_events, total_trips
select 
	driver_id, 
	round(sum(harsh_event_count)*1.0/count(trip_id),3) as avg_harsh_events,
	count(trip_id) as total_trips
from trips
group by driver_id
order by avg_harsh_events desc

-- 17.分州统计每 100 公里 harsh events 数：total_events / total_distance_km * 100，输出：state, events_per_100km
select 
	d.state, 
	round((sum(t.harsh_event_count)/sum(t.distance_km)*100),3)as events_per_100km
from drivers d
join trips t on d.driver_id = t.driver_id
group by d.state
order by events_per_100km DESC;

-- 18.从 events 表计算每位司机的高危行为画像（结合 trips），event_type 计数：hard_brake / rapid_accel / sharp_turn / speeding
输出：driver_id, hard_brake_cnt, rapid_accel_cnt, ...
select 
	t.driver_id,
	sum(case when e.event_type = 'hard_brake' then 1 else 0 end) as hard_brake_cnt,
	sum(case when e.event_type = 'rapid_accel' then 1 else 0 end) as rapid_accel_cnt,
	sum(case when e.event_type = 'sharp_turn' then 1 else 0 end) as sharp_turn_cnt,
	sum(case when e.event_type = 'speeding' then 1 else 0 end) as speeding_cnt
from trips t
join events e on t.trip_id = e.trip_id
group by t.driver_id;

-- 19. 生成一个给 AI 团队用的 “driver_daily_features” 视图（CTE 即可）：
维度：driver_id, 日期
特征：trip_cnt, total_km, harsh_event_cnt, night_trip_cnt

with driver_daily_features as (
	select 
		driver_id,
		strftime('%Y-%m-%d',start_time) as date,
		count(trip_id) as trip_cnt,
		sum(distance_km) as total_km, 
		count(harsh_event_count) as harsh_event_cnt,
		count(is_night_trip) as night_trip_cnt
	from trips 
	group by driver_id,date
)
select 
	driver_id,
	date,
	trip_cnt,
	total_km,
	harsh_event_cnt,
	night_trip_cnt  
from driver_daily_features 
where date between '2020-01-01' and '2025-01-01';

-- 20. 写一个查询返回：能安全发给国内团队的 driver-level 数据：
不能含：phone_number, license_number, vin
可含：driver_id（或 hash 过的 driver_key）、state、age、risk_segment、行为特征
输出：pseudo_driver_key, state, age, risk_segment, avg_km_per_trip, harsh_events_per_100km,driver_level_claim_rate

-- driver_level_claim_rate
with 
	trip_level as (
		select 
			driver_id,
			count(trip_id)as total_trips
		from trips 
		group by driver_id
	),
	claim_level as(
		select 
			driver_id,
			count(claim_id) as total_claims
		from claims 
		group by driver_id
	)
	
select 	
	tl.driver_id,
	round(coalesce(cl.total_claims*1.0/tl.total_trips,0),3) as driver_level_claim_rate
from trip_level tl 
left join claim_level cl on tl.driver_id = cl.driver_id
order by driver_level_claim_rate DESC;

-- 能安全发给国内团队的 driver-level 数据
with 
	trip_level as (
		select 
			driver_id,
			avg(distance_km) as avg_distance_km,
			count(*)as total_trips
		from trips 
		group by driver_id),
	claim_level as(
		select 
			driver_id,
			count(*) as total_claims
		from claims 
		group by driver_id)

select 
	d.driver_id,
	d.state, 
	d.age, 
	d.risk_segment, 
	round(tl.avg_distance_km,3) as avg_km_per_trip,
	round(coalesce(cl.total_claims*1.0/tl.total_trips,0),3) as driver_level_claim_rate
from drivers d 
left join trip_level tl on d.driver_id  = tl.driver_id 
left join claim_level cl on d.driver_id = cl.driver_id
group by d.driver_id;

-- 21. 做一个数据质量检查：
检查是否存在 trip 记录中的 driver_id，在 drivers 表中不存在（孤儿记录）
输出：所有异常 trip
select 
	t.driver_id,
	t.trip_id
from trips t 
left join drivers d on t.driver_id = d.driver_id
where d.driver_id is null

-- 22.检查 claims 中是否有 vehicle_id 在 vehicles 表中不存在的情况。
select 
	c.vehicle_id,
	v.vin
from claims c
left join vehicles v on c.vehicle_id = v.vehicle_id
where v.vehicle_id is null

-- 23. 检查 trips 中 end_time < start_time 的异常行程（应该为 0 行，如果不为 0 就是你要报 bug 的）。
select 
	trip_id,
	driver_id,
	start_time,
	end_time
from trips 
where end_time < start_time

-- 24. 构造一个“高风险司机名单”：
harsh_events_per_100km 高于全局平均
claim_rate 高于全局平均
夜间行程比例高
输出：driver_id, state, harsh_events_per_100km, claim_rate, night_trip_ratio

select 
	driver_id,
	round((sum(harsh_event_count)/sum(distance_km))*100,3) as avg_harsh_events_per_100km,  -- 1.048
	round(sum(is_night_trip)*1.0/count(trip_id),3) as avg_night_trip_ratio          --0.316
from trips

select 
	round(count(claim_id)*1.0/(select count(*) from trips),3) as avg_claim_ratio --0.178
from claims 

-------------------------------------------------------------------------------------------------
with a as (select 
	d.driver_id, 
	d.state,
	round((sum(t.harsh_event_count)/sum(t.distance_km))*100,3) as driver_level_harsh_events_per_100km,
	round(sum(is_night_trip)*1.0/count(trip_id),3)as driver_level_night_trip_ratio
from drivers d
join trips t on d.driver_id = t.driver_id 
group by d.driver_id),
	
b as (
	select 
		driver_id,
		count(*) as number_of_claims
	from claims 
	group by driver_id ),

c as (
	select 
		driver_id,
		count(*) as number_of_trips
	from trips 
	group by driver_id)

select 
	a.driver_id,
	a.state,
	a.driver_level_harsh_events_per_100km,
	a.driver_level_night_trip_ratio,
	round(b.number_of_claims*1.0/c.number_of_trips,3) as claim_rate
from a
left join b on a.driver_id = b.driver_id
left join c on a.driver_id = c.driver_id
where 
	driver_level_harsh_events_per_100km > 1.048 
	AND driver_level_night_trip_ratio > 0.316 
	AND claim_rate > 0.178

-- 25. 写一个“运营看板”的 SQL：按州输出
driver_cnt
total_km
events_per_100km
claim_rate              --  state_level_claim_rate = # claims/#ppl_in_state
avg_claim_amount
并按 events_per_100km 从高到低排序。
with x as (
	select 
		d.state,
		round(avg(c.claim_amount),3) as avg_claim_amount,
		count(distinct d.driver_id) as ppl_cnt_state,
		count(distinct c.driver_id) as claims_cnt_state
	from drivers d 
	left join claims c on d.driver_id = c.driver_id
	group by d.state
)

select 
	d.state,
	count(d.driver_id) as driver_cnt,
	sum(t.distance_km) as total_km,
	round((sum(t.harsh_event_count)/sum(t.distance_km))*100,3) as events_per_100km,
	round(x.claims_cnt_state*1.0/x.ppl_cnt_state,3) as claim_rate, 
	x.avg_claim_amount
from drivers d
left join trips t on d.driver_id = t.driver_id
left join x on d.state = x.state
group by d.state
order by events_per_100km DESC;


-- 针对性练习
-- 1. where & left join的冲突：找出所有 没有出过任何 claim 的司机
select 
	d.driver_id, 
	d.state
from drivers d 
left join claims c on c.driver_id = d.driver_id 
where c.driver_id is null

-- 2. group by:统计每个州的司机数，平均年龄，最大年龄
select 
	state,
	count(driver_id) as driver_cnt,
	round(avg(age),3) as driver_avg_age,
	max(age) as max_age
from drivers 
group by state









