# DriveSense-SQL-Pipeline

A lightweight telematics data platform for driving-behavior analytics, safety scoring, and downstream AI feature generation.

---
This project simulates an automotive industry-style dataset with drivers, vehicles, trips, events, and claims. The primary goal is to practice SQL for analytics, data quality checks, and feature engineering. 

## Data

All CSV files are under `data/`:

- `drivers.csv` – demographics, state, risk segment, PII
- `vehicles.csv` – VIN, make/model, mapping to drivers
- `trips.csv` – timestamps, distance, night driving, harsh events
- `events.csv` – hard brakes, rapid acceleration, speeding, sharp turns
- `claims.csv` – claim type, amounts, shop info, approval status
- `repair_shops.csv` – partner vs non-partner shops across states

Database: **SQLite**, queries written and tested in **DBeaver**.

```sql
CREATE TABLE drivers (
  driver_id       VARCHAR(10) PRIMARY KEY,
  signup_date     DATE,
  state           VARCHAR(2),
  age             INT,
  gender          VARCHAR(1),
  risk_segment    VARCHAR(10),   -- low / medium / high
  phone_number    VARCHAR(20),
  license_number  VARCHAR(20),
  is_us_based     BOOLEAN
);

CREATE TABLE vehicles (
  vehicle_id   VARCHAR(10) PRIMARY KEY,
  driver_id    VARCHAR(10) REFERENCES drivers(driver_id),
  vin          VARCHAR(20),
  make         VARCHAR(20),
  model        VARCHAR(30),
  model_year   INT
);

CREATE TABLE trips (
  trip_id            VARCHAR(10) PRIMARY KEY,
  driver_id          VARCHAR(10) REFERENCES drivers(driver_id),
  vehicle_id         VARCHAR(10) REFERENCES vehicles(vehicle_id),
  start_time         TIMESTAMP,
  end_time           TIMESTAMP,
  distance_km        NUMERIC(10,1),
  harsh_event_count  INT,
  is_night_trip      BOOLEAN
);

CREATE TABLE events (
  event_id    VARCHAR(10) PRIMARY KEY,
  trip_id     VARCHAR(10) REFERENCES trips(trip_id),
  event_time  TIMESTAMP,
  event_type  VARCHAR(20),  -- hard_brake / rapid_accel / sharp_turn / speeding
  severity    INT           -- 1 ~ 5
);

CREATE TABLE repair_shops (
  repair_shop_id  VARCHAR(10) PRIMARY KEY,
  shop_name       VARCHAR(50),
  city            VARCHAR(50),
  state           VARCHAR(2),
  is_partner      BOOLEAN
);

CREATE TABLE claims (
  claim_id        VARCHAR(10) PRIMARY KEY,
  driver_id       VARCHAR(10) REFERENCES drivers(driver_id),
  vehicle_id      VARCHAR(10) REFERENCES vehicles(vehicle_id),
  claim_date      DATE,
  claim_type      VARCHAR(20),
  claim_amount    NUMERIC(12,2),
  repair_shop_id  VARCHAR(10) REFERENCES repair_shops(repair_shop_id),
  is_approved     BOOLEAN
);

```

## Highlighted Tasks 

This project includes **27 SQL tasks** that closely resemble the business scenarios of companies in the automotive industry. 

1. Find the basic information of drivers who have filed claims.
找出发生过理赔的司机基本信息

```sql
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
```
2. Find claim rate by state. 
分州看理赔率, 理赔率 = 有至少一笔 claim 的司机数 / 该州总司机数
```sql
SELECT 
	d.state,
	COUNT(distinct c.driver_id) as drivers_with_claims,
	COUNT(*) as total_drivers,
	round(COUNT(distinct c.driver_id)*1.0/COUNT(*),3) as claim_rate
FROM drivers d 
LEFT JOIN  claims c on d.driver_id  = c.driver_id 
GROUP BY d.state;
```
3. Pseudo phone number
脱敏 'phone_number'，只保留后 4 位，前面用 ***-***- 如 +1-310-555-1234 → ***-***-1234

```sql
SELECT 
	driver_id,
	concat("***-***- "||substr(phone_number,-4)) as phone_masked
FROM drivers;
```
SQLite doesn't support dynamic data masking or 'MD5()'

4. Find drivers with an average single claim amount > 5000
找出平均单次 claim amount > 5000 的司机

```sql
select 
	driver_id,  
	round(avg(claim_amount),3) as avg_claim_amount,
	count(claim_id) as claim_count
from claims 
group by driver_id
having claim_amount > 5000;

```
5. Find all the information for each driver with their latest trip
计算每个司机的最近一次行程信息

```sql
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
```

6. Find 'avg_harsh_event_count' for each driver in their past trips.
统计每位司机在所有 trips 中的平均 harsh_event_count
```sql
select 
	driver_id, 
	round(sum(harsh_event_count)*1.0/count(trip_id),3) as avg_harsh_events,
	count(trip_id) as total_trips
from trips
group by driver_id
order by avg_harsh_events desc

```

7. high-risk behavior profile for each driver
从 events 表计算每位司机的高危行为画像（结合 trips），event_type 计数：hard_brake / rapid_accel / sharp_turn / speeding
```sql
select 
	t.driver_id,
	sum(case when e.event_type = 'hard_brake' then 1 else 0 end) as hard_brake_cnt,
	sum(case when e.event_type = 'rapid_accel' then 1 else 0 end) as rapid_accel_cnt,
	sum(case when e.event_type = 'sharp_turn' then 1 else 0 end) as sharp_turn_cnt,
	sum(case when e.event_type = 'speeding' then 1 else 0 end) as speeding_cnt
from trips t
join events e on t.trip_id = e.trip_id
group by t.driver_id;
```
8. Create a view, 'driver_daily_features'
生成一个 “driver_daily_features” 视图（CTE 即可）
维度：driver_id, 日期
特征：trip_cnt, total_km, harsh_event_cnt, night_trip_cnt

```sql
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
```
9. driver_level_info
写一个查询返回：能安全发给国内团队的 driver-level 数据：
不能含：phone_number, license_number, vin
可含：driver_id（或 hash 过的 driver_key）、state、age、risk_segment、行为特征
输出：pseudo_driver_key, state, age, risk_segment, avg_km_per_trip, harsh_events_per_100km,driver_level_claim_rate

```sql 
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
```




## Skills Highlighted 
- SQL (joins, windows, ETL, data checks)
- Feature Engineering
- Data Modeling
- Telematics / Driving Behavior Analytics
- Privacy-aware data handling
- Cross-border data workflow design



