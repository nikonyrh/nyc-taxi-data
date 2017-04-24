
SELECT
  COUNT(1) AS n_rows
FROM taxi_trips
WHERE
  pickup_dt BETWEEN '2015-12-18 00:00:00' AND '2016-01-01 00:00:00' AND
  pickup_lat BETWEEN 40.75666699610511 AND 40.77463330926793 AND
  pickup_lon BETWEEN -74.06452329287113 AND -74.04655697970831


SELECT
  pickup_dt_datehist_1d,
  company,
  sum(paid_total_travel_km) AS sum_paid_total_travel_km,
  min(paid_total_travel_km) AS min_paid_total_travel_km,
  max(paid_total_travel_km) AS max_paid_total_travel_km,
  SUM(1) AS n_rows
FROM (
  SELECT
    pickup_dt_datehist_1d,
    company,
    paid_total_travel_km
  FROM (
    SELECT
      FLOOR(CAST(CAST(pickup_dt AS datetime) AS float)) AS pickup_dt_datehist_1d,
      company,
      paid_total / travel_km AS paid_total_travel_km
    FROM taxi_trips
    WHERE
      pickup_dt BETWEEN '2015-07-11 00:00:00' AND '2015-07-25 00:00:00' AND
      pickup_lat BETWEEN 40.7868970226319 AND 40.804863335794714 AND
      pickup_lon BETWEEN -73.97067002713264 AND -73.95270371396983
  ) t
) t GROUP BY pickup_dt_datehist_1d, company


SELECT
  pickup_time_hist_2,
  sum(paid_total) AS sum_paid_total,
  min(paid_total) AS min_paid_total,
  max(paid_total) AS max_paid_total,
  SUM(1) AS n_rows
FROM (
  SELECT
    pickup_time_hist_2,
    paid_total
  FROM (
    SELECT
      FLOOR(pickup_time / 2.0) AS pickup_time_hist_2,
      paid_total
    FROM taxi_trips
    WHERE
      pickup_dt BETWEEN '2013-08-18 00:00:00' AND '2013-09-01 00:00:00' AND
      pickup_lat BETWEEN 40.6913502559109 AND 40.709316569073714 AND
      pickup_lon BETWEEN -73.9376796754615 AND -73.91971336229868
  ) t
) t GROUP BY pickup_time_hist_2


SELECT
  pickup_dt_datehist_1d,
  MIN(speed_kmph_p05) AS speed_kmph_p05,
  MIN(speed_kmph_p10) AS speed_kmph_p10,
  MIN(speed_kmph_p25) AS speed_kmph_p25,
  MIN(speed_kmph_p50) AS speed_kmph_p50,
  MIN(speed_kmph_p75) AS speed_kmph_p75,
  MIN(speed_kmph_p90) AS speed_kmph_p90,
  MIN(speed_kmph_p95) AS speed_kmph_p95,
  SUM(1) AS n_rows
FROM (
  SELECT
    pickup_dt_datehist_1d,
    PERCENTILE_DISC(0.05) WITHIN GROUP (ORDER BY speed_kmph) OVER (PARTITION BY pickup_dt_datehist_1d) AS speed_kmph_p05,
    PERCENTILE_DISC(0.10) WITHIN GROUP (ORDER BY speed_kmph) OVER (PARTITION BY pickup_dt_datehist_1d) AS speed_kmph_p10,
    PERCENTILE_DISC(0.25) WITHIN GROUP (ORDER BY speed_kmph) OVER (PARTITION BY pickup_dt_datehist_1d) AS speed_kmph_p25,
    PERCENTILE_DISC(0.50) WITHIN GROUP (ORDER BY speed_kmph) OVER (PARTITION BY pickup_dt_datehist_1d) AS speed_kmph_p50,
    PERCENTILE_DISC(0.75) WITHIN GROUP (ORDER BY speed_kmph) OVER (PARTITION BY pickup_dt_datehist_1d) AS speed_kmph_p75,
    PERCENTILE_DISC(0.90) WITHIN GROUP (ORDER BY speed_kmph) OVER (PARTITION BY pickup_dt_datehist_1d) AS speed_kmph_p90,
    PERCENTILE_DISC(0.95) WITHIN GROUP (ORDER BY speed_kmph) OVER (PARTITION BY pickup_dt_datehist_1d) AS speed_kmph_p95
  FROM (
    SELECT
      FLOOR(CAST(CAST(pickup_dt AS datetime) AS float)) AS pickup_dt_datehist_1d,
      speed_kmph
    FROM taxi_trips
    WHERE
      pickup_dt BETWEEN '2014-04-23 00:00:00' AND '2014-05-07 00:00:00' AND
      pickup_lat BETWEEN 40.73780147029229 AND 40.755767783455106 AND
      pickup_lon BETWEEN -74.02158724138101 AND -74.0036209282182
  ) t
) t GROUP BY pickup_dt_datehist_1d


SELECT
  COUNT(1) AS n_rows
FROM taxi_trips
WHERE
  pickup_dt BETWEEN '2014-10-28 00:00:00' AND '2014-12-27 00:00:00' AND
  paid_tip BETWEEN 7.097786820833247 AND 9.097786820833246 AND
  pickup_day = 4


SELECT
  pickup_dt_datehist_1d,
  company,
  sum(paid_total_travel_km) AS sum_paid_total_travel_km,
  min(paid_total_travel_km) AS min_paid_total_travel_km,
  max(paid_total_travel_km) AS max_paid_total_travel_km,
  SUM(1) AS n_rows
FROM (
  SELECT
    pickup_dt_datehist_1d,
    company,
    paid_total_travel_km
  FROM (
    SELECT
      FLOOR(CAST(CAST(pickup_dt AS datetime) AS float)) AS pickup_dt_datehist_1d,
      company,
      paid_total / travel_km AS paid_total_travel_km
    FROM taxi_trips
    WHERE
      pickup_dt BETWEEN '2014-04-25 00:00:00' AND '2014-06-24 00:00:00' AND
      paid_tip BETWEEN 10.177288677122187 AND 12.177288677122187 AND
      pickup_day = 6
  ) t
) t GROUP BY pickup_dt_datehist_1d, company


SELECT
  pickup_time_hist_2,
  sum(paid_total) AS sum_paid_total,
  min(paid_total) AS min_paid_total,
  max(paid_total) AS max_paid_total,
  SUM(1) AS n_rows
FROM (
  SELECT
    pickup_time_hist_2,
    paid_total
  FROM (
    SELECT
      FLOOR(pickup_time / 2.0) AS pickup_time_hist_2,
      paid_total
    FROM taxi_trips
    WHERE
      pickup_dt BETWEEN '2013-02-05 00:00:00' AND '2013-04-06 00:00:00' AND
      paid_tip BETWEEN 4.981247963060202 AND 6.981247963060202 AND
      pickup_day = 7
  ) t
) t GROUP BY pickup_time_hist_2


SELECT
  pickup_dt_datehist_1d,
  MIN(speed_kmph_p05) AS speed_kmph_p05,
  MIN(speed_kmph_p10) AS speed_kmph_p10,
  MIN(speed_kmph_p25) AS speed_kmph_p25,
  MIN(speed_kmph_p50) AS speed_kmph_p50,
  MIN(speed_kmph_p75) AS speed_kmph_p75,
  MIN(speed_kmph_p90) AS speed_kmph_p90,
  MIN(speed_kmph_p95) AS speed_kmph_p95,
  SUM(1) AS n_rows
FROM (
  SELECT
    pickup_dt_datehist_1d,
    PERCENTILE_DISC(0.05) WITHIN GROUP (ORDER BY speed_kmph) OVER (PARTITION BY pickup_dt_datehist_1d) AS speed_kmph_p05,
    PERCENTILE_DISC(0.10) WITHIN GROUP (ORDER BY speed_kmph) OVER (PARTITION BY pickup_dt_datehist_1d) AS speed_kmph_p10,
    PERCENTILE_DISC(0.25) WITHIN GROUP (ORDER BY speed_kmph) OVER (PARTITION BY pickup_dt_datehist_1d) AS speed_kmph_p25,
    PERCENTILE_DISC(0.50) WITHIN GROUP (ORDER BY speed_kmph) OVER (PARTITION BY pickup_dt_datehist_1d) AS speed_kmph_p50,
    PERCENTILE_DISC(0.75) WITHIN GROUP (ORDER BY speed_kmph) OVER (PARTITION BY pickup_dt_datehist_1d) AS speed_kmph_p75,
    PERCENTILE_DISC(0.90) WITHIN GROUP (ORDER BY speed_kmph) OVER (PARTITION BY pickup_dt_datehist_1d) AS speed_kmph_p90,
    PERCENTILE_DISC(0.95) WITHIN GROUP (ORDER BY speed_kmph) OVER (PARTITION BY pickup_dt_datehist_1d) AS speed_kmph_p95
  FROM (
    SELECT
      FLOOR(CAST(CAST(pickup_dt AS datetime) AS float)) AS pickup_dt_datehist_1d,
      speed_kmph
    FROM taxi_trips
    WHERE
      pickup_dt BETWEEN '2015-02-20 00:00:00' AND '2015-04-21 00:00:00' AND
      paid_tip BETWEEN 10.772842941696684 AND 12.772842941696684 AND
      pickup_day = 3
  ) t
) t GROUP BY pickup_dt_datehist_1d


SELECT
  COUNT(1) AS n_rows
FROM taxi_trips
WHERE
  travel_h BETWEEN 1.6494241886378544 AND 1.8994241886378544 AND
  n_passengers = 5


SELECT
  pickup_dt_datehist_1d,
  company,
  sum(paid_total_travel_km) AS sum_paid_total_travel_km,
  min(paid_total_travel_km) AS min_paid_total_travel_km,
  max(paid_total_travel_km) AS max_paid_total_travel_km,
  SUM(1) AS n_rows
FROM (
  SELECT
    pickup_dt_datehist_1d,
    company,
    paid_total_travel_km
  FROM (
    SELECT
      FLOOR(CAST(CAST(pickup_dt AS datetime) AS float)) AS pickup_dt_datehist_1d,
      company,
      paid_total / travel_km AS paid_total_travel_km
    FROM taxi_trips
    WHERE
      travel_h BETWEEN 0.874232680563546 AND 1.124232680563546 AND
      n_passengers = 6
  ) t
) t GROUP BY pickup_dt_datehist_1d, company


SELECT
  pickup_time_hist_2,
  sum(paid_total) AS sum_paid_total,
  min(paid_total) AS min_paid_total,
  max(paid_total) AS max_paid_total,
  SUM(1) AS n_rows
FROM (
  SELECT
    pickup_time_hist_2,
    paid_total
  FROM (
    SELECT
      FLOOR(pickup_time / 2.0) AS pickup_time_hist_2,
      paid_total
    FROM taxi_trips
    WHERE
      travel_h BETWEEN 1.1109378064982054 AND 1.3609378064982054 AND
      n_passengers = 1
  ) t
) t GROUP BY pickup_time_hist_2


SELECT
  pickup_dt_datehist_1d,
  MIN(speed_kmph_p05) AS speed_kmph_p05,
  MIN(speed_kmph_p10) AS speed_kmph_p10,
  MIN(speed_kmph_p25) AS speed_kmph_p25,
  MIN(speed_kmph_p50) AS speed_kmph_p50,
  MIN(speed_kmph_p75) AS speed_kmph_p75,
  MIN(speed_kmph_p90) AS speed_kmph_p90,
  MIN(speed_kmph_p95) AS speed_kmph_p95,
  SUM(1) AS n_rows
FROM (
  SELECT
    pickup_dt_datehist_1d,
    PERCENTILE_DISC(0.05) WITHIN GROUP (ORDER BY speed_kmph) OVER (PARTITION BY pickup_dt_datehist_1d) AS speed_kmph_p05,
    PERCENTILE_DISC(0.10) WITHIN GROUP (ORDER BY speed_kmph) OVER (PARTITION BY pickup_dt_datehist_1d) AS speed_kmph_p10,
    PERCENTILE_DISC(0.25) WITHIN GROUP (ORDER BY speed_kmph) OVER (PARTITION BY pickup_dt_datehist_1d) AS speed_kmph_p25,
    PERCENTILE_DISC(0.50) WITHIN GROUP (ORDER BY speed_kmph) OVER (PARTITION BY pickup_dt_datehist_1d) AS speed_kmph_p50,
    PERCENTILE_DISC(0.75) WITHIN GROUP (ORDER BY speed_kmph) OVER (PARTITION BY pickup_dt_datehist_1d) AS speed_kmph_p75,
    PERCENTILE_DISC(0.90) WITHIN GROUP (ORDER BY speed_kmph) OVER (PARTITION BY pickup_dt_datehist_1d) AS speed_kmph_p90,
    PERCENTILE_DISC(0.95) WITHIN GROUP (ORDER BY speed_kmph) OVER (PARTITION BY pickup_dt_datehist_1d) AS speed_kmph_p95
  FROM (
    SELECT
      FLOOR(CAST(CAST(pickup_dt AS datetime) AS float)) AS pickup_dt_datehist_1d,
      speed_kmph
    FROM taxi_trips
    WHERE
      travel_h BETWEEN 1.8193608755737294 AND 2.0693608755737296 AND
      n_passengers = 4
  ) t
) t GROUP BY pickup_dt_datehist_1d

