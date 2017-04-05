SELECT
  COUNT(1) AS n_rows
FROM taxi_trips
WHERE
  pickup_dt BETWEEN '2016-02-01 00:00:00' AND '2016-02-15 00:00:00' AND
  pickup_lat BETWEEN 40.72779496275363 AND 40.74576127591644 AND
  pickup_lon BETWEEN -73.9020881976274 AND -73.88412188446459


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
      pickup_dt BETWEEN '2015-07-27 00:00:00' AND '2015-08-10 00:00:00' AND
      pickup_lat BETWEEN 40.73188095430312 AND 40.74984726746594 AND
      pickup_lon BETWEEN -73.98743667806977 AND -73.96947036490695
  ) t
) t GROUP BY pickup_time_hist_2


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
      pickup_dt BETWEEN '2014-05-04 00:00:00' AND '2014-05-18 00:00:00' AND
      pickup_lat BETWEEN 40.72914802633311 AND 40.74711433949592 AND
      pickup_lon BETWEEN -73.99845200999427 AND -73.98048569683145
  ) t
) t GROUP BY pickup_dt_datehist_1d, company


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
      pickup_dt BETWEEN '2013-02-11 00:00:00' AND '2013-02-25 00:00:00' AND
      pickup_lat BETWEEN 40.79138107867433 AND 40.80934739183714 AND
      pickup_lon BETWEEN -74.03992678516114 AND -74.02196047199833
  ) t
) t GROUP BY pickup_dt_datehist_1d


SELECT
  COUNT(1) AS n_rows
FROM taxi_trips
WHERE
  travel_h BETWEEN 2.539472570934696 AND 2.789472570934696 AND
  n_passengers = 1


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
      travel_h BETWEEN 0.8413569620106552 AND 1.0913569620106554 AND
      n_passengers = 5
  ) t
) t GROUP BY pickup_time_hist_2


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
      travel_h BETWEEN 1.6064776867442159 AND 1.8564776867442159 AND
      n_passengers = 5
  ) t
) t GROUP BY pickup_dt_datehist_1d, company


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
      travel_h BETWEEN 0.97411637889984 AND 1.22411637889984 AND
      n_passengers = 2
  ) t
) t GROUP BY pickup_dt_datehist_1d


SELECT
  COUNT(1) AS n_rows
FROM taxi_trips
WHERE
  pickup_dt BETWEEN '2014-03-20 00:00:00' AND '2014-05-19 00:00:00' AND
  paid_tip BETWEEN 3.1336249797815183 AND 5.133624979781518 AND
  pickup_day = 2


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
      pickup_dt BETWEEN '2015-04-30 00:00:00' AND '2015-06-29 00:00:00' AND
      paid_tip BETWEEN 12.400480251628748 AND 14.400480251628748 AND
      pickup_day = 7
  ) t
) t GROUP BY pickup_time_hist_2


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
      pickup_dt BETWEEN '2014-02-14 00:00:00' AND '2014-04-15 00:00:00' AND
      paid_tip BETWEEN 12.223882127682643 AND 14.223882127682643 AND
      pickup_day = 6
  ) t
) t GROUP BY pickup_dt_datehist_1d, company


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
      pickup_dt BETWEEN '2015-08-16 00:00:00' AND '2015-10-15 00:00:00' AND
      paid_tip BETWEEN 14.333569641073911 AND 16.33356964107391 AND
      pickup_day = 7
  ) t
) t GROUP BY pickup_dt_datehist_1d

