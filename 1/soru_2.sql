WITH t AS (
  SELECT trp.start_station_id, inf.short_name as short_name,trp.end_station_id, info.short_name as short_name_1, 
  AVG (CASE WHEN trp.member_gender = 'Male' THEN trp.duration_sec END)  AS ort_erkek_tur ,
  AVG (CASE WHEN trp.member_gender = 'Female' THEN trp.duration_sec END) AS ort_kadin_tur
  FROM bigquery-public-data.san_francisco_bikeshare.bikeshare_trips AS trp
  JOIN bigquery-public-data.san_francisco_bikeshare.bikeshare_station_info AS inf
  ON trp.start_station_id = CAST(inf.station_id AS INT)
  JOIN bigquery-public-data.san_francisco_bikeshare.bikeshare_station_info AS info
  ON trp.end_station_id = CAST (info.station_id AS INT)
  WHERE trp.member_gender IN('Male' , 'Female')
  GROUP BY start_station_id, short_name ,end_station_id ,short_name_1
)
SELECT * FROM t
WHERE ort_erkek_tur IS NOT NULL
AND ort_kadin_tur IS NOT NULL





