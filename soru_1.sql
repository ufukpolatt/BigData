SELECT r.name, SUM(s.capacity) AS total_capacity
FROM bigquery-public-data.san_francisco_bikeshare.bikeshare_regions AS r
LEFT JOIN bigquery-public-data.san_francisco_bikeshare.bikeshare_station_info AS s ON r.region_id = s.region_id
GROUP BY r.name 
HAVING SUM(s.capacity) < 5000
