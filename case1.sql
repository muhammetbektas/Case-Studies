/*
 * reset_points: Calculates the previous distance for each record and flags if a reset occurs (reset = 1).
 * handle_routes: Adds a segment_id to group cumulative segments by using is_reset to mark where each segment begins.
 * route_distances: Finds the maximum distance in each segment (segment_distance), and captures the start and end timestamps for each segment.
 * route_totals: Aggregates the segment_distance for each route to get the total_distance. It also calculates the total_duration as the difference
between the earliest and latest recorded times for each route.
*/


WITH reset_points AS (
    SELECT 
        route_id,
        distance,
        CAST(LEFT(recorded_at, CHARINDEX(' ', recorded_at))+''+SUBSTRING(recorded_at,CHARINDEX(' ', recorded_at),9) AS DATETIME) recorded_at,
        LAG(distance) OVER (PARTITION BY route_id ORDER BY recorded_at) AS prev_distance,
        CASE 
            WHEN distance < LAG(distance) OVER (PARTITION BY route_id ORDER BY recorded_at) 
            THEN 1 
            ELSE 0 
        END AS reset
    FROM 
        navigation_records
), 
handle_routes AS (
    SELECT 
        route_id,
        distance,
        recorded_at,
        SUM(reset) OVER (PARTITION BY route_id ORDER BY recorded_at) AS point_id
    FROM 
        reset_points
), 
route_distances AS (
    SELECT 
        route_id,
        MAX(distance) AS point2point_distance,
        MIN(recorded_at) AS stream_start,
        MAX(recorded_at) AS stream_end
    FROM handle_routes
    GROUP BY 
        route_id, point_id
), 
route_totals AS (
    SELECT 
        route_id,
        SUM(point2point_distance) AS total_distance,
        DATEDIFF(minute,MIN(stream_start),MAX(stream_end)) AS total_duration
    FROM 
        route_distances
    GROUP BY 
        route_id
)
SELECT route_id,ROUND(total_distance,2) total_distance,total_duration 'total duration (minutes)' FROM route_totals