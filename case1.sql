/*
 * reset_points: Calculates the previous distance for each record and flags if a reset occurs (reset = 1).
 * handle_routes: Adds a point_id to group cumulative resets by using reset to mark where each reset begins.
 * route_distances: Finds the maximum distance in each reset (point2point_distance), and captures the start and end timestamps for each reset.
 * route_totals: Aggregates the point2point_distance for each route to get the total_distance. It also calculates the total_duration as the difference
between the earliest and latest recorded times for each route.
*/

/*T-SQL*/

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

|route_id|total_distance|total duration (minutes)|
|--------|--------------|------------------------|
|100,057 |376,573.06    |207                     |
|151,768 |107,633.21    |369                     |
|152,873 |72,982.36     |480                     |
|153,328 |67,300.67     |511                     |
|153,609 |69,643.58     |668                     |
|153,610 |66,695.22     |381                     |
|153,878 |35,236.42     |442                     |
|224,883 |2,193,529.45  |1,041                   |
|446,648 |63,024.18     |336                     |
|447,299 |177,439.34    |395                     |
|449,536 |35,766.57     |418                     |
|451,926 |83,064.77     |640                     |
|454,215 |74,010.41     |585                     |
|455,962 |609,449.47    |362                     |
|456,635 |182,633.75    |437                     |
|458,536 |116,118.19    |471                     |
|459,964 |23,676.74     |167                     |
|460,733 |79,105.55     |462                     |
|465,254 |336,396.74    |313                     |
|468,174 |261,792.17    |426                     |

