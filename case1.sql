/*
 * reset_points: Calculates the previous distance for each record and flags if a reset occurs (reset = 1).
 * handle_routes: Adds a point_id to group cumulative resets by using reset to mark where each reset begins.
 * route_distances: Finds the maximum distance in each reset (point2point_distance), and captures the start and end timestamps for each reset.
 * route_totals: Aggregates the point2point_distance for each route to get the total_distance. It also calculates the total_duration as the difference
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

|route_id|total_distance|total duration (minutes)|
|--------|--------------|------------------------|
|100057|376573.06|207|
|151768|107633.21|369|
|152873|72982.36|480|
|153328|67300.67|511|
|153609|69643.58|668|
|153610|66695.22|381|
|153878|35236.42|442|
|224883|2193529.45|1041|
|446648|63024.18|336|
|447299|177439.34|395|
|449536|35766.57|418|
|451926|83064.77|640|
|454215|74010.41|585|
|455962|609449.47|362|
|456635|182633.75|437|
|458536|116118.19|471|
|459964|23676.74|167|
|460733|79105.55|462|
|465254|336396.74|313|
|468174|261792.17|426|

