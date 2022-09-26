WITH events AS (
    SELECT
        event_name,
        (SELECT value.string_value 
            FROM UNNEST(event_params) 
            WHERE key = 'page_title') AS page_title,
-- calculating event count        
        SUM((SELECT COUNT(value.string_value) 
                FROM UNNEST(event_params) 
                WHERE key = 'page_title')) AS event_count,
        COUNT(DISTINCT user_pseudo_id) AS user,
-- calculating event count per user        
        COUNT(DISTINCT CASE WHEN event_name = 'page_view' 
                THEN CONCAT(user_pseudo_id, CAST(event_timestamp AS STRING)) END) / 
                COUNT(DISTINCT user_pseudo_id) AS event_count_per_user,
        SUM(ecommerce.purchase_revenue) AS total_revenue
    FROM
        `ga4-data-344909.analytics_**********.events_*`
    WHERE
        --_table_suffix between '20210101' and '20210131'
        -- change event_name to select another event AND
        event_name = 'page_view'
    GROUP BY 
        event_name, 
        page_title
    ORDER BY event_count DESC)
SELECT 
    event_name
    , page_title
    , event_count
    , user
    , ROUND(event_count_per_user, 2) AS event_count_per_user
    , total_revenue
FROM events
ORDER BY event_count_per_user DESC