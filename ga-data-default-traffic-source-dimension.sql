SELECT
    -- traffic_source.name (dimensions | name of the marketing campaign that first  acquired the user)
    traffic_source.name,
    -- traffic_source.medium (dimension | name of the medium (paid search, organic search, email, etc.) that first acquired the user)
    traffic_source.medium,
    -- traffic_source.source (dimension | name of the network that first acquired the user)
    traffic_source.source
FROM
    `ga4-data-344909.analytics_**********.events_*`
WHERE 
    -- define static and/or dynamic start and end date
    _table_suffix BETWEEN '20220210' AND format_date('%Y%m%d', date_sub(current_date(), INTERVAL 1 DAY))