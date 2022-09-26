SELECT
    --transaction id (dimensions | the transaction ID of the ecommerce transaction)
    ecommerce.transaction_id,
    --total item quantity (metric | total number of items in this event, which is the sum of items.quantity)
    SUM(ecommerce.total_item_quantity) AS total_item_quantity,
    --purchase revenue in pln (metric | purchase revenue of this event)
    SUM(ecommerce.purchase_revenue_in_usd) AS purchase_revenue_in_pln,
    --unique items (metric | the number of unique items in this event, based on item_id, item_name, item_brand)
    SUM(ecommerce.unique_items) AS unique_items
FROM 
    `ga4-data-344909.analytics_**********.events_*`
WHERE
    _table_suffix BETWEEN "20220210" AND format_date('%Y%m%d', date_sub(current_date(), INTERVAL 1 day))
    AND ecommerce.transaction_id IS NOT NULL
GROUP BY 
    transaction_id
ORDER BY 
    purchase_revenue_in_pln DESC

