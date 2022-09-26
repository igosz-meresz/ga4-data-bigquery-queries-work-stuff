WITH ecommerceProducts AS (
SELECT 
    --item name
    item_name AS itemName,
    --item views
    COUNT(CASE WHEN event_name = "view_item" THEN 
            CONCAT(event_timestamp, CAST(user_pseudo_id AS STRING)) ELSE NULL END) AS itemViews,
    --add to carts
    COUNT(CASE WHEN event_name = "add_to_cart" THEN
            CONCAT(event_timestamp, CAST(user_pseudo_id AS STRING)) ELSE NULL END) AS addToCarts,
    --cart to view rate
    (CASE WHEN COUNT(CASE WHEN event_name = "view_item" THEN
        user_pseudo_id ELSE NULL END) = 0 THEN 0
            ELSE COUNT(DISTINCT CASE WHEN event_name = "add_to_cart" THEN
                user_pseudo_id ELSE NULL END) /
                COUNT(DISTINCT CASE WHEN event_name = "view_item" THEN
                    user_pseudo_id ELSE NULL END) END * 1000) AS cartToViewRate,
    --ecommerce purchases
    COUNT(CASE WHEN event_name = "purchase" THEN ecommerce.transaction_id ELSE NULL END) AS ecommercePurchases,
    --purchase to view rate
    (CASE WHEN COUNT(CASE WHEN event_name = "view_item" THEN user_pseudo_id ELSE NULL END) = 0 THEN 0
    ELSE COUNT(DISTINCT CASE WHEN event_name = "purchase" THEN user_pseudo_id ELSE NULL END) /
    COUNT(DISTINCT CASE WHEN event_name = "view_item" THEN user_pseudo_id ELSE NULL END) END * 100) AS purchaseToViewRate,
    --item purchase quantity
    SUM(CASE WHEN event_name = "purchase" THEN items.quantity ELSE NULL END) AS itemPurchaseQuantity,
    --item revenue
    SUM(item_revenue) AS itemRevenue
FROM `ga4-data-344909.analytics_**********.events_*`, UNNEST(items) AS items
--WHERE
    --_table_suffix between '20210101' and '20210131' AND
GROUP BY itemName)

SELECT 
    itemName,
    itemViews,
    addToCarts,
    cartToViewRate,
    ecommercePurchases,
    purchaseToViewRate,
    itemPurchaseQuantity,
    itemRevenue
FROM 
    ecommerceProducts
WHERE 
    itemViews > 0 OR itemRevenue > 0
ORDER BY
    ecommercePurchases DESC