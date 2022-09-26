SELECT
    -- item id (dimension | the id of the item)
    items.item_id,
    -- item name (dimension | the name of the item)
    items.item_name,
    -- item brand (dimension | the brand of the item)
    items.item_brand,
    -- item variant (dimension | the variant of the item)
    items.item_variant,
    -- item category (dimension | the category of the item)
    items.item_category,
    -- item category 2 (dimension | the subcategory of the item)
    items.item_category2,
    -- item category 3 (dimension | the subcategory of the item)
    items.item_category3,
    -- item category 4 (dimension | the subcategory of the item)
    items.item_category4,
    -- item category 5 (dimension | the subcategory of the item)
    items.item_category5,
    -- price in pln (metric | the price of the item)
    items.price,
    -- quantity (metric | the revenue of the item)
    items.quantity,
    -- coupon (dimension | coupon code applied to this item)
    items.coupon,
    -- affiliation (dimension | a product affiliation to designate a supplying company or BM store location)
    items.affiliation,
    -- location id (dimension | the location associated with the item)
    items.location_id,
    -- item list name (dimension | the name of the list in wchich item was presented to the user)
    items.item_list_name,
    -- item list index (dimension | the position of the item in a list)
    items.item_list_index,
    -- promotion id (dimension | the id of a product promotion)
    items.promotion_id,
    -- promotion name (dimension | the name of a product promotion)
    items.promotion_name
FROM 
    `ga4-data-344909.analytics_**********.events_*`,
    UNNEST(items) AS items
WHERE
    _table_suffix BETWEEN '20220210' AND FORMAT_DATE('%Y%m%d', date_sub(current_date(), INTERVAL 1 DAY))

    
    
    