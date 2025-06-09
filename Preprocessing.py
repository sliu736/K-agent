# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW kagent_custom.bright.gmaps_business_vw AS
# MAGIC SELECT *
# MAGIC FROM kagent.bright_initiative.google_maps_businesses_csv
# MAGIC WHERE
# MAGIC   -- Extract 5-digit ZIP code at the end of the address using regex
# MAGIC   CAST(REGEXP_EXTRACT(address, '\\b(\\d{5})\\b$') AS INT) BETWEEN 94101 AND 94188;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW kagent_custom.bright.gmaps_business_vw AS
# MAGIC SELECT *,
# MAGIC CASE 
# MAGIC     WHEN RAND() < 0.5 THEN 'Suhuiliu736@gmail.com'
# MAGIC     ELSE 'harikrishnamsds@gmail.com'
# MAGIC   END AS supervisor_email
# MAGIC FROM kagent.bright_initiative.google_maps_businesses_csv
# MAGIC WHERE
# MAGIC   TRY_CAST(REGEXP_EXTRACT(address, '\\b(\\d{5})\\b$') AS INT) BETWEEN 94101 AND 94188
# MAGIC   AND 
# MAGIC   country = 'United States'
# MAGIC   OR
# MAGIC   category in ('Dog park','Theme park','Park','City park','Spa','Gym','Health spa','Facial spa','Medical spa','Massage spa','Day spa','Wholesale bakery','Wedding bakery','Chinese bakery','Movie theater','Restaurant')
# MAGIC   OR  category like "%spa%" or category like "%bakery%" or category like '%Medical%' --or category like '%Restaurant%'
# MAGIC   and category  not like '%parking%' and category !='Movie rental kiosk'
# MAGIC     

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW kagent_custom.bright.hotels_vw AS
# MAGIC SELECT
# MAGIC   hotel_id,
# MAGIC   url,
# MAGIC   title,
# MAGIC   location,
# MAGIC   country,
# MAGIC   city,
# MAGIC   metro_railway_access,
# MAGIC   number_of_reviews,
# MAGIC   review_score,
# MAGIC   description,
# MAGIC   property_highlights,
# MAGIC   availability,
# MAGIC   top_reviews,
# MAGIC   managed_by,
# MAGIC   manager_score,
# MAGIC   property_information,
# MAGIC   manager_language_spoken,
# MAGIC   property_surroundings,
# MAGIC   house_rules,
# MAGIC   fine_print,
# MAGIC   coordinates,
# MAGIC   
# MAGIC   -- Add random supervisor_email
# MAGIC   CASE 
# MAGIC     WHEN RAND() < 0.5 THEN 'Suhuiliu736@gmail.com'
# MAGIC     ELSE 'harikrishnamsds@gmail.com'
# MAGIC   END AS supervisor_email,
# MAGIC
# MAGIC   -- Normalize review_score from 1–10 to 1–5
# MAGIC   ROUND(review_score / 2.0, 1) AS normalized_review_score
# MAGIC
# MAGIC FROM kagent.bright_initiative.booking_hotel_listings_csv
# MAGIC WHERE city = 'San Francisco';
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW kagent_custom.bright.airbnb_vw AS
# MAGIC SELECT
# MAGIC   name,
# MAGIC   price,
# MAGIC   description,
# MAGIC   availability,
# MAGIC   reviews,
# MAGIC   ratings,
# MAGIC   seller_info,
# MAGIC   location,
# MAGIC   lat,
# MAGIC   long,
# MAGIC   guests,
# MAGIC   pets_allowed,
# MAGIC   description_items,
# MAGIC   category_rating,
# MAGIC   house_rules,
# MAGIC   details,
# MAGIC   arrangement_details,
# MAGIC   amenities,
# MAGIC
# MAGIC   -- Add random supervisor_email
# MAGIC   CASE 
# MAGIC     WHEN RAND() < 0.5 THEN 'Suhuiliu736@gmail.com'
# MAGIC     ELSE 'harikrishnamsds@gmail.com'
# MAGIC   END AS supervisor_email
# MAGIC
# MAGIC FROM kagent.bright_initiative.airbnb_properties_information_csv
# MAGIC WHERE LOWER(location) LIKE '%san francisco%';
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM kagent_custom.bright.airbnb_vw  LIMIT 5

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW kagent_custom.nimble.yelp_vw AS
# MAGIC SELECT
# MAGIC   business_name,
# MAGIC   business_reviews,
# MAGIC   business_rating,
# MAGIC   business_categories,
# MAGIC   business_review_snippet,
# MAGIC   is_organic,
# MAGIC   date,
# MAGIC
# MAGIC   -- Random supervisor_email
# MAGIC   CASE 
# MAGIC     WHEN RAND() < 0.5 THEN 'Suhuiliu736@gmail.com'
# MAGIC     ELSE 'harikrishnamsds@gmail.com'
# MAGIC   END AS supervisor_email,
# MAGIC
# MAGIC   -- Converted review count to integer
# MAGIC   TRY_CAST(
# MAGIC     CASE 
# MAGIC       WHEN LOWER(business_reviews) LIKE '%k' THEN 
# MAGIC         CAST(REGEXP_REPLACE(LOWER(business_reviews), 'k', '') AS DOUBLE) * 1000
# MAGIC       ELSE 
# MAGIC         CAST(business_reviews AS DOUBLE)
# MAGIC     END AS INT
# MAGIC   ) AS business_reviewsA_int
# MAGIC
# MAGIC FROM kagent.nimble.dbx_yelp_serp_daily;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW kagent_custom.bright.locations_vw AS
# MAGIC
# MAGIC -- 1. From gmaps_business
# MAGIC SELECT
# MAGIC   name,
# MAGIC   description,
# MAGIC   address AS location,
# MAGIC   country,
# MAGIC   'San Fransisco' AS city,
# MAGIC   CAST(lat AS DOUBLE) AS latitude,
# MAGIC   CAST(lon AS DOUBLE) AS longitude,
# MAGIC   NULL AS price,
# MAGIC   TRY_CAST(reviews_count AS INT) AS reviews_count,
# MAGIC   TRY_CAST(rating AS DOUBLE) AS rating,
# MAGIC   services_provided AS location_amenities,
# MAGIC   NULL AS availability,
# MAGIC   NULL AS house_rules,
# MAGIC   NULL AS manager_info,
# MAGIC   supervisor_email,
# MAGIC   'gmaps_business' AS source,
# MAGIC   category,
# MAGIC   open_hours
# MAGIC FROM kagent_custom.bright.gmaps_business_vw
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC -- 2. From hotels
# MAGIC SELECT
# MAGIC   title AS name,
# MAGIC   description,
# MAGIC   location,
# MAGIC   country,
# MAGIC   city,
# MAGIC   TRY_CAST(SPLIT(coordinates, ',')[0] AS DOUBLE) AS latitude,
# MAGIC   TRY_CAST(SPLIT(coordinates, ',')[1] AS DOUBLE) AS longitude,
# MAGIC   NULL AS price,
# MAGIC   number_of_reviews AS reviews_count,
# MAGIC   review_score AS rating,
# MAGIC   property_highlights AS location_amenities,
# MAGIC   availability,
# MAGIC   house_rules,
# MAGIC   CONCAT(managed_by, ' (Score: ', manager_score, ')') AS manager_info,
# MAGIC   supervisor_email,
# MAGIC   'hotel' AS source,
# MAGIC   'Hotels' as category,
# MAGIC   null as open_hours
# MAGIC FROM kagent_custom.bright.hotels_vw
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC -- 3. From airbnb
# MAGIC SELECT
# MAGIC   name,
# MAGIC   description,
# MAGIC   location,
# MAGIC   NULL AS country,
# MAGIC   NULL AS city,
# MAGIC   lat AS latitude,
# MAGIC   long AS longitude,
# MAGIC   price,
# MAGIC   TRY_CAST(reviews AS INT) AS reviews_count,
# MAGIC   ratings AS rating,
# MAGIC   amenities as location_amenities,
# MAGIC   availability,
# MAGIC   house_rules,
# MAGIC   seller_info AS manager_info,
# MAGIC   supervisor_email,
# MAGIC   'airbnb' AS source,
# MAGIC   'airbnb' as category,
# MAGIC   null as open_hours
# MAGIC FROM kagent_custom.bright.airbnb_vw;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from kagent_custom.bright.locations_vw limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace view kagent_custom.nimble.search_daily_vw
# MAGIC as 
# MAGIC select latitude,longitude,amenities,accessibility  from kagent.nimble.dbx_google_maps_search_daily

# COMMAND ----------

locations_df = spark.table("kagent_custom.bright.locations_vw")
search_daily_df = spark.table("kagent_custom.nimble.search_daily_vw")

joined_df = locations_df.join(search_daily_df, on=["latitude", "longitude"])

display(joined_df)

# COMMAND ----------

joined_df.write.format("delta").mode("overwrite").saveAsTable("kagent_custom.location_data.locations_accesability")