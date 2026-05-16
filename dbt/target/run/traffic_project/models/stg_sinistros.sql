
  create view "analytics"."public"."stg_sinistros__dbt_tmp"
    
    
  as (
    SELECT
    id_sinistro,
    latitude,
    longitude,
    data_sinistro
FROM  analytics.prep.sinistros_enriched
WHERE latitude IS NOT NULL AND longitude IS NOT NULL
  );