with datas as (

    select distinct dt_sinistro as dt
    from {{ ref('stg_sinistros') }}
    where dt_sinistro is not null

)

select 

    dt

   ,EXTRACT(YEAR FROM dt) as ano
   ,EXTRACT(MONTH FROM dt) as mes
   ,EXTRACT(DAY FROM dt) as dia

   ,INITCAP(TRIM(TO_CHAR(dt, 'Day'))) as ds_dia_semana
   ,INITCAP(TRIM(TO_CHAR(dt, 'Month'))) as ds_mes

   ,CASE 
        WHEN EXTRACT(MONTH FROM dt) IN (1,2,3) THEN 'Q1'
        WHEN EXTRACT(MONTH FROM dt) IN (4,5,6) THEN 'Q2'
        WHEN EXTRACT(MONTH FROM dt) IN (7,8,9) THEN 'Q3'
        WHEN EXTRACT(MONTH FROM dt) IN (10,11,12) THEN 'Q4'
   END as ds_trimestre

from datas