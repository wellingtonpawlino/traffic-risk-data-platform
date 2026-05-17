with locais as (

    select distinct
        nm_logradouro,
        vl_latitude,
        vl_longitude
    from {{ ref('stg_sinistros') }}
    where nm_logradouro is not null

)

select 

    row_number() over () as id_local  -- chave artificial

   ,nm_logradouro
   ,vl_latitude
   ,vl_longitude

from locais
