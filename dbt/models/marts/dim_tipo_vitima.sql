with tipos as (

    select distinct
        ds_tipo_vitima
    from {{ ref('stg_pessoas') }}
    where ds_tipo_vitima is not null

)

select 

    row_number() over (order by ds_tipo_vitima) as id_tipo_vitima,

    ds_tipo_vitima

from tipos