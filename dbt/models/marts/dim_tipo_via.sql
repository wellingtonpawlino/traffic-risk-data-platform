with vias as (

    select distinct
        ds_tipo_via
    from {{ ref('stg_pessoas') }}
    where ds_tipo_via is not null

)

select 

    row_number() over (order by ds_tipo_via) as id_tipo_via,

    ds_tipo_via

from vias