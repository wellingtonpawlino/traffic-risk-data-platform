with tipos as (

    select distinct
        ds_tipo_sinistro_primario
    from {{ ref('stg_sinistros') }}
    where ds_tipo_sinistro_primario is not null

)

select 

    row_number() over (order by ds_tipo_sinistro_primario) as id_tipo_sinistro

   ,ds_tipo_sinistro_primario

from tipos