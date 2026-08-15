with gravidades as (

    select distinct
        ds_gravidade_lesao
    from {{ ref('stg_pessoas') }}
    where ds_gravidade_lesao is not null

)

select 

    row_number() over (order by ds_gravidade_lesao) as id_gravidade_lesao,

    ds_gravidade_lesao

from gravidades