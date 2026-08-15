with locais as (

    select distinct
        id_cod_ibge,
        nm_municipio
    from {{ ref('stg_pessoas') }}

)

select 

    row_number() over (order by id_cod_ibge) as id_local_pessoa,

    id_cod_ibge,
    nm_municipio

from locais