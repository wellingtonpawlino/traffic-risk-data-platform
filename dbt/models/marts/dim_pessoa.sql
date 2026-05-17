with pessoas as (

    select distinct
         ds_sexo
        ,ds_profissao
        ,ds_grau_instrucao
        ,ds_nacionalidade
    from {{ ref('stg_pessoas') }}

)

select 

    row_number() over () as id_pessoa,

         ds_sexo
        ,ds_profissao
        ,ds_grau_instrucao
        ,ds_nacionalidade

from pessoas