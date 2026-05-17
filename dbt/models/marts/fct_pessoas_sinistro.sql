select

    p.id_sinistro,
    dlp.id_local_pessoa,
    dg.id_gravidade_lesao,
    dtv.id_tipo_vitima,
    p.dt_sinistro

from {{ ref('stg_pessoas') }} p

left join {{ ref('dim_local_pessoa') }} dlp
    on p.id_cod_ibge = dlp.id_cod_ibge

left join {{ ref('dim_gravidade') }} dg
    on p.ds_gravidade_lesao = dg.ds_gravidade_lesao

left join {{ ref('dim_tipo_vitima') }} dtv
    on p.ds_tipo_vitima = dtv.ds_tipo_vitima