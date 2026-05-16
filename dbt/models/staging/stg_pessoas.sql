
SELECT
     id_sinistro::int                                            AS id_sinistro
    ,cod_ibge::int                                               AS id_cod_ibge
    ,INITCAP(TRIM(LOWER(municipio)))                             AS nm_municipio
    ,INITCAP(TRIM(LOWER(regiao_administrativa)))                 AS nm_regiao_administrativa
    ,INITCAP(TRIM(LOWER(tipo_via)))                              AS ds_tipo_via
    ,INITCAP(TRIM(LOWER(tipo_veiculo_vitima)))                   AS ds_tipo_veiculo_vitima
    ,INITCAP(TRIM(LOWER(sexo)))                                  AS ds_sexo
    ,idade::int                                                  AS nr_idade
    ,INITCAP(TRIM(LOWER(gravidade_lesao)))                       AS ds_gravidade_lesao
    ,INITCAP(TRIM(LOWER(tipo_de_vitima)))                        AS ds_tipo_vitima
    ,INITCAP(TRIM(LOWER(faixa_etaria_demografica)))              AS ds_faixa_etaria_demografica
    ,INITCAP(TRIM(LOWER(faixa_etaria_legal)))                    AS ds_faixa_etaria_legal
    ,INITCAP(TRIM(LOWER(profissao)))                             AS ds_profissao
    ,INITCAP(TRIM(LOWER(grau_de_instrucao)))                     AS ds_grau_instrucao
    ,INITCAP(TRIM(LOWER(nacionalidade)))                         AS ds_nacionalidade

    ,TO_DATE(NULLIF(data_sinistro, ''), 'DD/MM/YYYY')            AS dt_sinistro
    ,ano_sinistro::int                                           AS ano_sinistro
    ,mes_sinistro::int                                           AS mes_sinistro
    ,dia_sinistro::int                                           AS dia_sinistro

    ,TO_DATE(NULLIF(data_obito, ''), 'DD/MM/YYYY')               AS dt_obito
    ,ano_obito::int                                              AS ano_obito
    ,mes_obito::int                                              AS mes_obito
    ,dia_obito::int                                              AS dia_obito

    ,INITCAP(TRIM(LOWER(local_obito)))                           AS ds_local_obito
    ,INITCAP(TRIM(LOWER(local_via)))                             AS ds_local_via

    ,is_fatal_pessoa::boolean                                    AS fl_fatal_pessoa

FROM prep.pessoas
WHERE id_sinistro IS NOT NULL