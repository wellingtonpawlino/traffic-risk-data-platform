
SELECT
     id_sinistro::int                                            AS id_sinistro
    ,cod_ibge::int                                               AS id_cod_ibge
    ,INITCAP(TRIM(LOWER(municipio)))                             AS nm_municipio
    ,INITCAP(TRIM(LOWER(regiao_administrativa)))                 AS nm_regiao_administrativa
    ,CASE
    	WHEN tipo_via IS NULL OR TRIM(tipo_via) = '' 
		THEN 'Sem Informacoes'
    	ELSE INITCAP(TRIM(LOWER(tipo_via))) END AS ds_tipo_via

	,CASE 
     	WHEN tipo_veiculo_vitima IS NULL OR TRIM(tipo_veiculo_vitima) = '' THEN 'Sem Informacoes'
     	ELSE INITCAP(TRIM(LOWER(tipo_veiculo_vitima))) END AS ds_tipo_veiculo_vitima

    
	,CASE
    	WHEN sexo IS NULL  OR TRIM(sexo) = '' OR LOWER(TRIM(sexo)) = 'nao disponivel' 
		THEN 'Sem Informacoes'
    	ELSE INITCAP(TRIM(LOWER(sexo))) END AS ds_sexo

    ,idade::int                                                  AS nr_idade
	
	,CASE
    	WHEN faixa_etaria_legal IS NULL OR TRIM(faixa_etaria_legal) = '' 
    	THEN 'Sem Informacoes'
    	ELSE INITCAP(TRIM(LOWER(faixa_etaria_legal))) END as ds_faixa_etaria_legal
 
    ,CASE
    	WHEN gravidade_lesao IS NULL OR TRIM(gravidade_lesao) = '' OR LOWER(TRIM(gravidade_lesao)) = 'nao disponivel'
		THEN 'Sem Informacoes'
    	ELSE INITCAP(TRIM(LOWER(gravidade_lesao))) END AS ds_gravidade_lesao
	,CASE
		WHEN tipo_de_vitima IS NULL OR TRIM(tipo_de_vitima) = '' OR LOWER(TRIM(tipo_de_vitima)) = 'nao disponivel'
        THEN 'Sem Informacoes' 
		ELSE INITCAP(TRIM(LOWER(tipo_de_vitima))) END AS ds_tipo_vitima
	,CASE
		WHEN profissao IS NULL OR TRIM(profissao) = '' OR LOWER(TRIM(profissao)) = 'não informado'
		THEN 'Sem Informacoes' 
        ELSE INITCAP(
            TRIM(
                LOWER(
                    REGEXP_REPLACE(profissao, '\(.*?\)', '')))) END AS ds_profissao
	,CASE 
    	WHEN grau_de_instrucao IS NULL OR TRIM(grau_de_instrucao) = '' THEN 'Sem Informacoes'
    	ELSE INITCAP(TRIM(LOWER(grau_de_instrucao))) END              AS ds_grau_instrucao
	,CASE
    	WHEN nacionalidade IS NULL OR TRIM(nacionalidade) = ''  OR LOWER(TRIM(nacionalidade)) = 'nao disponivel'
        THEN 'Sem Informacoes'

    	WHEN LOWER(TRIM(nacionalidade)) IN ('brasileira', 'brasileiro', 'bras') 
        THEN 'Brasileiro'
    	ELSE INITCAP(TRIM(LOWER(nacionalidade))) END AS ds_nacionalidade


    ,TO_DATE(NULLIF(data_sinistro, ''), 'DD/MM/YYYY')            AS dt_sinistro
    ,ano_sinistro::int                                           AS ano_sinistro
    ,mes_sinistro::int                                           AS mes_sinistro
    ,dia_sinistro::int                                           AS dia_sinistro

    ,TO_DATE(NULLIF(data_obito, ''), 'DD/MM/YYYY')               AS dt_obito
    ,ano_obito::int                                              AS ano_obito
    ,mes_obito::int                                              AS mes_obito
    ,dia_obito::int                                              AS dia_obito


	,CASE
    	WHEN local_obito IS NULL OR TRIM(local_obito) = '' 
		THEN 'Sem Informacoes'
    	ELSE INITCAP(TRIM(LOWER(local_obito))) END AS ds_local_obito

	,CASE
		WHEN local_via IS NULL OR TRIM(local_via) = ''  OR LOWER(TRIM(local_via)) = 'nao disponivel'
		THEN 'Sem Informacoes' 
    	ELSE INITCAP(TRIM(LOWER(local_via)))END AS ds_local_via
		
    ,(gravidade_lesao = 'FATAL')                                 AS fl_fatal_pessoa

FROM prep.pessoas
WHERE id_sinistro IS NOT NULL