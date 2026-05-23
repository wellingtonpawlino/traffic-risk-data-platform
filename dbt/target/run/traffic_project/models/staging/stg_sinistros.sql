
  create view "analytics"."staging"."stg_sinistros__dbt_tmp"
    
    
  as (
    SELECT 
	id_sinistro::int AS id_sinistro
	,CASE
    	WHEN tipo_registro IS NULL  OR TRIM(tipo_registro) = '' OR LOWER(TRIM(tipo_registro)) = 'nao disponivel'
    	THEN 'Sem Informacoes'
    	ELSE INITCAP(TRIM(LOWER(tipo_registro))) 
		END AS ds_tipo_registro
   ,TO_DATE(NULLIF(data_sinistro, ''), 'DD/MM/YYYY')               AS dt_sinistro
   ,ano_sinistro::int                                              AS ano_sinistro
   ,mes_sinistro::int                                              AS mes_sinistro
   ,dia_sinistro::int                                              AS dia_sinistro
   ,NULLIF(hora_sinistro, '')::time AS hr_sinistro
   
   ,CASE
    	WHEN logradouro IS NULL OR TRIM(logradouro) = '' OR LOWER(TRIM(logradouro)) = 'nao disponivel'
    	THEN 'Sem Informacoes'
    	ELSE INITCAP(TRIM(LOWER(logradouro)))
		END AS nm_logradouro
   ,NULLIF(REPLACE(latitude, ',', '.'), '')::numeric  AS vl_latitude
   ,NULLIF(REPLACE(longitude, ',', '.'), '')::numeric AS vl_longitude 
   ,CASE
   		WHEN tp_sinistro_primario IS NULL OR TRIM(tp_sinistro_primario) = '' OR LOWER(TRIM(tp_sinistro_primario)) = 'nao disponivel'
    	THEN 'Sem Informacoes'
    	ELSE INITCAP(TRIM(LOWER(tp_sinistro_primario))) 
		END AS ds_tipo_sinistro_primario
   ,NULLIF(qtd_pedestre, 0)::int AS nr_qtd_pedestre
   ,NULLIF(qtd_bicicleta, 0)::int AS nr_qtd_bicicleta
   ,NULLIF(qtd_motocicleta, 0)::int AS nr_qtd_motocicleta
   ,NULLIF(qtd_automovel, 0)::int AS nr_qtd_automovel
   ,NULLIF(qtd_onibus, 0)::int AS nr_qtd_onibus
   ,NULLIF(qtd_caminhao, 0)::int AS nr_qtd_caminhao
   ,NULLIF(qtd_veic_outros, 0)::int AS nr_qtd_veic_outros
   ,NULLIF(qtd_veic_nao_disponivel, 0)::int AS nr_qtd_veic_nao_disponivel
   ,NULLIF(qtd_gravidade_fatal, 0)::int AS nr_qtd_gravidade_fatal
   ,NULLIF(qtd_gravidade_grave, 0)::int AS nr_qtd_gravidade_grave
   ,NULLIF(qtd_gravidade_leve, 0)::int AS nr_qtd_gravidade_leve
   ,NULLIF(qtd_gravidade_ileso, 0)::int AS nr_qtd_gravidade_ileso
   ,NULLIF(qtd_gravidade_nao_disponivel, 0)::int AS nr_qtd_gravidade_nao_disponivel
   ,CASE 
    	WHEN tp_sinistro_atropelamento::text = 'S' THEN TRUE
    	ELSE FALSE 
		END AS fl_sinistro_atropelamento
   ,CASE 
    	WHEN tp_sinistro_colisao_frontal::text = 'S' THEN TRUE
    	ELSE FALSE 
		END AS fl_sinistro_colisao_frontal 
  ,CASE 
    	WHEN tp_sinistro_colisao_traseira::text = 'S' THEN TRUE
    	ELSE FALSE 
		END AS fl_sinistro_colisao_traseira
  ,CASE 
    	WHEN tp_sinistro_colisao_lateral::text = 'S' THEN TRUE
    	ELSE FALSE 
		END AS fl_sinistro_colisao_lateral
  ,CASE 
    	WHEN tp_sinistro_colisao_transversal::text = 'S' THEN TRUE
   	 	ELSE FALSE
		END AS fl_sinistro_colisao_transversal
  ,CASE 
    	WHEN tp_sinistro_colisao_outros::text = 'S' THEN TRUE
    	ELSE FALSE
		END AS fl_sinistro_colisao_outros
  ,CASE 
    	WHEN tp_sinistro_choque::text = 'S' THEN TRUE
    	ELSE FALSE
		END AS fl_sinistro_choque
  ,CASE 
    	WHEN tp_sinistro_capotamento::text = 'S' THEN TRUE
    	ELSE FALSE
		END AS fl_sinistro_capotamento 
  ,CASE 
    	WHEN tp_sinistro_engavetamento::text = 'S' THEN TRUE
    	ELSE FALSE
		END AS fl_sinistro_engavetamento
  ,CASE 
    	WHEN tp_sinistro_tombamento::text = 'S' THEN TRUE
    	ELSE FALSE
		END AS fl_sinistro_tombamento 
  ,CASE 
    	WHEN tp_sinistro_outros::text = 'S' THEN TRUE
    	ELSE FALSE
		END AS fl_sinistro_outros
  ,CASE 
    	WHEN tp_sinistro_nao_disponivel::text = 'S' THEN TRUE
    	ELSE FALSE
		END AS fl_sinistro_nao_disponivel

  ,is_fatal_sinistro

FROM prep.sinistros 
WHERE id_sinistro IS NOT NULL
  );