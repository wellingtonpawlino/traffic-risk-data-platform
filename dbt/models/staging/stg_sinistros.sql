select 
	id_sinistro::int AS id_sinistro
	,CASE
    	WHEN tipo_registro IS NULL  OR TRIM(tipo_registro) = '' OR LOWER(TRIM(tipo_registro)) = 'nao disponivel'
    	THEN 'Sem Informacoes'
    	ELSE INITCAP(TRIM(LOWER(tipo_registro))) END AS ds_tipo_registro
   ,TO_DATE(NULLIF(data_sinistro, ''), 'DD/MM/YYYY')               AS dt_sinistro
   ,ano_sinistro::int                                              AS ano_sinistro
   ,mes_sinistro::int                                              AS mes_sinistro
   ,dia_sinistro::int                                              AS dia_sinistro
   ,NULLIF(hora_sinistro, '')::time AS hr_sinistro
   ,CASE
    	WHEN dia_da_semana IS NULL OR TRIM(dia_da_semana) = '' OR LOWER(TRIM(dia_da_semana)) = 'nao disponivel'
    	THEN 'Sem Informacoes'
		ELSE INITCAP(TRIM(LOWER(dia_da_semana))) END AS ds_dia_semana
   ,CASE
    	WHEN turno IS NULL  OR TRIM(turno) = '' OR LOWER(TRIM(turno)) = 'nao disponivel' 
		THEN 'Sem Informacoes'
    	ELSE INITCAP(TRIM(LOWER(turno))) END AS ds_turno
  ,INITCAP(TRIM(LOWER(logradouro)))                             AS nm_logradouro
  ,NULLIF(REPLACE(latitude, ',', '.'), '')::numeric AS vl_latitude
  ,NULLIF(REPLACE(longitude, ',', '.'), '')::numeric AS vl_longitude 
  ,CASE
   	WHEN tp_sinistro_primario IS NULL OR TRIM(tp_sinistro_primario) = '' OR LOWER(TRIM(tp_sinistro_primario)) = 'nao disponivel'
    THEN 'Sem Informacoes'
    ELSE INITCAP(TRIM(LOWER(tp_sinistro_primario))) END AS ds_tipo_sinistro_primario
 ,CASE
    WHEN qtd_pedestre IS NULL OR qtd_pedestre = 0
    THEN NULL
    ELSE qtd_pedestre::int END AS nr_qtd_pedestre

 ,CASE
    WHEN qtd_bicicleta IS NULL OR qtd_bicicleta = 0
    THEN NULL
    ELSE qtd_bicicleta::int END AS nr_qtd_bicicleta

 ,CASE
    WHEN qtd_motocicleta IS NULL OR qtd_motocicleta = 0
    THEN NULL
    ELSE qtd_motocicleta::int END AS nr_qtd_motocicleta
	
,CASE
    WHEN qtd_automovel IS NULL OR qtd_automovel = 0
    THEN NULL
    ELSE qtd_automovel::int END AS nr_qtd_automovel

,CASE
    WHEN qtd_onibus IS NULL OR qtd_onibus = 0
    THEN NULL
    ELSE qtd_onibus::int END AS nr_qtd_onibus
,CASE
    WHEN qtd_caminhao  IS NULL OR qtd_caminhao = 0
    THEN NULL
    ELSE qtd_caminhao::int END AS nr_qtd_caminhao

,CASE
    WHEN qtd_veic_outros  IS NULL OR qtd_veic_outros = 0
    THEN NULL
    ELSE qtd_veic_outros::int END AS nr_qtd_veic_outros

,CASE
    WHEN qtd_veic_nao_disponivel  IS NULL OR qtd_veic_nao_disponivel = 0
    THEN NULL
    ELSE qtd_veic_nao_disponivel::int END AS nr_qtd_veic_nao_disponivel


,CASE
    WHEN qtd_gravidade_fatal  IS NULL OR qtd_gravidade_fatal = 0
    THEN NULL
    ELSE qtd_gravidade_fatal::int END AS nr_qtd_gravidade_fatal
	
,CASE
    WHEN qtd_gravidade_grave IS NULL OR qtd_gravidade_grave = 0
    THEN NULL
    ELSE qtd_gravidade_grave::int END AS nr_qtd_gravidade_grave
	
,CASE
    WHEN qtd_gravidade_leve IS NULL OR qtd_gravidade_leve = 0
    THEN NULL
    ELSE qtd_gravidade_leve::int END AS nr_qtd_gravidade_leve
	
,CASE
    WHEN qtd_gravidade_ileso IS NULL OR qtd_gravidade_ileso = 0
    THEN NULL
    ELSE qtd_gravidade_ileso::int END AS nr_qtd_gravidade_ileso
	
,CASE
    WHEN qtd_gravidade_nao_disponivel IS NULL OR qtd_gravidade_nao_disponivel = 0
    THEN NULL
    ELSE qtd_gravidade_nao_disponivel::int END AS nr_qtd_gravidade_nao_disponivel

, 
CASE 
    WHEN tp_sinistro_atropelamento = 'S' THEN TRUE
    ELSE FALSE
END AS fl_sinistro_atropelamento

, 
CASE 
    WHEN tp_sinistro_colisao_frontal = 'S' THEN TRUE
    ELSE FALSE
END AS fl_sinistro_colisao_frontal

from prep.sinistros 