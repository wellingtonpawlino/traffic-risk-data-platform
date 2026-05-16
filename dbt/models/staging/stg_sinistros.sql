
SELECT
     id_sinistro::int                                           AS id_sinistro
	 ,id_veiculo::int                                            AS id_veiculo
	 ,CASE
    	WHEN marca_modelo IS NULL OR TRIM(marca_modelo) = '' OR LOWER(TRIM(marca_modelo)) IN ('nao informado', 'nao info', '-')
    	THEN 'Sem Informacoes'
    	ELSE TRIM(UPPER(marca_modelo)) END AS ds_marca_modelo
	,CASE
    	WHEN tipo_veiculo IS NULL OR TRIM(tipo_veiculo) = '' OR LOWER(TRIM(tipo_veiculo)) = 'nao disponivel'
    	THEN 'Sem Informacoes'
    	ELSE INITCAP(TRIM(LOWER(tipo_veiculo))) END AS ds_tipo_veiculo
	,TO_DATE(NULLIF(data_sinistro, ''), 'DD/MM/YYYY')               AS dt_sinistro	

	,ano_sinistro::int                                              AS ano_sinistro
    ,mes_sinistro::int                                              AS mes_sinistro
    ,dia_sinistro::int                                              AS dia_sinistro
	 FROM prep.veiculos
WHERE id_sinistro IS NOT NULL 