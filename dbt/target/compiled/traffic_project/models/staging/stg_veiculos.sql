SELECT
    id_sinistro,
    id_veiculo,
    marca_modelo,
    ano_fab,
    ano_modelo,
    cor_veiculo,
    tipo_veiculo,
    data_sinistro
FROM prep.veiculos
WHERE id_sinistro IS NOT NULL