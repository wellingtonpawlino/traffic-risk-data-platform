with sinistros as (

    select * 
    from {{ ref('stg_sinistros') }}

)

select 

    id_sinistro

    -- métricas
   ,nr_qtd_pedestre
   ,nr_qtd_bicicleta
   ,nr_qtd_motocicleta
   ,nr_qtd_automovel
   ,nr_qtd_onibus
   ,nr_qtd_caminhao
   ,nr_qtd_gravidade_fatal
   ,nr_qtd_gravidade_grave
   ,nr_qtd_gravidade_leve

    --flags
   ,fl_sinistro_atropelamento
   ,fl_sinistro_colisao_frontal
   ,fl_sinistro_colisao_traseira
   ,fl_sinistro_colisao_lateral
   ,fl_sinistro_capotamento


   ,dt_sinistro

   ,INITCAP(TRIM(TO_CHAR(dt_sinistro, 'Day'))) AS ds_dia_semana

   ,CASE 
        WHEN hr_sinistro BETWEEN '00:00:00' AND '05:59:59' THEN 'Madrugada'
        WHEN hr_sinistro BETWEEN '06:00:00' AND '11:59:59' THEN 'Manha'
        WHEN hr_sinistro BETWEEN '12:00:00' AND '17:59:59' THEN 'Tarde'
        WHEN hr_sinistro BETWEEN '18:00:00' AND '23:59:59' THEN 'Noite'
        ELSE 'Sem Informacoes'
    END AS ds_turno

from sinistros