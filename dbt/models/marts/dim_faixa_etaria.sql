select
    row_number() over (order by ds_faixa_etaria) as id_faixa_etaria,
    ds_faixa_etaria
from (
    values
        ('0-17'),
        ('18-24'),
        ('25-34'),
        ('35-44'),
        ('45-54'),
        ('55-64'),
        ('65-79'),
        ('80+'),
        ('Sem Informacoes')
) as t(ds_faixa_etaria)