{{ config(
    materialized='table',
    table_type='iceberg',
    s3_data_naming='schema_table_unique'
) }}

select
    origem,
    destino,
    transportadora,
    count(*) as total_envios,
    sum(case when status_entrega = 1 then 1 else 0 end) as total_atrasados,
    round(avg(atraso_min), 2) as atraso_medio_min,
    round(avg(distancia_milhas), 2) as distancia_media_milhas,
    round(avg(tempo_planejado_min), 2) as tempo_medio_planejado,
    round(
        cast(sum(case when status_entrega = 1 then 1 else 0 end) as double) /
        nullif(count(*), 0) * 100, 2
    ) as percentual_atraso

from {{ ref('int_fedex_metricas') }}
group by origem, destino, transportadora
order by total_envios desc
