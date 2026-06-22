{{ config(materialized='view') }}

select
    ano,
    mes,
    dia,
    dia_semana,
    transportadora,
    origem,
    destino,
    distancia_milhas,
    tempo_planejado_min,
    atraso_min,
    status_entrega,

    case
        when atraso_min > 0 then 'atrasado'
        when atraso_min < 0 then 'adiantado'
        else 'no_prazo'
    end as classificacao_atraso,

    round(distancia_milhas / nullif(cast(tempo_planejado_min as double), 0) * 60, 2) as velocidade_media_mph,

    date(cast(ano as varchar) || '-' ||
        lpad(cast(mes as varchar), 2, '0') || '-' ||
        lpad(cast(dia as varchar), 2, '0')) as data_envio

from {{ ref('stg_fedex_shipments') }}
