{{ config(materialized='view') }}

-- Staging from seed (for testing without EMR)
select
    "year" as ano,
    "month" as mes,
    dayofmonth as dia,
    dayofweek as dia_semana,
    actual_shipment_time as horario_real_saida,
    planned_shipment_time as horario_planejado_saida,
    planned_delivery_time as horario_planejado_entrega,
    carrier_name as transportadora,
    carrier_num as numero_viagem,
    try_cast(planned_timeoftravel as integer) as tempo_planejado_min,
    try_cast(shipment_delay as integer) as atraso_min,
    source as origem,
    destination as destino,
    try_cast(distance as double) as distancia_milhas,
    try_cast(delivery_status as integer) as status_entrega
from {{ ref('fedex_shipments') }}
where "year" is not null
