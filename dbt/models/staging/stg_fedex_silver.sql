{{ config(materialized='view') }}

-- Staging from EMR Silver output (production mode)
-- Switch to this model when EMR pipeline is active
select
    "Year" as ano,
    "Month" as mes,
    DayofMonth as dia,
    DayOfWeek as dia_semana,
    Actual_Shipment_Time as horario_real_saida,
    Planned_Shipment_Time as horario_planejado_saida,
    Planned_Delivery_Time as horario_planejado_entrega,
    Carrier_Name as transportadora,
    Carrier_Num as numero_viagem,
    Planned_TimeofTravel as tempo_planejado_min,
    Shipment_Delay as atraso_min,
    Source as origem,
    Destination as destino,
    Distance as distancia_milhas,
    Delivery_Status as status_entrega,
    Travel_Time_Real as tempo_real_viagem,
    Route_ID as rota_id,
    Delay_Category as categoria_atraso
from {{ source('landing', 'fedex_silver') }}
