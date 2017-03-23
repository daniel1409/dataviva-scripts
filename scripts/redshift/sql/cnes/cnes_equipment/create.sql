DROP TABLE IF EXISTS cnes_equipment CASCADE;

CREATE TABLE cnes_equipment
(
    year                             integer,
    region                           varchar(1) encode lzo,
    mesoregion                       varchar(4) encode lzo,
    microregion                      varchar(5) encode lzo,
    state                            varchar(2) encode lzo,
    municipality                     varchar(7) encode lzo,
    cnes                             varchar(7) encode lzo,
    dependency_level                 varchar(1) encode lzo,
    unit_type                        varchar(2) encode lzo,
    equipment_type                   varchar(1) encode lzo,
    equipment_code                   varchar(2) encode lzo,
    equipment_quantity               integer encode lzo,
    equipment_quantity_in_use        integer encode lzo,
    sus_availability_indicator       varchar(1) encode lzo,
    health_region                    char(5) encode lzo
    
) sortkey(year,region,mesoregion,microregion,state,municipality,cnes,dependency_level,unit_type,equipment_type,equipment_code,sus_availability_indicator,health_region);