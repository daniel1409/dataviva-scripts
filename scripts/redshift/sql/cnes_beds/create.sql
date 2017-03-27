DROP TABLE IF EXISTS cnes_bed CASCADE;

CREATE TABLE cnes_bed
(
    year                             integer encode lzo,
    region                           varchar(1) encode lzo,
    mesoregion                       varchar(4) encode lzo,
    microregion                      varchar(5) encode lzo,
    state                            varchar(2) encode lzo,
    municipality                     varchar(7) encode lzo,
    establishment                    varchar(7) encode lzo,
    unit_type                        varchar(2) encode lzo,
    bed_type                         varchar(1) encode lzo,
    bed_type_per_specialty           varchar(2) encode lzo,
    number_existing_bed              integer encode lzo,
    number_existing_contract         integer encode lzo,
    number_sus_bed                   integer encode lzo,
    number_non_sus_bed               integer encode lzo,
    health_region                    varchar(5) encode lzo
    
) sortkey(year,region,mesoregion,microregion,state,municipality);