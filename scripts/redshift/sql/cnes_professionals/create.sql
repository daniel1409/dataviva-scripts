DROP TABLE IF EXISTS cnes_professional CASCADE;

CREATE TABLE cnes_professional
(
    year                             integer encode lzo,
    region                           varchar(1) encode lzo,
    mesoregion                       varchar(4) encode lzo,
    microregion                      varchar(5) encode lzo,
    state                            varchar(2) encode lzo,
    municipality                     varchar(7) encode lzo,
    establishment                    varchar(7) encode lzo,
    unit_type                        varchar(2) encode lzo,
    occupation_group                 varchar(1) encode lzo,
    occupation_family                varchar(4) encode lzo,
    cns_number                       varchar(15) encode lzo,
    professional_link                varchar(8) encode lzo,
    sus_healthcare_professional      varchar(1) encode lzo,
    other_hours_worked               integer encode lzo,
    hospital_hour                    integer encode lzo,
    ambulatory_hour                  integer encode lzo,
    health_region                    varchar(5) encode lzo
    
    
) sortkey(year, region, mesoregion, microregion, state, municipality);