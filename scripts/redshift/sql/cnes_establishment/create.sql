DROP TABLE IF EXISTS cnes_establishment CASCADE;

CREATE TABLE cnes_establishment
(
    year                             integer encode lzo,
    region                           varchar(1) encode lzo,
    mesoregion                       varchar(4) encode lzo,
    microregion                      varchar(5) encode lzo,
    state                            varchar(2) encode lzo,
    municipality                     varchar(7) encode lzo,
    establishment                    varchar(7) encode lzo,
    unit_type                        varchar(2) encode lzo, 
    sus_bond                         varchar(1) encode lzo,
    provider_type                    varchar(2) encode lzo,
    ambulatory_attention             varchar(1) encode lzo,
    hospital_attention               varchar(1) encode lzo,
    emergency_facility               varchar(1) encode lzo,
    ambulatory_care_facility         varchar(1) encode lzo,
    surgery_center_facility          varchar(1) encode lzo,
    obstetrical_center_facility      varchar(1) encode lzo,
    neonatal_unit_facility           varchar(1) encode lzo,
    hospital_care                    varchar(1) encode lzo,
    selective_waste_collection       varchar(1) encode lzo,
    dependency_level                 varchar(1) encode lzo,
    health_region                    varchar(5) encode lzo,
    administrative_sphere            varchar(2) encode lzo,
    tax_withholding                  varchar(2) encode lzo,
    hierarchy_level                  varchar(2) encode lzo
) sortkey(year,region,mesoregion,microregion,state,municipality);