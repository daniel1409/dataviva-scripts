DROP TABLE IF EXISTS cnes_search CASCADE;

CREATE TABLE cnes_search
(
    id           varchar(7)    encode lzo,
    name_pt      varchar(255)  encode lzo,
    region       varchar(255)  encode lzo,
    mesoregion   varchar(255)  encode lzo,
    microregion  varchar(255)  encode lzo,
    state        varchar(255)  encode lzo,
    municipality varchar(255)  encode lzo,
    search       varchar(1000) encode lzo
    
);