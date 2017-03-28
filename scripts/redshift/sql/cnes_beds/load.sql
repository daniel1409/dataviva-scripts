copy cnes_bed (year, region, mesoregion, microregion, state, municipality, establishment, unit_type, bed_type, bed_type_per_specialty, number_existing_bed, number_existing_contract, number_sus_bed, number_non_sus_bed, health_region)
from 's3://dataviva-etl/redshift/raw_from_mysql/cnes_formatted/cnes_bed' 
credentials 'aws_iam_role=arn:aws:iam::<aws_iam_role>:role/Redshift'
ignoreheader 1;