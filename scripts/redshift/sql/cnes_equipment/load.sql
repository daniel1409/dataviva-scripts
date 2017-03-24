copy cnes_equipment (year, region, mesoregion, microregion, state, municipality, establishment, dependency_level, unit_type, equipment_type, equipment_code, equipment_quantity, equipment_quantity_in_use, sus_availability_indicator, health_region)
from 's3://dataviva-etl/redshift/raw_from_mysql/cnes_formatted/cnes_equipment' 
credentials 'aws_iam_role=arn:aws:iam::<aws_iam_role>:role/Redshift'
ignoreheader 1;
