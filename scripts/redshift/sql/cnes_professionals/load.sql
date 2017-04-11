copy cnes_professional (year, region, mesoregion, microregion, state, municipality, establishment, unit_type, occupation_group, occupation_family, cns_number, professional_link, sus_healthcare_professional, other_hours_worked, hospital_hour, ambulatory_hour, year, health_region, hierarchy_level)
from 's3://dataviva-etl/redshift/raw_from_mysql/cnes_formatted/cnes_professional' 
credentials 'aws_iam_role=arn:aws:iam::<aws_iam_role>:role/Redshift'
ignoreheader 1;
