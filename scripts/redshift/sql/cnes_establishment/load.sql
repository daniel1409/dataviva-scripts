copy cnes_establishment (year, region, mesoregion, microregion, state, municipality, establishment, unit_type, sus_bond, provider_type, ambulatory_attention, hospital_attention, emergency_facilities, ambulatory_care_facilities, surgery_center_facilities, obstetrical_center_facilities, neonatal_unit_facilities, hospital_care, selective_waste_collection, dependency_level, health_region, administrative_sphere, tax_withholding, hierarchy_level)
from 's3://dataviva-etl/redshift/raw_from_mysql/cnes_formatted/cnes_establishment' 
credentials 'aws_iam_role=arn:aws:iam::<aws_iam_role>:role/Redshift'
ignoreheader 1;
