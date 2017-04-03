copy cnes_search (id, name_pt, region, mesoregion, microregion, state, municipality, search)
from 's3://dataviva-etl/attrs/cnes_location' 
credentials 'aws_iam_role=arn:aws:iam::<aws_iam_role>:role/Redshift'
ignoreheader 1
delimiter(';');
