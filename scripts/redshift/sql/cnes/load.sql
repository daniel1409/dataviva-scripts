copy cnes_names (id, name_pt, name_en)
from 's3://dataviva-etl/attrs/cnes_final' 
credentials 'aws_iam_role=arn:aws:iam::<aws_iam_role>:role/Redshift'
ignoreheader 1
delimiter ';';