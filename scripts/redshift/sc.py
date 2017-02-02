import click
import time
import pandas as pd
from io import BytesIO
from os import path

from s3 import S3
from notification import Notification
from location import Location

class Sc():
    def __init__(self, s3, csv_path):
        self.s3 = s3
        self.csv_path = csv_path
        self.filename = path.basename(csv_path)
        self.start = time.time()
        
        print self.filename
        self.open_df()

    def open_df(self):
        self.df = pd.read_csv(
            self.s3.read_csv(self.csv_path),
            sep=',',
            header=0,
            names=['year', 'COD_MATRICULA', 'COD_ALUNO', 'age', 'gender', 'MUNICIPIO_END', 'class', 'COD_ENTIDADE', 'municipality', 'DEPENDENCIA_ADM', 'ethnicity', 'course', 'COD_ETAPA_ENSINO'],
            usecols=['year', 'age', 'gender', 'class', 'municipality', 'ethnicity', 'course'],
            converters={
                'course': lambda x: '' if x == 'NULL' else str(x).strip(),
                'class': lambda x: '' if x == 'NULL' else str(x).strip(),
                'municipality': lambda x: '' if x == 'NULL' else str(x),
                'age': lambda x: 0 if not x or x == 'NULL' else x,
            },
            dtype={
                'year': 'int16',
                'age': 'int8',
                'gender': 'int8',
                'class': 'str',
                'municipality': 'str',
                'ethnicity': 'int8',
                'course': 'str',
            },
            engine='c'
        )


    def save(self, output):
        csv_buffer = BytesIO()

        self.df.to_csv(
            csv_buffer,
            sep="|",
            index=False,
            columns=['year', 'region', 'mesoregion', 'microregion', 'state', 'municipality', 'course_field', 'course', 'class', 'age', 'gender', 'ethnicity'],
        )

        self.s3.resource.Object('dataviva-etl', path.join(output, self.filename)).put(Body=csv_buffer.getvalue())

        self.duration = time.time() - self.start
        self.duration_str = '%02d:%02d' % (self.duration / 60, self.duration % 60)
        print '  Saved.'
        print '  Time: %s' % self.duration_str

    def add_year(self):
        try:
            year = self.filename.replace('_','.').split('.')[2]
            self.df['year'] = int(year)
        except:
            print 'Filename must be in format "hedu_student_yyyy.csv".'

        print '+ year'

    def add_course_field(self):
        course_fields = []
        for course in self.df['course']:
            course_fields.append(course[:2])

        self.df['course_field'] = course_fields

        print '+ course field'

    def fix_municipality(self, location):
        municipalities_6 = []
        municipalities_7 = []

        for _, row in location.municipalities_df.iterrows():
                id_ibge = row['municipality'][:-1]
                municipalities_6.append(id_ibge)
                municipalities_7.append(self.add_dv_to_id_ibge(id_ibge))

        municipalities_df = pd.DataFrame({
            'municipality': pd.Series(municipalities_6),
            'municipality7': pd.Series(municipalities_7)
        })

        self.df = pd.merge(
            self.df, 
            municipalities_df,
            on='municipality',
            how='left'
        )

        self.df.drop('municipality', 1, inplace=True)
        self.df.rename(
            columns={
                'municipality7': 'municipality'
            }, 
            inplace=True
        )

        print '- fix municipalities'

    def add_dv_to_id_ibge(self, id_ibge):
        invalid = {
            '220191': '2201919',
            '220225': '2202251',
            '220198': '2201988',
            '261153': '2611533',
            '311783': '3117836',
            '315213': '3152131',
            '430587': '4305871',
            '520393': '5203939',
            '520396': '5203962'
        }
        
        if invalid.get(id_ibge):
            return invalid.get(id_ibge)

        pesos = [1,2,1,2,1,2]
        ponderacao = []

        for i in range(6):
            ponderacao.append(int(id_ibge[i]) * pesos[i])

        ponderacao = map(lambda x: x if x < 10 else 1 + (x % 10), ponderacao)

        soma = reduce(lambda x, y: x + y, ponderacao)

        resto = soma % 10

        if resto:
            dv = 10 - resto
        else:
            dv = 0

        return id_ibge + str(dv)


@click.command()
@click.argument('input', default='redshift/raw_from_mysql/sc', type=click.Path())
@click.argument('output', default='redshift/raw_from_mysql/sc_formatted', type=click.Path())
def main(input, output):
    s3 = S3()
    notification = Notification()
    location = Location(s3)
    
    for csv_path in s3.get_keys(input):
        sc = Sc(s3, csv_path)
        
        sc.add_course_field()
        sc.fix_municipality(location)

        sc.df = location.add_columns(sc.df)

        sc.save(output)
        notification.send_email('sauloantuness@gmail.com', sc.filename, sc.duration_str)


if __name__ == '__main__':
    main()
