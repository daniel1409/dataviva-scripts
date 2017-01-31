import click
import time
import pandas as pd
from io import BytesIO
from os import path

from s3 import S3
from notification import Notification
from location import Location

class Hedu():
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
            names=['university', 'CO_CATEGORIA_ADMINISTRATIVA', 'CO_ORGANIZACAO_ACADEMICA', 'CO_CURSO', 'CO_ALUNO', 'CO_GRAU_ACADEMICO', 'CO_MODALIDADE_ENSINO', 'graduates', 'entrants', 'DT_INGRESSO_CURSO', 'gender', 'age', 'ethnicity', 'CO_UF_NASCIMENTO', 'CO_MUNICIPIO_NASCIMENTO', 'shift', 'municipality', 'course', 'CO_LOCAL_OFERTA_IES', 'ANO_INGRESSO', 'MES_INGRESSO'],
            usecols=['course', 'university', 'shift', 'age', 'graduates', 'entrants', 'gender', 'ethnicity', 'municipality'],
            converters={
                'university': lambda x: '%05d' % int(x),
                'municipality': str,
                'course': lambda x: x.strip()
            },
            engine='c'
        )


    def save(self, output):
        csv_buffer = BytesIO()

        self.df.to_csv(
            csv_buffer,
            sep="|",
            index=False,
            columns=['year', 'region', 'mesoregion', 'microregion', 'state', 'municipality', 'university', 'course_field', 'course', 'shift', 'age', 'graduates', 'entrants', 'gender', 'ethnicity'],
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


@click.command()
@click.argument('input', default='redshift/raw_from_mysql/hedu/hedu_student', type=click.Path())
@click.argument('output', default='redshift/raw_from_mysql/hedu_formatted', type=click.Path())
def main(input, output):
    s3 = S3()
    notification = Notification()
    location = Location(s3)
    
    for csv_path in s3.get_keys(input):
        hedu = Hedu(s3, csv_path)
        
        hedu.add_year()
        hedu.add_course_field()

        hedu.df = location.add_columns(hedu.df)

        hedu.save(output)
        notification.send_email('sauloantuness@gmail.com', hedu.filename, hedu.duration_str)


if __name__ == '__main__':
    main()
