import click
import time
import pandas as pd
from io import BytesIO
from os import path

from s3 import S3
from notification import Notification
from location import Location


class CnesEstablishment():
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
            names=['cnes','municipality','cod_cep','cpf_cnpj','pf_pj','cnpj_man','vinc_sus','tp_unid','tp_prest','nivate_a','nivate_h','qt_leito_hosp_cirurg','qt_leito_hosp_clin','qt_leito_hosp_complex','qt_sala_pedi_ue','qt_sala_rep_pedi_ue','qt_cons_odonto_ue','qt_sala_higie_ue','qt_sala_gesso_ue','qt_sala_curativo_ue','qt_sala_peqcirur_ue','qt_sala_cons_med_ue','urgemerg','qt_cons_clincbasica_amb','qt_cons_clincesp_amb','qt_cons_clincind_amb','qt_cons_nmed_amb','qt_sala_rep_ped_amb','qt_cons_odonto_amb','qt_sala_peqcirur_amb','qt_sala_enf_amb','qt_sala_imun_amb','qt_sala_nebu_amb','qt_sala_gesso_amb','qt_sala_cura_amb','qt_sala_ciruramb_amb','atendamb','qt_sala_cirur_cc','qt_sala_recup_cc','qt_sala_ciruramb_cc','centrcir','qt_sala_preparto_co','qt_sala_partonor_co','qt_sala_curetagem_co','qt_sala_cirur_co','centrobs','qt_leito_rep_pedi_ue','qt_equip_odonto_ue','qt_leito_rep_pedi_amb','qt_equip_odonto_amb','qt_leito_recu_cc','qt_leito_preparto_co','centrneo','atendhos','coletres','qt_sala_atend_adulto_ue','qt_sala_obs_adulto_ue','qt_sala_rep_amb','qt_leito_rep_ue','qt_leito_rep_amb','qt_leito_rn_nn','atencaobasica_amb','mediacomplexidade_amb','altacomplexidade_amb','internacao_hosp','mediacomplexidade_hosp','altacomplexidade_hosp','tipointernacao','tipodeambulatorio','tipodesadt','tipodeurgencia','niv_dep1','regsaude','esfera','retencao_2','niv_hier_2'],
            usecols=['cnes', 'municipality'],
            converters={
                'municipality': str,
                'cnes': str
            },
            engine='c'
        )

    def save(self, output):
        csv_buffer = BytesIO()

        self.df.to_csv(
            csv_buffer,
            sep="|",
            index=False,
            columns=['year', 'region', 'mesoregion', 'microregion', 'state', 'municipality', 'cnes']
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
            print 'Filename must be in format "establishment_yyyy.csv".'

        print '+ year'

@click.command()
@click.argument('input', default='redshift/raw_from_mysql/cnes/cnes_establishment', type=click.Path())
@click.argument('output', default='redshift/raw_from_mysql/cnes_formatted', type=click.Path())
def main(input, output):
    s3 = S3()
    notification = Notification()
    location = Location(s3)

    for csv_path in s3.get_keys(input):
        cnes_establishment = CnesEstablishment(s3, csv_path)

        cnes_establishment.add_year()

        cnes_establishment.df = location.fix_municipality(cnes_establishment.df)
        cnes_establishment.df = location.add_columns(cnes_establishment.df)

        cnes_establishment.save(output)
        notification.send_email('sauloantuness@gmail.com', cnes_establishment.filename, cnes_establishment.duration_str)

if __name__ == '__main__':
    main()
