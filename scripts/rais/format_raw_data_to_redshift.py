# -*- coding: utf-8 -*-

import click
import os
import bz2
import pandas as pd
import sendgrid
from datetime import datetime

@click.command()
@click.argument('input_path', type=click.Path(exists=True))
@click.argument('output_path', type=click.Path())
@click.argument('cnae_csv_path', type=click.Path())
def main(input_path, output_path, cnae_csv_path):
    """
    input_path can be a path to a single file or a path to a directory
    input_path must be a path to a directory
    """
    for file_path in file_paths(input_path):
        print file_path; begin = datetime.now()

        df = open_rais_df(file_path)
        df = add_columns_cnae_to_df(df, cnae_csv_path)
        save_df(df, file_path, output_path)

        duration = datetime.now() - begin
        send_email(df['year'][0], duration)

def open_rais_df(file_path):
    columns = ['ocupation', 'cnae', 'literacy', 'age', 'establishment', 'simple', 'municipality', 'employee', 'color', 'wage_received', 'average_monthly_wage', 'gender', 'establishment_size', 'year']
    df = pd.read_csv(
        file_path,
        sep=";",
        header=0,
        names=columns,
        converters={
            'cnae': str,
            'wage_received': lambda x : float(x.replace(',','.')),
            'average_monthly_wage': lambda x : float(x.replace(',','.'))
        },
        engine='python'
    )

    return df

def add_columns_cnae_to_df(rais_df, cnae_csv_path):
    sections_and_divisions = cnae_sections_and_divisions(cnae_csv_path)

    sections = []
    divisions = []

    activities_without_sections_and_divisions = set()
    
    unspecified = {
        'section': '0',
        'division': '00'
    }

    for cnae_id in rais_df['cnae']:
        section_and_division = sections_and_divisions.get(cnae_id, unspecified)

        if section_and_division == unspecified:
            activities_without_sections_and_divisions.add(cnae_id)

        sections.append(section_and_division['section'])
        divisions.append(section_and_division['division'])

    rais_df['cnae_section'] = sections
    rais_df['cnae_division'] = divisions

    print 'Economic activities without sections and divisions:', list(activities_without_sections_and_divisions)

    return rais_df

def cnae_sections_and_divisions(cnae_csv_path):
  cnae_df = open_cnae_df(cnae_csv_path)
  cnae = {}

  for _, row in cnae_df.iterrows():
    if len(row['id']) == 6:
      
      section = row['id'][0]
      division = row['id'][1:3]
      id = row['id'][1:]

      cnae[id] = {
        'section': section,
        'division': division
      }

  return cnae

def open_cnae_df(file_path):
    df = pd.read_csv(
        file_path,
        sep=";",
        header=0,
        names=['id', 'name_en', 'name_pt'],
        converters={
            "id": str,
            'name_en': str,
            'name_pt': str
        },
        engine='python'
    )

    return df

def save_df(df, file_path, output_path):
    if not os.path.exists(output_path):
        os.makedirs(output_path)

    year = df['year'][0]
    file_output_path = os.path.join(output_path, "rais_{0}.csv.bz2".format(year))

    df.to_csv(
        # file_output_path,
        bz2.BZ2File(file_output_path + '.bz2', 'wb'),
        sep="|",
        header=False,
        index=False,
        columns=['ocupation', 'cnae', 'cnae_section', 'cnae_division', 'literacy', 'age', 'establishment', 'simple', 'municipality', 'employee', 'color', 'wage_received', 'average_monthly_wage', 'gender', 'establishment_size', 'year']
    )

def send_email(year, duration):
    sg = sendgrid.SendGridAPIClient(apikey=os.environ.get('SENDGRID_API_KEY'))
    data = {
      "personalizations": [
        {
          "to": [
            {
              "email": "sauloantuness@gmail.com"
            }
          ],
          "subject": "Rais " + str(year) + " is completed"
        }
      ],
      "from": {
        "email": "calc-server@dataviva.info"
      },
      "content": [
        {
          "type": "text/plain",
          "value": "Duration %s" % duration
        }
      ]
    }
    response = sg.client.mail.send.post(request_body=data)
    if response.status_code == 202:
        print "email sended"

def file_paths(folder):
    if os.path.isfile(folder):
        return [folder]

    files = []

    for dirpath, _, filenames in os.walk(folder):
       for f in filenames:
           files.append(os.path.abspath(os.path.join(dirpath, f)))

    files.sort()
    return files

if __name__ == "__main__":
    main()  