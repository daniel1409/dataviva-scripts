# -*- coding: utf-8 -*-

import click
import os
import bz2
import pandas as pd
import sendgrid
from datetime import datetime

@click.command()
@click.argument('file_path', type=click.Path(exists=True))
@click.argument('output_path', default='output', type=click.Path())
def main(file_path, output_path):
    for year in ['2003', '2004', '2005', '2006', '2007', '2008', '2009', '2010', '2011', '2012', '2013', '2014']:
        print file_path.replace("2003", year)

        begin = datetime.now()
        
        generate_csv_bz2(file_path.replace("2003", year), output_path)
        
        end = datetime.now()
        duration = end - begin

        send_email(year, duration)

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
          "subject": "Rais " + year + " is completed"
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


def generate_csv_bz2(file_path, output_path):
    df = pd.read_csv(file_path, sep=";", header=0)

    year = df['Year'][0]

    if not os.path.exists(output_path):
        os.makedirs(output_path)

    new_file_path = os.path.join(output_path, "rais_{0}.csv.bz2".format(year))
    df.to_csv(bz2.BZ2File(new_file_path, 'wb'), sep=";", index=False, header=False)


if __name__ == "__main__":
    main()  