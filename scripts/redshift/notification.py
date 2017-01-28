import os
import sendgrid


class Notification():
    def send_email(self, destination, subject, content=''):
        sg = sendgrid.SendGridAPIClient(apikey=os.environ.get('SENDGRID_API_KEY'))
        data = {
          "personalizations": [
            {
              "to": [
                {
                  "email": destination
                }
              ],
              "subject": subject
            }
          ],
          "from": {
            "email": "calc-server@dataviva.info"
          },
          "content": [
            {
              "type": "text/plain",
              "value": content
            }
          ]
        }
        response = sg.client.mail.send.post(request_body=data)
        if response.status_code == 202:
            print "email sended"