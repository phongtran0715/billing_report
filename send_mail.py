import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from os import path
from threading import Thread, Event


class SendEmail:
    smtp_server = None
    smtp_port = None
    sender_email = None
    password = None

    def __init__(self, smtp_server, smtp_port, sender_email, password):
        self.smtp_server = smtp_server
        self.smtp_port = smtp_port
        self.sender_email = sender_email
        self.password = password

    def send_email(self, receiver_email, title, body, attachment=None):
        msg = MIMEMultipart()
        msg['From'] = self.sender_email
        msg['To'] = receiver_email
        msg['Subject'] = title
        msg.attach(MIMEText(body, 'plain'))

        if attachment is None:
            pass
        else:
            if path.exists(attachment):
                # open the file to be sent
                file = open(attachment, "rb")
                # instance of MIMEBase and named as p
                p = MIMEBase('application', 'octet-stream')
                # To change the payload into encoded form
                p.set_payload(file.read())
                # encode into base64
                encoders.encode_base64(p)
                p.add_header('Content-Disposition', "attachment; filename= %s" % attachment)
                # attach the instance 'p' to instance 'msg'
                msg.attach(p)

        s = smtplib.SMTP(self.smtp_server, self.smtp_port)
        # start TLS for security
        s.starttls()
        # Authentication
        s.login(self.sender_email, self.password)
        # Converts the Multipart msg into a string
        text = msg.as_string()
        # sending the mail
        s.sendmail(self.sender_email, receiver_email, text)
        s.quit()
