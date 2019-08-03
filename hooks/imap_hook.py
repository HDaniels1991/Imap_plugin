from airflow.hooks.base_hook import BaseHook
import imaplib
import email
import os
import logging


class IMAPHook(BaseHook):
    '''
    Authenticates a connection and provides a list of functions
    to interact with a mail server.
    :param imap_conn_id: this is the airflow connection id.
    :type imap_conn_id: string
    '''
    def __init__(self, imap_conn_id):
        self.imap_conn_id = imap_conn_id
        self.connection = self.get_connection(imap_conn_id)

    def authenticate(self):
        '''
        Authenticates access to the mail server.
        '''
        mail = imaplib.IMAP4_SSL(self.connection.host)
        # Login to your mail
        typ, accountDetails = mail.login(user=self.connection.login,
                                         password=self.connection.password)
        if typ != 'OK':
            logging.info('Sign in failed')
        else:
            self.mail = mail

    def find_mail(self, mailbox, search_criteria={}):
        '''
        Searches for emails in the mailbox that meet the search criteria.
        :param mailbox: the mailbox to search.
        :type mailbox: string
        :param search_criteria: the mail search criteria.
        :type search_criteria: dictionary
        '''
        self.mail.select(mailbox=mailbox)

        # create search criteria
        sc = []
        for key, value in search_criteria.items():
            sc.append('({} "{}")'.format(key, value))
        sc = ' '.join(sc)
        if not sc:
            sc = 'ALL'

        # Search mail
        typ, data = self.mail.search(None, sc)
        mail_ids = data[0].split()
        # check we've found mail:
        if not mail_ids:
            logging.info('No mail found')
            return ''
        # check we've only returned one item:
        if len(data[0].split()) > 1:
            logging.info('Multiple emails found, Using latest email')
            mail_id = mail_ids[-1]
            return mail_id
        logging.info('Single email found')
        mail_id = mail_ids[0]
        return mail_id

    def get_mail_attachment(self, mail_id, local_path='', file_name=''):
        '''
        Downloads the attachements of a given email.
        :param mail_id: The id of the email in the mailbox.
        :type mail_id: string
        :param local_path: the local directory to store the attachments.
        :type local_path: string
        :param file_name: An optional new name for the saved attachment.
        :type file_name: String.
        '''
        typ, data = self.mail.fetch(mail_id, '(RFC822)')
        raw_email = data[0][1]
        # converts byte literal to string removing b''
        raw_email_string = raw_email.decode('utf-8')
        email_message = email.message_from_string(raw_email_string)

        for part in email_message.walk():
            if part.get_content_maintype() == 'multipart':
                continue
            if part.get('Content-Disposition') is None:
                continue
            fileName = part.get_filename()
            if fileName:
                logging.info('Attachment called {} found'.format(fileName))
                filePath = os.path.join(local_path, fileName)
                if file_name:
                    filePath = os.path.join(local_path, file_name)
                outdir = os.path.dirname(filePath)
                if outdir:
                    os.makedirs(outdir, exist_ok=True)
                if not os.path.isfile(filePath):
                    fp = open(filePath, 'wb')
                    fp.write(part.get_payload(decode=True))
                    fp.close()
                if os.path.isfile(filePath):
                    logging.info(
                        'Successfully downloaded attachment to {}'.format(
                            filePath))
