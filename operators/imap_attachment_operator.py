from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from imap_plugin.hooks.imap_hook import IMAPHook
from datetime import datetime


class IMAPAttachmentOperator(BaseOperator):
    '''
    IMAP Attachment Operator
    Searches for an email recieved on the execution date
    and downloads the attachement to disc.
    :param imap_conn_id: The airflow connection id.
    :type imap_conn_id: String.
    :param mailbox: The mailbox to search.
    :type mailbox: String.
    :param search_criteria: The email search criteria.
    :type search_criteria: Dictionary.
    :param local_path: The local directory to save attachments.
    :type local_path: String.
    :param file_name: An optional new name for the saved attachment.
    :type file_name: String.
    '''
    template_fields = ['file_name']

    @apply_defaults
    def __init__(self,
                 imap_conn_id,
                 mailbox,
                 search_criteria={},
                 local_path='',
                 file_name='',
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.imap_conn_id = imap_conn_id
        self.mailbox = mailbox
        self.search_criteria = search_criteria
        self.local_path = local_path
        self.file_name = file_name

    def execute(self, context):
        yesterday = context.get('yesterday_ds')
        self.mail_date = datetime.strptime(
            yesterday, '%Y-%m-%d').strftime('%d-%b-%Y')
        self.search_criteria['ON'] = self.mail_date
        self.get_attachment()

    def get_attachment(self):
        '''
        Gets the email attachment.
        '''
        imap_hook = IMAPHook(self.imap_conn_id)
        imap_hook.authenticate()
        mail_id = imap_hook.find_mail(
            mailbox=self.mailbox,
            search_criteria=self.search_criteria)
        if mail_id:
            imap_hook.get_mail_attachment(mail_id,
                                          self.local_path,
                                          self.file_name)
