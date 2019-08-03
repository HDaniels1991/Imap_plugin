
from airflow.plugins_manager import AirflowPlugin
from imap_plugin.hooks.imap_hook \
    import IMAPHook
from imap_plugin.operators.imap_attachment_operator \
    import IMAPAttachmentOperator


class IMAPPlugin(AirflowPlugin):
    name = 'imap_plugin'
    operators = [IMAPAttachmentOperator]
    hooks = [IMAPHook]
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []