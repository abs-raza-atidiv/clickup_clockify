from envyaml import EnvYAML

devMode = EnvYAML('config.yaml').get('dev_mode')
table_prefix = '_' if devMode else ''

CLICKUP_SPACE = table_prefix + 'clickup_space'
CLICKUP_TASK = table_prefix + 'clickup_task'
CLICKUP_LIST = table_prefix + 'clickup_list'

CLOCKIFY_CLIENT = table_prefix + 'clockify_client'
CLOCKIFY_PROJECT = table_prefix + 'clockify_project'
CLOCKIFY_TASK = table_prefix + 'clockify_task'

CLOCKIFY_USER = table_prefix + 'clockify_user'
CLOCKIFY_TIMESHEET = table_prefix + 'clockify_timesheet'

ASANA_TASKS = table_prefix + 'asana_task'
WATERMARK = table_prefix + 'watermark'