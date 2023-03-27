from decouple import config
from envyaml import EnvYAML

devMode = EnvYAML('config.yaml').get('dev_mode')

# ClickUp creds
clickup_token = config('CLICKUP_TOKEN')
clickup_team_id = config('CLICKUP_TEAM_ID')

# Clockify creds
clocify_token = config('CLOCKIFY_TOKEN')
clockify_workspace_id = config('CLOCKIFY_ATIDIV_WORKSPACE_ID')

if devMode:
    clocify_token = EnvYAML('config.yaml').get('dev').get('CLOCKIFY_TOKEN')
    clockify_workspace_id = EnvYAML('config.yaml').get('dev').get('CLOCKIFY_ATIDIV_WORKSPACE_ID')

asana_token = config('ASANA_TOKEN')