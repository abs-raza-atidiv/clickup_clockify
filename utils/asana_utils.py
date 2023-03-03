import utils.endpoints as api
import settings as env
import requests
import pandas as pd 
import json 
import utils.bigquery_utils as bq
import utils.db as db

from  datetime import datetime

asana_workspace_id = '701236911131463'
asana_workspace_name = 'skinlaundry.com'

def get_projects():
    '''
    Get all projects.

    Returns: List of Dictionary
    '''
    try:
        url = api.asana_project_api
        response = requests.get(url=url, headers=api.asana_header)

        if response.status_code==200:
            resp = response.json()
            data = resp.get('data')
            return data
        else:
            print(response.text)
            return []
    except Exception as e:
        print(str(e))
    

def get_tasks(project_id):
    try:
        url = api.asana_task_api.format(project_id)

        response = requests.get(url=url, headers=api.asana_header)

        if response.status_code==200:
            resp = response.json()
            data = resp.get('data')
            return data
        
    except Exception as e:
        print(str(e))

