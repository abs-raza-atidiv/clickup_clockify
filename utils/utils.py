import utils.endpoints as api
import settings as env
import requests
import pandas as pd 
import json 
import utils.bigquery_utils as bq
import utils.db as db

from  datetime import datetime

# GET CLICKUP SPACES
# ---------------------------------------------------------------
def get_clickup_spaces():
    try:
        spaces = {}
        url = api.clickup_spaces

        response = requests.get(url, headers=api.clickup_header)

        if response.status_code == 200:
            resp = response.json()
            spaces = resp.get('spaces')

            return spaces
        

    except Exception as e:
        print(str(e))


# # GET CLICKUP FOLDERS
# # ---------------------------------------------------------------
# def get_clickup_folders(space_id):
#     try:
#         folders = {}
#         url = api.clickup_folders.format(space_id=space_id)

#         response = requests.get(url, headers=api.clickup_header)

#         if response.status_code == 200:
#             resp = response.json()
#             folders = resp.get('folders')
#             print(folders)
#             return folders
        
#     except Exception as e:
#         print(str(e))




# GET CLICKUP LISTS
# ---------------------------------------------------------------
def get_clickup_lists(space_id):
    try:
        lists = []
        url = api.clickup_folderless_list.format(space_id=space_id)

        response = requests.get(url, headers=api.clickup_header)

        if response.status_code == 200:
            resp = response.json()
            lists = resp.get('lists')
            return lists
        else:
            return lists

    except Exception as e:
        print(str(e))


# GET CLOCKIFY CLIENTS
# ---------------------------------------------------------------
def get_clockify_clients():
    try:
        
        response = requests.get(url=api.clockify_client_api, headers=api.clockify_header)

        if response.status_code == 200:
            resp = response.json()
            return resp

    except Exception as e:
        print(str(e))


# CREATE CLOCKIFY CLIENTS
# ---------------------------------------------------------------
def create_clockify_client(client_name, client_note):
    try:
        
        payload = json.dumps({
            "name": client_name,
            "note": client_note
        })

        response = requests.post(url=api.clockify_client_api, headers=api.clockify_header, data = payload)
        if response.status_code == 201:
            print('client created - ', client_name)
            return pd.json_normalize( response)
        else:
            print('failed api. Err: ', response.text)

    except Exception as e:
        
        print(str(e))


# CREATE CLOCKIFY projects
# ---------------------------------------------------------------
def create_clockify_projects(project_name, project_note, client_id):
    try:
        
        payload = json.dumps({
            "name": project_name,
            "note": project_note,
            "clientId": client_id ## client_id of an existing client on Clockify
        })

        response = requests.post(url=api.clockify_project_api, headers=api.clockify_header, data = payload)

        if response.status_code == 201:
            print('project created - ', project_name)
        else:
            print('failed api. Err: ', response.text)

    except Exception as e:
        
        print("create_clockify_projects ", str(e))


# GET CLOCKIFY projects
# ---------------------------------------------------------------
def get_clockify_projects():
    try:
        
        response = requests.get(url=api.clockify_project_api, headers=api.clockify_header)
        
        if response.status_code == 200:
            projects = response.json()
            return projects

    except Exception as e:
        
        print("create_clockify_projects ", str(e))



def get_space_client_mapping():

    client = get_clockify_clients()
    mapping = {}

    for elm in client:
        mapping[elm['note']] = elm['id']

    return mapping


def get_clickup_tasks(list_id):

    all_tasks = []
    page = 0

    while True:

        response = requests.get(url=api.clickup_task.format(list_id=list_id, page_no=page), headers=api.clickup_header)

        if response.status_code == 200:
            resp = response.json()
            tasks = resp['tasks']

            if len(tasks) > 0:
                all_tasks.extend(tasks)
                page += 1
            else: 
                break

    tasks_df = pd.json_normalize(all_tasks)

    return tasks_df

def fetch_all_clickup_tasks():

    master_tasks_df = pd.DataFrame()

    spaces = get_clickup_spaces()

    for spc in spaces:

        space_list = get_clickup_lists(spc['id'])

        for lst in space_list:

            tasks_df = get_clickup_tasks(lst['id'])
            # print(tasks_df)
            master_tasks_df =  pd.concat([master_tasks_df, tasks_df])

            print('Appended {} row to master_df. New master len {}'.format(len(tasks_df), len(master_tasks_df)))

        print('ended list {}\n\n'.format(spc['name']))

    print('parsed all spaces\n\n')

    print(master_tasks_df)  

    print(master_tasks_df.columns)
    
    standardize_column(master_tasks_df)

    master_tasks_df['pull_date'] = datetime.now()

    print(master_tasks_df.columns)
    print(master_tasks_df.head())

    return master_tasks_df


def standardize_column(df):

    columns = df.columns

    new_columns = [elm.replace('.','_') for elm in columns]

    df.columns = new_columns
    

def get_clockify_clients_bq():
    value_list = []

    try:
        sql = "select distinct id as client_id from {}.{}.{}".format(
            bq.gcp_project, 
            bq.bq_dataset,
            db.CLICKUP_TASK
        )

        df = bq.gcp2df(sql) 

        value_list = df.values.tolist()

    except Exception as E:
        print(str(E))
    
    
    return value_list


def create_clockify_task(proj_id, task_name, clickup_list_id, clickup_task_jd):
    try:
        # import ipdb; ipdb.set_trace()
        url = api.clockify_task_api.format(project_id=proj_id)
        
        payload = json.dumps({
            "name": task_name
        })

        response = requests.post(url, headers=api.clockify_header, data=payload)

        if response.status_code == 201:
            resp = response.json()
            resp['clickup_list_id'] = clickup_list_id
            resp['clickup_task_id'] = clickup_task_jd
            resp['pull_date'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            # df = pd.DataFrame([resp])
            # print('Task created {} for projectid {}'.format(task_name, proj_id))
            ## update succesfull task to BQ
            try:
                # bq.df2gcp(df, db.CLOCKIFY_TASK, mode='append')
                write_json_log(resp, 'df_csv.json')

            except Exception as e:
                log_error(clickup_task_jd + " <<clickup_task_jd. Err Message " + str(e), "tasks_created_on_clockify_not_sent_to_bq_run2")

    except Exception as e:
        log_error(clickup_task_jd + " <<clickup_task_jd. Err Message " + str(e), "tasks_not_created_on_clockify_run2")
        # print(str(e))


def log_error(txt, file_name):
    with open(file_name+'.txt', 'a') as f:
        f.write('\n'+txt)


def read_json_log(file_name):
    with open(file_name, 'r') as f:
       data = json.load(f)
       return data

def write_json_log(err, file_name):
    with open(file_name) as f:
       log_data = json.load(f)
    
    log_data.append(err)

    # print(log_data)
    with open(file_name, 'w') as f:
        json.dump(log_data, f)

        # write_json_log({"name": "clickup task 2", "pull_date": "2023-01-24 00:00:00"}, 'df_csv.json')


def get_clockify_tasks(project_id):

    url = api.clockify_task_api.format(project_id=project_id)+'?page-size=5000&is-active=true'

    response = requests.get(url, headers=api.clockify_header)

    if response.status_code == 200:
        resp = json.loads(response.text)

        return resp


def DELETE_ALL_CLOCKIFY_TASK():
    

    project = get_clockify_projects()

    for prj in project:
        try:
            project_id = prj['id']
            
            if project_id == '63e23e4c192143097fc8d3ea': continue

            tasks = get_clockify_tasks(project_id=project_id)

            log_error('\n\nDELETING {} TASKS FOR PROJECT {}'.format(len(tasks), project_id), 'delete_task')
            
            for elm in tasks:
                try:
                    task_id = elm['id']

                    delete_clockify_task(project_id=project_id, task_id=task_id)
                except Exception as e:
                    print(str(e))
                    log_error('\nerror thrown for PROJECT {} -- TASK {}. {}'.format(project_id, task_id, str(e)), 'delete_task')

            log_error('\n\n PROJECT CLEANED {}\n\n'.format(prj['name']), 'delete_task')
        except Exception as e:
            print(str(e))
            
def delete_clockify_task(project_id, task_id):

    url = api.delete_clociky_task.format(projectId=project_id, taskId=task_id)

    response = requests.delete(url=url, headers=api.clockify_header)

    if response.status_code == 200:
        log_error('SUCCESS. ID {}__{}'.format(project_id, task_id), 'delete_task')
    else:    
        log_error('\nDELETE FAILED. ID {}__{}'.format(project_id, task_id), 'delete_task')


def update_task_name(project_id, task_id, clickup_parent_id, clickup_child_id, child_name, parent_name):
    try:
        url = api.delete_clociky_task.format(projectId=project_id, taskId=task_id)

        payload = json.dumps({
            "name": "{}-{}-{}-{}".format(clickup_child_id, child_name, clickup_parent_id, parent_name)
        })
        response = requests.put(url=url, headers=api.clockify_header , data=payload)

        if response.status_code == 200:
            log_error('updated {}.'.format(task_id), 'updated_clockify_tasks')
    except Exception as e:
        print(str(e))