import utils.endpoints as api
import settings as env
import requests
import pandas as pd 
import json 
import utils.bigquery_utils as bq
import utils.db as db
from  datetime import datetime, date, timedelta
import time
from envyaml import EnvYAML
CONFIG = EnvYAML('config.yaml').get('prod')


## --------------------------------------------------------------------------------------------------
## GET LIST SPACES THAT NEEDS NOT TO BE MOVED TO CLOCKIFY
## --------------------------------------------------------------------------------------------------
def standardize_column(df):

    columns = df.columns

    new_columns = [elm.replace('.','_') for elm in columns]

    df.columns = new_columns

    return df
    

## --------------------------------------------------------------------------------------------------
## GET LIST SPACES THAT NEEDS NOT TO BE MOVED TO CLOCKIFY
## --------------------------------------------------------------------------------------------------
def get_clockify_clients_bq():
    df = pd.DataFrame()
    try:
        sql = "select * from {}.{}.{}".format(
            bq.gcp_project, 
            bq.bq_dataset,
            db.CLOCKIFY_CLIENT
        )

        df = bq.gcp2df(sql) 

        return df
        # value_list = df.values.tolist()

    except Exception as E:
        print(str(E))
    

## --------------------------------------------------------------------------------------------------
## GET LIST FROM BIGQUERY
## --------------------------------------------------------------------------------------------------
def get_clickup_list_ids_bq():
    df = pd.DataFrame()
    try:
        sql = "select id from {}.{}.{}".format(
            bq.gcp_project, 
            bq.bq_dataset,
            db.CLICKUP_LIST
        )

        df = bq.gcp2df(sql) 

        return df
        # value_list = df.values.tolist()

    except Exception as E:
        print(str(E))
    

## --------------------------------------------------------------------------------------------------
## LOGGING FUNCTIONS
## --------------------------------------------------------------------------------------------------
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


## --------------------------------------------------------------------------------------------------
## TIMESTAMP RELATED FUNCTIONS
## --------------------------------------------------------------------------------------------------
def get_unix_timestamp(_db_date, timedelay=0):
    '''
    Calculates unix timestamp of pull_date passed on. with delay (minutes) if provided
    Returns: unix timestamp
    '''

    last_cycle = datetime.strptime(_db_date, "%Y-%m-%d %H:%M:%S") - timedelta(minutes=timedelay)

    unix_time = int(time.mktime(last_cycle.timetuple()) * 1000)

    return unix_time, last_cycle


def current_date_time():
    ''' String Format : '2023-02-23 19:23:44' '''
    try:
        return datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    except Exception as e:
        print(str(e))

## --------------------------------------------------------------------------------------------------
## GET LIST SPACES THAT NEEDS NOT TO BE MOVED TO CLOCKIFY
## --------------------------------------------------------------------------------------------------
def update_task_name(_clockify_project_id, _clockify_task_id, _clickup_child_id, _clickup_child_name, _clickup_parent_id='', _clickup_parent_name=''):
    try:
        import ipdb; ipdb.set_trace()
        url = api.clociky_single_task.format(projectId=_clockify_project_id, taskId=_clockify_task_id)

        if _clickup_parent_id:
            task_new_name = "{} || {} || {} || {}".format(_clickup_child_id, _clickup_child_name, _clickup_parent_id, _clickup_parent_name)
        else:
            task_new_name = "{} || {}".format(_clickup_child_id, _clickup_child_name)

        payload = json.dumps({
            "name": task_new_name
        })
        print(task_new_name)
        response = requests.put(url=url, headers=api.clockify_header , data=payload)

        if response.status_code == 200:
            # log_error('updated {}.'.format(_clickup_child_id), 'updated_clockify_tasks')
            return response
        else:
            return None
    except Exception as e:
        print(str(e))



## --------------------------------------------------------------------------------------------------
## GET LIST SPACES THAT NEEDS NOT TO BE MOVED TO CLOCKIFY
## --------------------------------------------------------------------------------------------------
def get_clickup_rejected_spaces():
    try:
        rejected_list = CONFIG.get('rejected_clickup_space_ids')
        rejected_list_str = [str(x) for x in rejected_list]
        return rejected_list_str        
        
    except Exception as e:
        print(str(e))


## --------------------------------------------------------------------------------------------------
## GET LIST SPACES THAT NEEDS NOT TO BE MOVED TO CLOCKIFY
## --------------------------------------------------------------------------------------------------
def dump_updated_clickup_list_to_bq(all_lists):
    try:
        if len(all_lists)>0:
            all_lists_df = pd.json_normalize(all_lists)

            if len(all_lists_df.columns)>0:

                new_df = standardize_column(all_lists_df)

                new_df['pull_date'] = current_date_time()

                bq.df2gcp(new_df, db.CLICKUP_LIST, mode='append')
            else:
                log_error('NO COLUMNS FOUND IN NEW LIST', 'log__'+str(date.today()))

    except Exception as e:
        log_error(str(e), 'log__'+str(date.today()))
        print(str(e))


## --------------------------------------------------------------------------------------------------
## GET LIST SPACES THAT NEEDS NOT TO BE MOVED TO CLOCKIFY
## --------------------------------------------------------------------------------------------------
def dump_updated_clockify_project_to_bq(_responses):
    '''Append new projects entries in clockify_projects table'''
    try:
        if len(_responses)>0:
            all_lists_df = pd.json_normalize(_responses)

            if len(all_lists_df.columns)>0:

                project_df = standardize_column(all_lists_df)
                project_df.drop(axis = 1, columns=['memberships'], inplace=True)
                project_df['pull_date'] = current_date_time()

                bq.df2gcp(project_df, db.CLOCKIFY_PROJECT, mode='append')
            else:
                log_error('NO COLUMNS FOUND IN NEW LIST', 'log__'+str(date.today()))

    except Exception as e:
        # log_error(str(e), 'log__'+str(date.today()))
        print(str(e))


## --------------------------------------------------------------------------------------------------
## GET LIST SPACES THAT NEEDS NOT TO BE MOVED TO CLOCKIFY
## --------------------------------------------------------------------------------------------------
def dump_updated_clickup_space_to_bq(_responses_df, drop_col=[]):
    '''Append new space entries in clickup_space table'''
    try:
        if len(_responses_df.columns)>0:

            space_df = standardize_column(_responses_df)
            # project_df.drop(axis = 1, columns=[], inplace=True)
            db_columns = ['id','name','color','private','admin_can_manage','multiple_assignees',
                          'archived','pull_date']
            space_df = space_df[db_columns]

            space_df['pull_date'] = current_date_time()

            bq.df2gcp(space_df, db.CLICKUP_SPACE, mode='replace')
        else:
            log_error('NO COLUMNS FOUND IN NEW LIST', 'log__'+str(date.today()))

    except Exception as e:
        # log_error(str(e), 'log__'+str(date.today()))
        print(str(e))


## --------------------------------------------------------------------------------------------------
## GET UPDATED OBJECT VALUES
## --------------------------------------------------------------------------------------------------
def get_updated_tasks_from_list(list_id, _unix_ts):
    '''Arguement required - ClickupListID, Time since updated (from config)
    
    Returns: DataFrame - containing tasks that are updated in _unix_ts time for provided list ID'''
    all_tasks = []
    page = 0

    params = "&date_updated_gt="+str(_unix_ts)

    while True:

        response = requests.get(url=api.clickup_task.format(list_id=list_id, 
                                                                   page_no=page, 
                                                                   addon_parameters=params),
                                headers=api.clickup_header)

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


## --------------------------------------------------------------------------------------------------
## GET UPDATED OBJECT VALUES
## --------------------------------------------------------------------------------------------------
def fetch_updated_tasks_from_clickup():
    master_tasks_df = pd.DataFrame()
    
    pull_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    try: 
        db_pull_date = bq.gcp2df("select max(pull_date) from `{}.{}.{}` where object_type='task' and platform_name='clickup'"
                                  .format(bq.gcp_project, bq.bq_dataset, db.WATERMARK))
    except Exception as e: 
        print(str(e))
        db_pull_date=[] 
    
    db_pull_date = db_pull_date.values[0][0]
    if not db_pull_date:
        db_pull_date = datetime.strftime(datetime.now() - timedelta(days=1), '%Y-%m-%d %H:%M:%S')

    unix_ts, datetime_ts = get_unix_timestamp(db_pull_date, 20)
    # for spc in spaces:

    list_ids = get_clickup_list_ids_bq()['id'].values.tolist()
    # import ipdb; ipdb.set_trace()

    for lst in list_ids:
        try:
            
            tasks_df = get_updated_tasks_from_list(lst, unix_ts)
            if len(tasks_df):
                a2 = tasks_df[['id', 'name', 'date_updated']]

                master_tasks_df = pd.concat([master_tasks_df, a2])

                print('{} Appended {} row to master_df. New master len {}'.format(datetime.now(), 
                                                                                len(tasks_df), 
                                                                                len(master_tasks_df)))
            # if len(master_tasks_df)>=4: break

        except Exception as e:
            # log_error(str(e), 'log__'+str(date.today()))
            print(str(e))

    # import ipdb; ipdb.set_trace()

    print('parsed all spaces\n\n')

    a3 = standardize_column(master_tasks_df)

    # master_tasks_df['pull_date'] = pull_date

    to_update_records = create_snapshot(a3, datetime_ts)

    return to_update_records


## --------------------------------------------------------------------------------------------------
## GET UPDATED OBJECT VALUES
## --------------------------------------------------------------------------------------------------
def create_snapshot(new_df, _time_marker):
    try:
        ## Before dataframe Columns: [id, name, date_updated]
        db_df = get_previous_data_from_bq(_time_marker) 

        ## Remove extra columns which we do not want to compare
        # new_df.drop(axis = 1, columns = ['cursor'], inplace=True)
        
        # old_df = db_df.drop(axis = 1, columns = ['pull_date','cursor'])
        old_df = db_df

        ## JOIN 
        diff_df = new_df.merge(old_df, on=new_df.columns.tolist(), how='left', indicator=True)

        ## ANTI-JOIN
        updated_and_new_data = diff_df.loc[diff_df['_merge'] == 'left_only', new_df.columns]

        updated_and_new_data['pull_date'] = str(datetime.now())

        return updated_and_new_data
    
    except Exception as e: 
        print(str(e))
        return pd.DataFrame() 


## --------------------------------------------------------------------------------------------------
## GET UPDATED OBJECT VALUES
## --------------------------------------------------------------------------------------------------
def get_previous_data_from_bq(_pull_date):
    query = "select platform_id as id, new_value as name, platform_update_date as date_updated from \
            {}.{}.{} where platform_name='clickup' and object_type='task' \
            and pull_date >='{}'".format(bq.gcp_project, 
                                           bq.bq_dataset, 
                                           db.WATERMARK,
                                           _pull_date)
    df = bq.gcp2df(query)
    return df


## --------------------------------------------------------------------------------------------------
## GET UPDATED OBJECT VALUES
## --------------------------------------------------------------------------------------------------
def get_clockify_task_mapping(_filter_ids):
    query = "select id as clockify_id, projectId as clockify_project_id, clickup_task_id, \
            name as clockify_task_name from \
            {}.{}.{} where clickup_task_id in ('{}')".format(bq.gcp_project, 
                                           bq.bq_dataset, 
                                           db.CLOCKIFY_TASK,
                                           "','".join(_filter_ids))
    print(query)
    df = bq.gcp2df(query)
    return df