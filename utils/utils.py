import utils.endpoints as api
import settings as env
import requests
import pandas as pd 
import json 
import utils.bigquery_utils as bq
import utils.db as db
from  datetime import datetime, date, timedelta
import time
import numpy as np

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
## GET LIST FROM BIGQUERY
## --------------------------------------------------------------------------------------------------
def clockify_existing_users():
    df = pd.DataFrame()
    try:
        sql = "select id from {}.{}.{}".format(
            bq.gcp_project, 
            bq.bq_dataset,
            db.CLOCKIFY_USER
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



def current_date_time():
    ''' String Format : '2023-02-23 19:23:44' '''
    try:
        return datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    except Exception as e:
        print(str(e))


## --------------------------------------------------------------------------------------------------
## GET LIST SPACES THAT NEEDS NOT TO BE MOVED TO CLOCKIFY
## --------------------------------------------------------------------------------------------------
def prep_and_dump_new_data_in_bq(_df, _table_name, _mode='append'):
    try:
        if len(_df)>0 and len(_df.columns)>0:

            new_df = standardize_column(_df)

            new_df['pull_date'] = current_date_time()

            bq.df2gcp(new_df, _table_name, mode=_mode)
        else:
            log_error('No new users to dump in database', 'log__'+str(date.today()))

    except Exception as e:
        log_error(str(e), 'log__'+str(date.today()))
        print(str(e))


## --------------------------------------------------------------------------------------------------
## GET LIST SPACES THAT NEEDS NOT TO BE MOVED TO CLOCKIFY
## --------------------------------------------------------------------------------------------------
def dump_new_timesheets_on_clockify(_responses):
    ''' Input Arguement _response: List '''
    try:
        if len(_responses)>0:
            all_lists_df = pd.json_normalize(_responses)

            if len(all_lists_df.columns)>0:

                project_df = standardize_column(all_lists_df)
                project_df.drop(axis = 1, columns=['memberships'], inplace=True)
                project_df['pull_date'] = current_date_time()

                bq.df2gcp(project_df, db.CLOCKIFY_TIMESHEET, mode='append')
            else:
                log_error('NO COLUMNS FOUND IN NEW LIST', 'log__'+str(date.today()))

    except Exception as e:
        # log_error(str(e), 'log__'+str(date.today()))
        print(str(e))


## --------------------------------------------------------------------------------------------------
## GET UPDATED OBJECT VALUES
## --------------------------------------------------------------------------------------------------
def create_snapshot(new_df, old_df):
    try:
        ## Remove extra columns which we do not want to compare
        # new_df.drop(axis = 1, columns = ['cursor'], inplace=True)
        
        # old_df = db_df.drop(axis = 1, columns = ['pull_date','cursor'])
        # old_df = db_df

        ## Dropping any column of list, dict, object type to avoid messing merging
        drop_cols = []
        for col in new_df.columns: 
            if not (isinstance(new_df[col][0], str) or 
                    isinstance(new_df[col][0], int) or 
                    isinstance(new_df[col][0], float) or 
                    isinstance(new_df[col][0], bool) or 
                    isinstance(new_df[col][0], np.bool_)): 
                drop_cols.append(col)

        # import ipdb; ipdb.set_trace()
        print(drop_cols)
        new_df.drop(axis=1, columns=drop_cols,inplace=True)

        ## Get columns of df
        old_cols = old_df.columns
        new_cols = new_df.columns

        ## Get new df common matching columns with old df
        common_cols = list(np.intersect1d(new_cols, old_cols))

        ## Trim both dataframes with common columns
        new_df = new_df[common_cols]
        old_df = old_df[common_cols]

        ## JOIN 
        diff_df = new_df.merge(old_df, on=common_cols, how='left', indicator=True)

        ## ANTI-JOIN
        updated_and_new_data = diff_df.loc[diff_df['_merge'] == 'left_only', common_cols]

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
## GET COCKIFY USERS
## --------------------------------------------------------------------------------------------------
def get_clockify_users():
    try:
        all_users = []
        page = 1
        while True:
            url = api.clockify_user_api.format(page_no=page)
            response = requests.get(url=url, headers=api.clockify_header)

            if response.status_code == 200:
                resp = response.json()
                if len(resp) > 0:
                    all_users.extend(resp)
                    page += 1
                else: 
                    break
        resp_df = pd.DataFrame(all_users)
        resp_df.drop(axis=1, columns=['settings','memberships','customFields'], inplace=True)

        return resp_df
    
    except Exception as e: 
        print(str(e))
        return pd.DataFrame() 


## --------------------------------------------------------------------------------------------------
## GET COCKIFY TIME ENTRIES
## --------------------------------------------------------------------------------------------------
def get_clockify_timeentries(users_ids):
    try:
        all_entries = []
        for elm in users_ids:
            page = 1

            while True:
                url = api.clockify_timeentry_api.format(userId=elm, page_no=page)
                response = requests.get(url=url, headers=api.clockify_header)

                if response.status_code == 200:
                    resp = response.json()
                    if len(resp) > 0:
                        all_entries.extend(resp)
                        page += 1
                    else: 
                        break

        resp_df = pd.json_normalize(all_entries)
        # resp_df.drop(axis=1, columns=['settings','memberships','customFields'], inplace=True)

        return resp_df
    
    except Exception as e: 
        print(str(e))
        return pd.DataFrame() 
