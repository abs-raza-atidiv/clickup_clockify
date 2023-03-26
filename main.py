from utils.utils import *
from utils.bigquery_utils import *
import utils.db as db
# from asana_sync import asana_data_pull

pull_date = current_date_time()


def main():
    print(datetime.now())
    
    ## Get dataframe of users
    users = get_clockify_users()

    old_users = get_table_data(db.CLOCKIFY_USER)
    all_users_id = old_users['id'].values.tolist()
    
    new_users = create_snapshot(new_df=users, old_df=old_users)
    
    if len(new_users): 
        prep_and_dump_new_data_in_bq(new_users, db.CLOCKIFY_USER)
        
        ## If new users exist, add their ids to all_users list
        all_users_id += new_users['id'].values.tolist()

    ##----------------------------------------------------------------------------------
    ## TIMES ENTRIES 
    ##----------------------------------------------------------------------------------
    timeentries = get_clockify_timeentries(all_users_id)

    old_data = get_table_data(db.CLOCKIFY_TIMESHEET)
    
    new_data = create_snapshot(new_df=timeentries, old_df=old_data)

    if len(new_data): 
        prep_and_dump_new_data_in_bq(new_data, db.CLOCKIFY_TIMESHEET)


main()