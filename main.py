from utils.utils import *
from utils.bigquery_utils import *
import utils.db as db
# from asana_sync import asana_data_pull

pull_date = current_date_time()


def update_clickup_spaces():
    pass

def update_clickup_list(all_spaces):
    pass

def update_clickup_tasks():
    try:
        ## Get updated tasks from clickup
        updated_tasks = fetch_updated_tasks_from_clickup()

        if len(updated_tasks):
            updated_tasks.drop_duplicates(inplace=True)
            clickup_list_to_update = updated_tasks['id'].values.tolist()

            ## Get clockify task IDs and project IDs against the updated clickup tasks
            mapping_df = get_clockify_task_mapping(clickup_list_to_update)
            push_to_watermark = []

            ## Hit API to update names on clockify
            for idx, row in updated_tasks.iterrows():

                clockify_elm = mapping_df[ mapping_df.clickup_task_id == row['id']].reset_index()
                if len(clockify_elm):
                    # import ipdb; ipdb.set_trace()
                    clockify_id = clockify_elm.loc[0, 'clockify_id']
                    clockify_project_id = clockify_elm.loc[0, 'clockify_project_id']
                    clockify_task_name = clockify_elm.loc[0, 'clockify_task_name']
                    

                    resp = update_task_name(clockify_project_id, clockify_id, row['id'], row['name'])
                    if resp:
                        response = resp.json()

                        new_dict = {
                            "clockify_id": clockify_id,
                            "old_value": clockify_task_name,
                            "new_value": response.get('name'),
                            "object_type": 'task',
                            "platform_update_date": row['date_updated'],
                            "platform_id": row['id'], ## Clickup task id
                            "platform_name": "clickup",
                            "pull_date": pull_date
                        }

                        push_to_watermark.append(new_dict)

            # Push updated records in watermark table if any updated records found 
            if len(push_to_watermark):
                # import ipdb; ipdb.set_trace()
                watermark_df = pd.DataFrame(push_to_watermark)
                df2gcp(watermark_df, db.WATERMARK, mode='append')
        else:
            print('No tasks updated in this iteration.')
    except Exception as e:
        print(str(e))

def main():
    print(datetime.now())
    
    # clients = clickup_spaces()
  
    # projects = clickup_list(clients)
    
    update_clickup_tasks()

  
main()