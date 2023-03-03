from utils.utils import *
import utils.asana_utils as asana
from utils.bigquery_utils import *
import utils.db as db

pull_date = current_date_time()

def main():
    try: 
        
        import ipdb; ipdb.set_trace()
        project_id_1 = '63f771cd62887f06044e4520'
        list_1 = get_clockify_tasks(project_id_1)

        for elm in list_1:
            delete_clockify_task(project_id_1, elm['id'])

        import ipdb; ipdb.set_trace()

        project_id_2 = '63f77295a561315041e6e43c'
        list_2 = get_clockify_tasks(project_id_2)


        for elm in list_2:
            delete_clockify_task(project_id_2, elm['id'])

    
    except Exception as e: 
        print(str(e))
    

main()