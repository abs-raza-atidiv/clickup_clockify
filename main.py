from utils.utils import *
from utils.bigquery_utils import *
import utils.db as db

def main():

    '''
    # CREATE NEW CLIENTS ON CLOCKIFY FROM CLICKUP SPACES
    # --------------------------------------------------- 
    spaces = get_clickup_spaces()
    # clockify_clients = get_clockify_clients()
    clockify_clients = get_clockify_clients_bq()

    # space_ids_in_clients = [x['note'] for x in clockify_clients] ## this one works when we fetch clikcup id from clockify client notes
    space_ids_in_clients = [x['id'] for x in clockify_clients]

    # Create CLIENTS for those SPACES from clickup which do not exist on clockify
    for elm in spaces:
        try:
            if elm['id'] not in space_ids_in_clients: 
                client_name = elm.get('name') 
                space_id = elm.get('id')
                
                # clockify_client_df = create_clockify_client(client_name, client_note= space_id)
                
                ## Insert data in database
                df2gcp(clockify_client_df, db.CLOCKIFY_CLIENT)

            else:
                print('client existed ', elm['name'])
            
        except Exception as err:
            print(str(err))


    # # Create PROJECTS on clockify for LISTS which do not exist on clockify
    # #------------------------------------------------------------------------ 

    clockify_clients = get_clockify_clients()
    space_client_mapping = get_space_client_mapping()

    clockify_projects = get_clockify_projects()
    list_ids_in_projects = [x['note'] for x in clockify_projects]

    import ipdb; ipdb.set_trace()

    for spc in spaces:
        clickup_list = get_clickup_lists(spc['id'])

        for elm in clickup_list:
            try:
                if elm['id'] not in list_ids_in_projects:
                    list_name = elm.get('name')
                    list_id = elm.get('id')
                    list_space_id = space_client_mapping[spc['id']]

                    create_clockify_projects(list_name, project_note = list_id, client_id= list_space_id )
                else:
                    print('Existing Project ', elm.get('name'), ' for Client ', spc['name'])

            except Exception as e:
                print(str(e))
        
        if len(clickup_list) == 0:
            print('NO LIST FOUND FOR THIS PROJECT')
    '''

    ''' GETTING ALL CLIKCUP TASKS IN DATAFRAME and UPLOAD TO BIGQUERY '''
    # clickup_task_df = fetch_all_clickup_tasks()

    # clickup_task_df = clickup_task_df[ clickup_task_df.id != '12ck3ph']
    # df2gcp(clickup_task_df, db.CLICKUP_TASK, mode = 'replace')



    ''' CREATE TASKS ON CLOCKIFY FROM BIGQUERY '''
    # # date_df = gcp2df('select max(pull_date) as pull_date from `productivity-377410.tickets_dataset.clickup_task`')
    # # max_datetime = str(date_df.values[0][0])
    ## ------------------------------------------------------------------------------

    # clickup_df = gcp2df("select id as clickup_task_id, name clikcup_task_name, list_id clickup_list_id, list_name as clickup_list_name \
    #      from `productivity-377410.tickets_dataset.clickup_task`")

    # # ## ------------------------------------------------------------------------------

    # clockify_projects = get_clockify_projects()
    # clockify_projects_lst = [
    #     {"clickup_id": x['note'], "clockify_project_id": x['id'], "clockify_project_name": x['name']} for x in clockify_projects
    # ]
    # project_df = pd.DataFrame(clockify_projects_lst)

    # # ## ------------------------------------------------------------------------------

    # # try:
    # #     # this is a double check being set to make sure we dont create a task twice on clockify  
    # #     clockify_bq_task_list = []
    # #     clockify_bq_task = gcp2df("select distinct clickup_task_id \
    # #         from `productivity-377410.tickets_dataset.clockify_task`").values.tolist()
    # #     clockify_bq_task_list = [x[0] for x in clockify_bq_task]
    # # except Exception as e: print(str(e))

    # # ## ------------------------------------------------------------------------------

    # import ipdb; ipdb.set_trace()

    # for idx, elm in clickup_df.iterrows():
        
    #     clk_project_id = project_df[ project_df.clickup_id == elm['clickup_list_id']]['clockify_project_id'].values[0]
    #     clk_project_name = project_df[ project_df.clickup_id == elm['clickup_list_id']]['clockify_project_name'].values[0]
        
    #     if clk_project_id: #and elm['clickup_task_id'] not in clockify_bq_task_list:
    #         print('CUP List {} <<>> {} CKFY Project. '.format(elm['clickup_list_name'], clk_project_name))


    #         create_clockify_task(clk_project_id, elm['clickup_task_id']+': '+elm['clikcup_task_name'], elm['clickup_list_id'], elm['clickup_task_id'])        
    
    # print('\n\nCREATED ALL TASKS FOR ALL PROJECTS \n\n')
    # import ipdb; ipdb.set_trace()

    # print('\n\n HOLDING COMMNAD LINE \n\n')


    ''' UPDATE CHILD TASKS '''
    
    child_df = gcp2df(" select tbl1.id as child_id, tbl1.parent as parent_id, tbl1.name as child_name, tbl2.name as parent_name \
                        from `productivity-377410.tickets_dataset.clickup_task` tbl1 \
                        left join `productivity-377410.tickets_dataset.clickup_task` tbl2 \
                        on tbl1.parent = tbl2.id \
                        where tbl1.parent is not null ")

    # # ## ------------------------------------------------------------------------------

    # clockify_projects = get_clockify_projects()
    # clockify_projects_lst = [
    #     {"clickup_id": x['note'], "clockify_project_id": x['id'], "clockify_project_name": x['name']} for x in clockify_projects
    # ]
    # project_df = pd.DataFrame(clockify_projects_lst)

    # # ## ------------------------------------------------------------------------------

    # # try:
    # #     # this is a double check being set to make sure we dont create a task twice on clockify  
    # #     clockify_bq_task_list = []
    # #     clockify_bq_task = gcp2df("select distinct clickup_task_id \
    # #         from `productivity-377410.tickets_dataset.clockify_task`").values.tolist()
    # #     clockify_bq_task_list = [x[0] for x in clockify_bq_task]
    # # except Exception as e: print(str(e))

    # # ## ------------------------------------------------------------------------------
    
    
    data = read_json_log('df_csv.json')

    df = pd.DataFrame(data)
    # clockify_task_ids = df['id'].values.tolist()

    import ipdb; ipdb.set_trace()

    for idx, elm in child_df.iterrows():
        # print(elm['id'])
        sub_df = df[ df.clickup_task_id == elm['child_id']]['id'].reset_index()

        # print(sub_df)
        # print(len(sub_df))
        if len(sub_df):
            # print(sub_df['id'][0])  #projectId
            task_id = sub_df['id'][0]
            project_id = df[ df.clickup_task_id == elm['child_id']]['projectId'].reset_index()
            project_id = project_id['projectId'][0]
            update_task_name(project_id, task_id, elm['parent_id'], elm['child_id'], elm['child_name'], elm['parent_name'])

    
    # for idx, elm in child_df.iterrows():
        
    #     clk_project_id = project_df[ project_df.clickup_id == elm['clickup_list_id']]['clockify_project_id'].values[0]
    #     clk_project_name = project_df[ project_df.clickup_id == elm['clickup_list_id']]['clockify_project_name'].values[0]
        
    #     if clk_project_id: #and elm['clickup_task_id'] not in clockify_bq_task_list:
    #         print('CUP List {} <<>> {} CKFY Project. '.format(elm['clickup_list_name'], clk_project_name))


    #         create_clockify_task(clk_project_id, elm['clickup_task_id']+': '+elm['clikcup_task_name'], elm['clickup_list_id'], elm['clickup_task_id'])        
    
    # print('\n\nCREATED ALL TASKS FOR ALL PROJECTS \n\n')
    # import ipdb; ipdb.set_trace()

    # print('\n\n HOLDING COMMNAD LINE \n\n')



main()