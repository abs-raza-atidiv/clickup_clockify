from utils.utils import *
from utils.bigquery_utils import *
import utils.db as db

def split_ids(df_req):
    slen = 0
    ctr = 0
    rerun = True
    df = df_req.reset_index()
    df_trim = df[df.clickup_task_id == 'undefined']
    df_good = df[df.clickup_task_id != 'undefined']
    
    while rerun:
        ctr = 0
        # import ipdb; ipdb.set_trace()
        print('rerun for {} records'.format(len(df_trim)))
        for idx, elm in df_trim.iterrows():
            if elm['name'][slen] == ':':
               df_trim['clickup_task_id'][idx] = elm['name'].split(':')[0]
               ctr += 1
            if elm['name'][slen] == '-':
               df_trim['clickup_task_id'][idx] = elm['name'].split('-')[0]
               ctr += 1
            # print(task_df['clickup_task_id'])

        slen += 1
        # import ipdb; ipdb.set_trace()

        df_new = df_trim[df_trim.clickup_task_id != 'undefined']
        df_trim = df_trim[df_trim.clickup_task_id == 'undefined']
        df_good=pd.concat([df_good, df_new])

        if len(df_trim[df_trim.clickup_task_id == 'undefined']) == 0:
            rerun = False

    return df_good
    # master_df = pd.concat([master_df, df])



def main():

    import ipdb; ipdb.set_trace()

    df = get_all_clockify_tasks()

    # df_list = df[df.clickup_task_id != 'undefined']['clickup_task_id'].values.tolist()

    df_fix = split_ids(df)

    df2gcp(df_fix, db.CLOCKIFY_TASK, mode='replace')

main()