# Program Init
import os
import sys

current_script_path = os.path.abspath(__file__)
main_folder = os.path.dirname(current_script_path)
parent_path = os.path.dirname(main_folder)
model_dir = os.path.join(main_folder, 'modeling', 'models')
query_dir = os.path.join(parent_path, 'general_query')

sys.path.insert(0,parent_path)

# Library
import vpluslib
import psutil
from modules.module import *
from prefect.blocks.system import Secret
from prefect import flow, task
from prefect.task_runners import SequentialTaskRunner,ConcurrentTaskRunner
from prefect_dbt_flow import dbt_flow
from prefect_dbt_flow.dbt import DbtProfile, DbtProject
from functools import lru_cache
from prefect.blocks.notifications import DiscordWebhook
from io import StringIO

unlimited_segment()

# Load Variables
cluster_name = 'Payment'

# Notifications
discord_webhook_block = DiscordWebhook.load("notif-vplus-success")
# Msg notif 
msg_notif_failed = f'ETL BSS {cluster_name} '+'{0}; error: {1} | @everyone '
msg_notif_success = f'ETL BSS {cluster_name} '+'{0}; total row: {1}'


# Connection number 
conn_target = 4
conn_source = 29
conn_s3 = 10
    

try:
    # Load Variables
    cluster_bss = cluster_name.lower()
    batch_row = 100000
    cond_word = cluster_bss
    prefix_name = f'bss_{cluster_bss}_'
    total_insert_data = 0
    flow_name = f'ETL BSS {cluster_name}'
    name_flow_modeling = f"Transform Data BSS {cluster_name}"


    # Base Query
    query_copy_command = 'query_copy_command.txt'
    query_all_columns = 'query_all_columns.txt'
    query_fetch = 'query_fetch_data.txt'
    query_all_tables = 'query_all_tables.txt'
    query_base = 'query_base.txt'
    query_primary = 'query_primary_key.txt'
    query_add_col = 'query_add_columns.txt'
    query_cc = 'query_copy_command.txt'
    query_grant_dir = 'query_grant.txt'
    query_model_dir = 'query_base_modeling.txt'

    # Table variables
    schema_raw_data = 'raw_data'
    schema_source = 'public'

    #S3
    bucket_internal = 'visionplus-etl-datateam'
    path_s3 = 'bss_' + cluster_bss + '/{0}'
    target_name = 'bss_' + cluster_bss + '/{0}'

    # Load base query
    with open(os.path.join(query_dir,query_all_tables), 'r') as file:
        query_all_table = file.read().replace('\n',' ').replace('\t', '        ')
    with open(os.path.join(query_dir,query_all_columns), 'r') as file:
        query_all_col = file.read().replace('\n',' ').replace('\t', '        ')
    with open(os.path.join(query_dir,query_fetch), 'r') as file:
        query_fetch_data = file.read().replace('\n',' ').replace('\t', '        ')
    with open(os.path.join(query_dir,query_base), 'r') as file:
        query_base_main = file.read().replace('\n',' ').replace('\t', '        ')
    with open(os.path.join(query_dir,query_primary), 'r') as file:
        query_primary_key = file.read().replace('\n',' ').replace('\t', '        ')
    with open(os.path.join(query_dir,query_add_col), 'r') as file:
        query_col_add = file.read().replace('\n',' ').replace('\t', '        ')
    with open(os.path.join(query_dir,query_cc), 'r') as file:
        query_req = file.read().replace('\n',' ').replace('\t', '        ')
    with open(os.path.join(query_dir,query_grant_dir), 'r') as file:
        query_grant = file.read().split(';')
    with open(os.path.join(query_dir,query_model_dir), 'r') as file:
        query_model = file.read()

    # Blocks
    profs_dbt = Secret.load("profiles-dbt")
    creds_iam_aws = Secret.load("credential-iam-aws")
    git_path = Secret.load("git-path-bss")
    user_git = Secret.load("user-git")
    token_git = Secret.load("token-git")
    
    # Check New Tables
    def check_tables():
        try:
            # Check New Table / New Columns
            get_new_table(conn_source, conn_target, cluster_bss, schema_raw_data, schema_source, query_model, model_dir, cluster_bss)
            try:
                post_grant_db(conn_target, query_grant)
                print('Success Grant Permission')
            except Exception as err:
                print('Error Grant Permission. ' + str(err))
                raise err
        except Exception as eror:
            discord_webhook_block.notify(msg_notif_failed.format('Failed',eror))
            raise eror

    check_tables()

    # Push Git
    push_git_changes(git_path.get(), token_git.get())

except Exception as eror:
    discord_webhook_block.notify(msg_notif_failed.format('Failed',eror))
    raise eror

@task(name='Extract Data',log_prints=True,cache_result_in_memory=False)
def extract_data(select_table_name,schema_source,schema_target,bucket_name,conn_number,path_s3):
    try:
        offset = 0
        num = 0
        batch = batch_row

        total_insert = 0
        s3_1 = vpluslib.connect_init(conn_s3)
        conn_psql = vpluslib.connect_init(conn_number, get_url=True)
        pk_name = get_pk_column(con_number=conn_number,query_pk_col=query_primary_key,table_name=select_table_name)
        excel_file = select_table_name+'{0}.csv'
        filter_query = get_filters_query(table_name=select_table_name,query_all_columns=query_all_col,conn_number=conn_number,schema_source=schema_source)
        query_req = query_base_main.format(main='{0}',table_name=select_table_name,pk=pk_name,filters=filter_query)
        select_table = delete_prefix_word(input_string=select_table_name,conditional_word=cond_word)
        select_query = prefix_name+select_table
        delete_create_folder_s3(s3=s3_1,bucket_name=bucket_internal,folder_name=path_s3.format(select_query))
        trunc_table(schema_table=schema_target,name_table=select_query,conn_target=conn_target)
        get_new_columns(conn_source_num=conn_source,conn_target_num=conn_target,query_all_columns=query_all_col,
                        query_add_columns=query_col_add,table_source_name=select_table_name,table_target_name=select_query)
        
        batch_count = batch_counts(query_init=query_req,conn_source=conn_psql,batch_size=batch)
        query_text = query_generator(table_name=select_table_name,query_all_columns=query_all_col,conn_number=conn_number,schema_source=schema_source)
        
        while num<= batch_count:
            csv_buffer = StringIO()
            num,offset,total_insert = batch_read(
                                    buffer = csv_buffer,
                                    totalrow=total_insert,
                                    query_name = select_query,
                                    offset = offset,
                                    num=num,
                                    s3_conn=s3_1,
                                    path_s3=path_s3.format(select_query)+'/'+excel_file.format(num),
                                    bucket_internal=bucket_internal,
                                    conn_psql=conn_psql,
                                    batch=batch,
                                    query_fetch_data=query_fetch_data.format(main_query=(query_req.format(query_text)),ofc = offset,batch_size=batch))
            batch_read.cache_clear()
            clear_cache()
            csv_buffer.seek(0)
            csv_buffer.truncate(0)
            csv_buffer.seek(0)
            time.sleep(5)
        return total_insert
    except Exception as eror:
        discord_webhook_block.notify(msg_notif_failed.format('Failed',eror))
        raise eror
    
@task(name='Load data',log_prints=True,cache_result_in_memory=False)
def execute_query(name_table,query_req,number_conn,path_s3,schema):
    try:
        connection = vpluslib.connect_init(number_conn)
        connection_cur = connection.cursor()
        connection_cur.execute(query_req.format(schema,name_table,path_s3,creds_iam_aws.get()))
        connection.commit()
        connection.close()
        clear_cache()
    except Exception as eror:
        discord_webhook_block.notify(msg_notif_failed.format('Failed',eror))
        raise eror

# Execute modeling on dbt
modeling_dbt=dbt_flow(
    project=DbtProject(
        name=name_flow_modeling,
        project_dir=os.path.join(main_folder,'modeling'),
        profiles_dir =str(profs_dbt.get()),
    ),
    profile=DbtProfile(
        target="analytics",
    ),
    flow_kwargs={
        "task_runner": SequentialTaskRunner(),
    },
)

@flow(name=flow_name,log_prints=True,cache_result_in_memory=False, task_runner=ConcurrentTaskRunner())
def etl_data_payment(total_insert_data=total_insert_data):
    # Extract Data
    query_list = get_tables_name(con_number=conn_source,query_all_tables=query_all_table) 
    query_list = query_list[::-1]
    for selection in query_list:
        total_row_data = extract_data(select_table_name=selection,
                     bucket_name=bucket_internal,
                     conn_number=conn_source,
                     path_s3=path_s3,
                     schema_source=schema_source,
                     schema_target=schema_raw_data
                     )
        total_insert_data+=total_row_data
        new_selection = delete_prefix_word(input_string=selection,conditional_word=cond_word)
        execute_query(name_table=prefix_name+new_selection,
                      schema = schema_raw_data,
                      path_s3=path_s3.format(f'{prefix_name}{new_selection}'),
                      query_req=query_req,
                      number_conn=conn_target                     
                      )
        time.sleep(3)
    
    # Modeling data from raw schema to analytics schema
    try:
        modeling_dbt()
        discord_webhook_block.notify(msg_notif_success.format('Success',total_insert_data))
    except Exception as eror:
        discord_webhook_block.notify(msg_notif_failed.format('Failed',eror))
        raise eror

if __name__ == "__main__":
    etl_data_payment()
    