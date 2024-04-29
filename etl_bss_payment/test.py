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
from modules.module import *
from prefect.blocks.system import Secret
from prefect import flow, task
from prefect.task_runners import SequentialTaskRunner
from prefect_dbt_flow import dbt_flow
from prefect_dbt_flow.dbt import DbtProfile, DbtProject
from functools import lru_cache
from prefect.blocks.notifications import DiscordWebhook


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
    # Load Connection Log
    conn_log = vpluslib.connect_init(2,ssh=True)
    log_init = vpluslib.log_init(name_script=f'ETL BSS {cluster_name}',connection=conn_log,status_script='ETL')
except Exception as err:
    discord_webhook_block.notify(msg_notif_failed.format('Failed',err))
    raise(str(err) + ' Server Connection')

try:
    # Load Variables
    cluster_bss = cluster_name.lower()
    batch_row = 300000
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
                log_init.log('Check Tables Success',status_run='Success')
            except Exception as err:
                print('Error Grant Permission. ' + str(err))
                raise err
        except Exception as eror:
            discord_webhook_block.notify(msg_notif_failed.format('Failed',eror))
            log_init.log('Check Tables Failed',status_run='Failed',erors=eror)
            raise eror

    check_tables()

    # Push Git
    push_git_changes(git_path.get(), token_git.get())

    log_init.log('Load variable success',status_run='Success')
except Exception as eror:
    discord_webhook_block.notify(msg_notif_failed.format('Failed',eror))
    log_init.log('Load Variable Failed',erors=eror,status_run='Failed')
    raise eror

# Prefect Functions
@task(name='Check Connections',log_prints=True)
def check_connection():
    try:
        s3_1 = vpluslib.connect_init(conn_s3)
        conn_source_paytv = vpluslib.connect_init(conn_source)
        conn_target_ = vpluslib.connect_init(conn_target)
        log_init.log('Check Connections Success',status_run='Success')
    except Exception as eror:
        discord_webhook_block.notify(msg_notif_failed.format('Failed',eror))
        log_init.log('Check Connections Failed',status_run='Failed',erors=eror)
        raise eror

@task(name='Extract Data',log_prints=True)
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
        trunc_table(schema_table=schema_target,name_table=select_query,conn_target=conn_target,conn_log=log_init)
        get_new_columns(conn_source_num=conn_source,conn_target_num=conn_target,query_all_columns=query_all_col,
                        query_add_columns=query_col_add,table_source_name=select_table_name,table_target_name=select_query)
        
        batch_count = batch_counts(query_init=query_req,conn_source=conn_psql,batch_size=batch)
        query_text = query_generator(table_name=select_table_name,query_all_columns=query_all_col,conn_number=conn_number,schema_source=schema_source)
        
        while num<= batch_count:
            num,offset,total_insert = batch_read(log_init=log_init,
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
        log_init.log(f'Extract data {select_query} Success',status_run='Success')
        return total_insert
    except Exception as eror:
        discord_webhook_block.notify(msg_notif_failed.format('Failed',eror))
        log_init.log(f'Extract data {select_query} Failed',erors=eror,status_run='Failed')
        raise eror
    
@task(name='Load data',log_prints=True)
def execute_query(name_table,query_req,number_conn,path_s3,schema):
    try:
        connection = vpluslib.connect_init(number_conn)
        connection_cur = connection.cursor()
        connection_cur.execute(query_req.format(schema,name_table,path_s3,creds_iam_aws.get()))
        connection.commit()
        log_init.log(f'Load data {name_table} Success',status_run='Success')
    except Exception as eror:
        discord_webhook_block.notify(msg_notif_failed.format('Failed',eror))
        log_init.log(f'Load data {name_table} Failed',erors=eror,status_run='Failed')
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

@flow(name=flow_name,log_prints=True)
def etl_data_payment(total_insert_data=total_insert_data):
    
    # Check Connection
    check_connection()

    # Extract Data
    query_list = get_tables_name(con_number=conn_source,query_all_tables=query_all_table) 
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
    
    
    # Modeling data from raw schema to analytics schema
    try:
        modeling_dbt()
        discord_webhook_block.notify(msg_notif_success.format('Success',total_insert_data))
    except Exception as eror:
        discord_webhook_block.notify(msg_notif_failed.format('Failed',eror))
        log_init.log(f'Modeling data Failed',erors=eror,status_run='Failed')
        raise eror

if __name__ == "__main__":
    etl_data_payment()
    


import connectorx as cx
import pandas as pd
import vpluslib
import yaml
import os
import subprocess
from functools import lru_cache
from io import StringIO
import time

def query_delete_order(query_init):
    query = query_init.lower()
    order_by_index = query.find('order by')

    if order_by_index != -1:
        modified_query = query[:order_by_index]
    else:
        print("No 'ORDER BY' clause found in the query.")
    return modified_query

def batch_counts(query_init,conn_source,batch_size):
    query_count = (query_delete_order(query_init)).format('count (*) as total') 
    df = cx.read_sql(conn_source,query=query_count,return_type='pandas')
    total_row = int(df['total'].iloc[0])
    batch_count = int(round(total_row/batch_size,0))
    return batch_count 

def trunc_table(schema_table,name_table,conn_target,conn_log):
    try:
        trunc_query = f'''truncate table {schema_table}.{name_table}'''
        conn_target = vpluslib.connect_init(conn_target)
        conn_target.cursor().execute(trunc_query)
        conn_target.commit()
        conn_target.close()    
        conn_log.log(f'truncate table {name_table} success',status_run='Success')
    except Exception as eror:
        if 'Query cancelled' in str(eror):
            trunc_query = f'''truncate table {schema_table}.{name_table}'''
            conn_target = vpluslib.connect_init(conn_target)
            conn_target.cursor().execute(trunc_query)
            conn_target.commit()  
            conn_target.close()     
            conn_log.log(f'truncate table {name_table} success',status_run='Success')
        else:
            conn_log.log(f'truncate table {name_table} failed',status_run='Failed',erors=eror)
            raise eror

def delete_create_folder_s3(s3,bucket_name,folder_name):
    objects = s3.list_objects_v2(Bucket=bucket_name, Prefix=folder_name)
    if 'Contents' in objects:
        for obj in objects['Contents']:
            s3.delete_object(Bucket=bucket_name, Key=obj['Key'])
            print(f"Deleted file '{obj['Key']}'")
        print(f"All files in folder '{folder_name}' have been deleted.")
    else:
        print(f"No files found in folder '{folder_name}'.")
    
    s3.put_object(Bucket=bucket_name, Key=(folder_name + '/'))
    print(f"Folder '{folder_name}' created in bucket '{bucket_name}'")
    
def delete_prefix_word(input_string,conditional_word):
    underscore_index = input_string.find('_')
    if underscore_index != -1 and conditional_word in str(input_string):  # If underscore is found
        result_string = input_string[underscore_index + 1:]
    else:
        result_string = input_string
    return result_string

def query_generator(table_name,query_all_columns,conn_number,schema_source):
    col_num_fix = []
    col_tgl_fix = []
    query_source = '''
    select * from {0}
    limit 0
    '''.format(table_name)
    conn = vpluslib.connect_init(conn_number,get_url=True)
    df_col = cx.read_sql(conn=conn,query=query_all_columns.format(table_name=table_name,table_schema=schema_source),return_type='pandas')
    df_source = cx.read_sql(conn=conn,query=query_source,return_type='pandas')

    col_source = df_source.columns.tolist()
    col_num = df_col[(df_col['data_type'].str.contains('int'))]['column_name'].tolist()
    col_time = df_col[df_col['data_type'].str.contains('timestamp')]['column_name'].tolist()
    col_non_num_prep = df_col[~df_col['data_type'].str.contains('int')]
    col_non_num = col_non_num_prep[~col_non_num_prep['data_type'].str.contains('timestamp')]['column_name'].tolist()

    if len(col_num)>0:
        for select_col in col_num:
            var_col = f'cast({select_col} as varchar) as {select_col}'
            col_num_fix.append(var_col)
    else:
        pass
    
    # Hours added +7
    if len(col_time)>0:
        for select_col in col_time:
            var_col = f"{select_col}+interval '7 hour' as {select_col}"
            col_tgl_fix.append(var_col)
    else:
        pass


    col_num_list = col_num_fix + col_non_num + col_tgl_fix
    
    
    indices_dict = {value: index for index, value in enumerate(col_source)}
    sorted_col_num_list = sorted(col_num_list, key=lambda x: indices_dict.get(x.split()[-1], float('inf')))
    result_string = ','.join(sorted_col_num_list)
    return result_string

def get_filters_query(table_name,query_all_columns,conn_number,schema_source):
    col_tgl_fix = []
    conn_psql = vpluslib.connect_init(conn_number, get_url=True)
    df_fil = cx.read_sql(conn=conn_psql,query=query_all_columns.format(table_name=table_name,table_schema=schema_source),return_type='pandas')
    
    col_time = df_fil[(df_fil['data_type'].str.contains('timestamp')) & (df_fil['column_name'].str.contains('created') | df_fil['column_name'].str.contains('updated'))]['column_name'].tolist()
    if len(col_time)>0:
        for select_col in col_time:
            var_col = f"(date({select_col}+interval '7 hour') >= date(now())-1)"
            col_tgl_fix.append(var_col)
    else:
        pass
    
    result_string = ' or '.join(col_tgl_fix)
    return result_string

def get_tables_name(con_number,query_all_tables):
    conn_psql = vpluslib.connect_init(con_number, get_url=True)
    df = cx.read_sql(conn=conn_psql,query=query_all_tables,return_type='pandas')
    all_tables = df['table_name'].tolist()
    return all_tables

def get_pk_column(con_number,query_pk_col,table_name):
    conn_psql = vpluslib.connect_init(con_number, get_url=True)
    pk_table = cx.read_sql(conn=conn_psql,query=query_pk_col.format(table_name),return_type='pandas')
    pk_name = str(pk_table['column_name'].iloc[0])
    return pk_name

def get_new_table(conn_source_num, conn_target_num, cluster_name, schema_raw_data, schema_source, query_model, output_path, cluster_bss):
    def check_schema(conn, schema, name):
        query = f""" SELECT table_name as {name}
                    FROM information_schema.tables
                    WHERE table_schema = '{schema}' and table_name != 'schema_migrations'
                """

        df = cx.read_sql(conn, query, return_type='pandas')

        return df
    
    def get_query_ddl(conn, table_name, schema):
        query = f""" SELECT column_name, data_type, ordinal_position
                        FROM information_schema.columns
                        WHERE table_name = '{table_name}' and table_schema ='{schema}'
                    """

        df = cx.read_sql(conn, query, return_type='pandas')

        return df
    
    # Connect
    conn_source = vpluslib.connect_init(conn_source_num, get_url=True)
    conn_target = vpluslib.connect_init(conn_target_num, get_url=True)
    conn_target_db = vpluslib.connect_init(conn_target_num)

    #
    df_source = check_schema(conn_source, schema_source, 'source_name')
    df_target = check_schema(conn_target, schema_raw_data, 'target_name')

    # 
    original_list = []
    for word in df_source['source_name']:
        word_clean = delete_prefix_word(input_string = word, conditional_word = cluster_name)
        original_list.append(word_clean)
        
    prefix = 'bss_' + cluster_name + '_'
    prefixed_list = [prefix + value for value in original_list]
    
    df_source['source_name_vtwo'] = prefixed_list
    df_check = pd.concat([df_source,df_target], axis=1)
    
    # Check Missing Table
    missing_rows = df_check.loc[df_check['source_name_vtwo'].notna() & ~df_check['source_name_vtwo'].isin(df_check['target_name']), 'source_name_vtwo']
    missing_source_names = [df_check.loc[df_check['source_name_vtwo'] == row, 'source_name'].iloc[0] for row in missing_rows if df_check.loc[df_check['source_name_vtwo'] == row, 'source_name'].iloc[0] is not None]

    log_msg = 'Table Source & Target is fully completed'
    
    # Stopper
    if len(missing_source_names) >= 1:
        for miss, misa in zip(missing_source_names, missing_rows):
            df_conn = get_query_ddl(conn_source, miss, 'public')
            
            schema = schema_raw_data
            table_name = misa

            def clean_data_type(data_type):
                if data_type == 'character varying':
                    return 'varchar(10000)'
                elif data_type == 'timestamp with time zone':
                    return 'timestamp'
                elif data_type in ['int', 'int8', 'smallint', 'serial', 'bigserial','integer']:
                    return 'bigint'
                elif data_type in ['numeric', 'decimal']:
                    return 'float'
                elif 'json' in data_type:
                    return 'varchar(20000)'
                elif data_type == 'date':
                    return 'date'
                elif data_type == 'USER-DEFINED':
                    return 'varchar(20000)'
                elif data_type == 'boolean':
                    return 'varchar(20000)'
                else:
                    return data_type

            def get_column_definition(row):
                column_name = row['column_name']
                data_type = row['data_type']
                cleaned_data_type = clean_data_type(data_type)
                return f'"{column_name}" {cleaned_data_type}'

            column_names_set = set()
            column_definitions_list = []

            for index, row in df_conn.iterrows():
                column_name = row['column_name']
                if column_name not in column_names_set:
                    column_names_set.add(column_name)
                    column_definitions_list.append(get_column_definition(row))

            # Construct the CREATE TABLE statement
            create_table_statement = 'CREATE TABLE {0}.{1} ({2});'.format(
                schema,
                table_name,
                ", ".join(column_definitions_list)
            )

            print(create_table_statement)
            
            conn_target_db.cursor().execute(create_table_statement)
            conn_target_db.commit()   
            
            log_msg = f'Create NEW table on {schema}.{table_name}'
            print(log_msg)

            # Create SQL DBT File
            generate_sql_modeling(table_name=misa, script_template=query_model, output_path=output_path)
    
        # Create Yaml Files
        generate_yaml_source(conn_target_num, cluster_bss, output_path)
        
    return log_msg

def get_new_columns(conn_source_num,conn_target_num,query_all_columns,query_add_columns,table_source_name,table_target_name,table_source_schema='public',table_target_schema='raw_data',schema_production='analytics'): 
    conn_psql = vpluslib.connect_init(conn_source_num,get_url=True)
    conn_redshift_url = vpluslib.connect_init(conn_target_num,get_url=True)
    conn_redshift = vpluslib.connect_init(conn_target_num)

    df_source = cx.read_sql(conn=conn_psql,query=query_all_columns.format(table_name=table_source_name,table_schema=table_source_schema),return_type='pandas')
    df_target = cx.read_sql(conn=conn_redshift_url,query=query_all_columns.format(table_name=table_target_name,table_schema=table_target_schema),return_type='pandas')
    df_target.drop(['data_type','ordinal_position'],axis=1,inplace=True)
    df_target['target'] = True

    if len(df_source)!=len(df_target):
        df_source.columns = df_source.columns.str.lower()
        result = pd.merge(df_source,df_target,on='column_name',how='left')
        result.sort_values('ordinal_position',ascending=True,inplace=True)
        result_values = result[result['target'].isnull()]['column_name'].tolist()
        result_values_type = result[result['target'].isnull()]['data_type'].tolist()
        for value,types in zip(result_values,result_values_type):
            if 'int' in str(types):
                result_types = f'{value} bigint'
            elif 'numeric' in str(types) or 'float' in str(types):
                result_types = f'{value} float8'
            elif 'serial' in str(types) or 'float' in str(types):
                result_types = f'{value} bigint'
            elif 'timestamp' in str(types):
                result_types = f'{value} timestamp'
            elif 'date'==str(types):
                result_types = f'{value} date'
            else:
                result_types = f'{value} varchar(20000)'
            try:
                conn_redshift.cursor().execute(query_add_columns.format(table_schema = table_target_schema, table_name = table_target_name, add_on_query = result_types))
                conn_redshift.cursor().execute(query_add_columns.format(table_schema = schema_production, table_name = table_target_name, add_on_query = result_types))
                conn_redshift.commit()
            except Exception as eror:
                if 'Query' in str(eror):
                    conn_redshift = vpluslib.connect_init(conn_target_num)
                    conn_redshift.cursor().execute(query_add_columns.format(table_schema = table_target_schema, table_name = table_target_name, add_on_query = result_types))
                    conn_redshift.cursor().execute(query_add_columns.format(table_schema = schema_production, table_name = table_target_name, add_on_query = result_types))
                    conn_redshift.commit()
                else:
                    raise eror
            print(f'New Columns : {value} with type: {types}')
    else:
        print(f'All Columns on {table_target_name} Match')
        pass
    conn_redshift.close()

def post_grant_db(conn_target_num,query_permission):
    conn_target_db = vpluslib.connect_init(conn_target_num)
    for query in query_permission:
        if query.strip():
            conn_target_db.cursor().execute(query)
            conn_target_db.commit()
        else:
            pass

def generate_yaml_source(conn_target_num,cluster_bss,path_modeling):
    query_get_tables = f'''
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = 'raw_data' and table_name like 'bss_{cluster_bss}%'
    '''
    list_table_name = get_tables_name(conn_target_num,query_get_tables)
    sources_data = [
        {
            'name': 'rawdata_users',
            'database': 'vplus_db_premium',
            'schema': 'raw_data',
            'table_names': list_table_name
        }
    ]
    for source in sources_data:
        source['tables'] = [{'name': table_name} for table_name in source.pop('table_names')]
    data =  {'version': 2, 'sources': sources_data}

    # Convert the data to YAML format
    formatted_yaml = yaml.dump(data, default_flow_style=False)

    # Write formatted YAML content to a new file
    with open(os.path.join(path_modeling,'source.yml'), 'w') as output_file:
        output_file.write(formatted_yaml)
    return print('Create File source.yaml on folder Modeling')

def generate_sql_modeling(table_name, script_template, output_path):       
    sql_content = script_template.format(table_name=table_name)
    file_path = os.path.join(output_path, f"{table_name}.sql")

    with open(file_path, "w") as sql_file:
        sql_file.write(sql_content)

    print(f"SQL file for {table_name} generated successfully!")

def push_git_changes(path, token):
    print('GIT PATH: ' + path)

    command_add = ['git', 'add', '.']
    subprocess.run(command_add)
    
    # Git commit
    command_commit = ['git', 'commit', '-m', 'Success push updated on git']
    subprocess.run(command_commit)
    
    # Set the remote URL with the token
    remote_name = 'origin'  # Assuming 'origin' is the correct remote name
    remote_url = f'https://{token}@github.com/V-Analystic/{path}'
    command_set_remote = ['git', 'remote', 'set-url', remote_name, remote_url]
    subprocess.run(command_set_remote)
    
    # Git push
    command_push = ['git', 'push', remote_name]
    subprocess.run(command_push)

    try:
        subprocess.run(command_push, check=True)
        print("Push successful.")
    except subprocess.CalledProcessError as e:
        print(f"Error: {e}")


@lru_cache(maxsize=None)
def batch_read(log_init,totalrow,conn_psql,s3_conn,path_s3,bucket_internal,offset,num,query_fetch_data,batch,query_name):
    try:
        try:
            df = cx.read_sql(conn=conn_psql,query=query_fetch_data,return_type='pandas')
        except Exception as eror:
            if 'connection closed' in str(eror).lower() or 'runtime' in str(eror).lower():
                time.sleep(10)
                df = cx.read_sql(conn=conn_psql,query=query_fetch_data,return_type='pandas')
            else:
                raise eror
        
        print(len(df))
        totalrow+=len(df)
        offset+=batch
        num+=1
        csv_buffer = StringIO()
        df.to_csv(csv_buffer,index=False)
        s3_conn.put_object(Body=csv_buffer.getvalue(), Bucket=bucket_internal, Key=path_s3)
    except Exception as eror:
        log_init.log(f'Extract partition data {query_name} failed',status_run='Failed',erors=eror)
        raise eror
    return num,offset,totalrow


