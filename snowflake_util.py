import os
import configuration as cfg
import pickle
import subprocess
import snowflake.connector
from tqdm import tqdm
import logging
import time


logging.getLogger('snowflake.connector').setLevel(logging.WARNING)

class Snowflakes(object):
    def __init__(self) -> None:
     self.cur =  self.make_connect()

    def make_connect(self):
        conn = snowflake.connector.connect(
                               user=os.environ['azure_object_id'],
                               account='FLIX-DWH', 
                               authenticator= 'externalbrowser',
                               warehouse= 'PROMETHEUS_WH',
                               database='PROMETHEUS_DB',
                               schema='FAH',
                               role='PROMETHEUS_ADMIN',
                               #socket_timeout=300
                            ) 

        return conn.cursor()
    
    def close_connection(self):
        self.cur.close()


    def upload_data(self):
        # load list of extracted files into a list object
        dict_target_files = self.load_pickle_file()
        main_folder_name = dict_target_files['main_folder']
        for folder_name, file_names_dic in  tqdm(dict_target_files['items'].items()):
            start = time.time()
            for f_name, _ in tqdm(file_names_dic.items(), leave=False):
                self.load_file_into_snowflake(os.path.join(cfg.sys_path['target_path'], main_folder_name, folder_name, f"{f_name.split(".")[0]}.csv")
                                        , folder_name
                                        , clear_staging=True)
                time.sleep(0.01)
                print(f"\n{f_name} is successfuly staged and copied to {folder_name} table!")
            now = time.time()
            print(f"\n{folder_name} process is completed sucessfuly. The overall time is : {now-start} seconds\n")
             
        self.close_connection()
        print(f"The conncetion is closed! Done")

    def filter_uploaded_files(self, ls):
        filterd = {}
        skipped_files =  cfg.exceptions["file_names"]
        for key, val in ls['items'].items():
            if key not in skipped_files:
                filterd[key] = val
            else:
                logging.info(f"{key} is skipped to upload due to the previous action")
        ls['items'] = filterd
        return ls
                
    
    def load_pickle_file(self):
        with open(os.path.join(cfg.sys_path["target_path"], cfg.sys_path[ "pickle_target_name"]), 'rb') as reader:
            final_list = self.filter_uploaded_files(pickle.load(reader))
            return  final_list

    def check_table_exist(self, folder_name):
        self.cur.execute(f"SHOW TABLES LIKE '{folder_name}_STG';")
        result = self.cur.fetchone()
        return True if result else False
           
 

    def load_file_into_snowflake(self, file_path, folder_name, clear_staging):
        
        if os.path.isfile(file_path):
            print(f"\nLoading file into Snowflake: {file_path}")
    
            # Truncate staging table if clear_staging is True
            if clear_staging:
                #snowsql_commands.append(f"TRUNCATE TABLE {folder_name}_STG;")
                if self.check_table_exist(folder_name):
                    self.cur.execute(f"TRUNCATE TABLE {folder_name}_STG")
            
           # self.cur.execute(f"create or replace table {folder_name}_STG(a int, b string)")
            self.cur.execute(f"CREATE OR REPLACE STAGE {folder_name}_STG;")
            self.cur.execute(f'PUT file://{file_path} @{folder_name}_STG;')

            main_table_name = folder_name.replace("_STG", "")
            self.cur.execute(f"COPY INTO {main_table_name} FROM @{folder_name}_STG FILE_FORMAT=(FORMAT_NAME = PROMETHEUS_DB.FAH.My_CSV_FORMAT) ON_ERROR = 'SKIP_FILE' ")

            #self.cur.execute(f"REMOVE @{folder_name}_STG;")
            
            










