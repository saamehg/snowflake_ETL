import os
import zipfile
import configuration as cfg
import logging
import sys
from tabulate import tabulate
from tqdm import tqdm
import pickle

terminal_size = os.get_terminal_size()
logging.basicConfig(
    level=logging.DEBUG,
    format="[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d] %(message)s",
    datefmt="%d/%b/%Y %H:%M:%S",
    stream=sys.stdout)

def write_to_file(target_path_list):
    with open(os.path.join(cfg.sys_path["target_path"], cfg.sys_path["pickle_target_name"] ), "wb" ) as writer:
        pickle.dump(target_path_list, writer)

#create report/name of the files, file row count&sum
def print_report(report_dic):
    for key, values in report_dic.items():
        file_names = list(values.keys())
        file_names.append("sum")

        num_rows = list(values.values())
        num_rows.append(sum(num_rows))
        table = [file_names,num_rows]
        
        print(f"\n{key}")
        print(tabulate(table, headers='firstrow', tablefmt='fancy_grid'))

# count number of rows of wach files
def get_file_rows(f_path):
    with open(f_path, 'rb') as f:
       return len(f.readlines())-1
    
#extract zip files and save them in a new path
def extract_file(source_path,target_path, file_name):
    try:
        zipdata = zipfile.ZipFile(source_path)
        zipinfos = zipdata.infolist()
        for zipinfo in zipinfos:
            zipinfo.filename = file_name
            zipdata.extract(zipinfo, path=target_path)
    except Exception as e:
        logging.error(e)
   
#get the list of zip files in each folder
def get_list_file_names(file_path):
    only_zipfiles = [f for f in os.listdir(file_path) if zipfile.is_zipfile(os.path.join(file_path,f))]
    return only_zipfiles

#get the name of the folders inside the main folder
def get_list_folder_names(folder_path):
    list_of_folders=[]
    for folder_name in os.listdir(folder_path):
        if os.path.isdir(os.path.join(folder_path,folder_name)):
            list_of_folders.append(folder_name)

    return list_of_folders

# unzip files, rename them and save them in a new pre-defined path, 
# count the row numbers and show the sum of rows of all csv files in a folder.
def run_unzip_pipeline():
    source_path = cfg.sys_path["source_path"]
    parent_folder_list = get_list_folder_names(source_path)

    _dic = {}
    items = {}
    target_path_list = {}

    for folder_name in parent_folder_list:
        child_folder_list = get_list_folder_names(os.path.join(source_path,folder_name))
        logging.info(f"{folder_name}:\n {child_folder_list}\n")
        for  item in tqdm(child_folder_list):
            print("=" * terminal_size.columns)
            print(f"\n <<< {item} >>>\n")
            list_zip_files = get_list_file_names(os.path.join(source_path,folder_name,item))
            zip_dict = {}
            for zip_file in list_zip_files:

                source_file_path = os.path.join(source_path,folder_name,item,zip_file)
                target_folder_path = os.path.join(cfg.sys_path["target_path"],folder_name,item)

                if not os.path.exists(target_folder_path):
                    os.makedirs(target_folder_path)
                if not os.path.exists(os.path.join(target_folder_path, f"{zip_file.split(".")[0]}.csv")):
                    extract_file(source_file_path,target_folder_path, f"{zip_file.split(".")[0]}.csv")
                    
                    logging.info(f"{zip_file} is extracted to {zip_file.split('.')[0]}.csv successfuly!")
                else:
                     logging.info(f"{zip_file.split('.')[0]}.csv was already extracted!")

                #target_path_list.append(os.path.join(source_file_path,target_folder_path, f"{zip_file.split(".")[0]}.csv"))
                num_rows = get_file_rows(os.path.join(target_folder_path, f"{zip_file.split(".")[0]}.csv"))
                zip_dict[zip_file] = num_rows
                
            items[item] = zip_dict
    print_report(items)
    write_to_file({"main_folder": folder_name,
                        "items":items}
                        )

