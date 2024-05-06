import configuration as cfg
import misc 
import unzip
from snowflake_util import Snowflakes

def main():  # Corrected function definition


    if cfg.pipeline['unzip_dl_file']:
        # Unzip the latest downloaded file
        misc.unzip_dl_file(cfg.sys_path["source_path"])

    if cfg.pipeline['run_unzip_pipeline']:
        # Unzip, rename and report zipfiles in the subfolders
        unzip.run_unzip_pipeline()

    if cfg.pipeline['upload_data']:
        #upload csv files into snowflake staging
        sn = Snowflakes()
        sn.upload_data()
    

if __name__ == "__main__":
    main()