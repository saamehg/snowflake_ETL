import os

snowflake_auth = {
    "warehouse": "PROMETHEUS_WH",
    "user": "PROMETHEUS_ADMIN",
    "azure_object_id": os.environ[
        "azure_object_id"
    ],  # set your Azure object Id in user environment variable
}

sys_path = {
    "source_path": "C:\\Users\\saameh.golzadeh\\Project\\Snowflake_Import\\data",
    "target_path": "C:\\Users\\saameh.golzadeh\\Project\\Snowflake_Import\\extracted_for_fah_import",
    "pickle_target_name": "target_extracted_files_path.pk", # keeps list of extracted files, number of rows to be loaded in snowflake
}

pipeline = {"unzip_dl_file": False, 
            "run_unzip_pipeline": False, 
            "upload_data": True}

exceptions = {
        "file_names" : [
            "GL_CODE_COMBINATIONS",
            "XLA_AE_HEADERS" 
        ]

}
