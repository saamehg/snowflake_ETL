import os
import zipfile
import configuration as cfg
import logging
import sys

logging.basicConfig(
    level=logging.DEBUG,
    format="[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d] %(message)s",
    datefmt="%d/%b/%Y %H:%M:%S",
    stream=sys.stdout,
)


def unzip_dl_file(file_path):
    list_of_files = os.listdir(file_path)
    try:
        if len(list_of_files) == 1 and zipfile.is_zipfile(
            os.path.join(file_path, list_of_files[0])
        ):
            logging.info(f"processing {list_of_files[0]}")
            zipdata = zipfile.ZipFile(os.path.join(file_path, list_of_files[0]), "r")
            zipinfos = zipdata.infolist()
            for zipinfo in zipinfos:
                zipinfo.filename = zipinfo.filename.replace(" ", "_")
                zipdata.extract(zipinfo, path=file_path)

            logging.info(f"{list_of_files[0]} is extracted")
        else:
            logging.error(f"the file is not zipped or more than one file exist")
    except Exception as e:
        print(e)
