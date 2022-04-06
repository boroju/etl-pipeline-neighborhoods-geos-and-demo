import os
import sys
import argparse
from pathlib import Path
import datadeliveryclient as ddc
import urllib3  # Needed to disable SSL warning messages
import requests  # Needed to create the HTTP Session
from amherst_common.amherst_logger import AmherstLogger
from urllib.request import urlopen
from zipfile import ZipFile
from io import BytesIO
from hdfs.ext.kerberos import KerberosClient
import bz2


def parse_args():
    """
    Argument parsing function
    :return: Namespace containing all of the command line arguments
    """
    # Setup argument parsing
    parser = argparse.ArgumentParser(description='Python program to retrieve ATTOM Jetstream data from API to Raw.', add_help=True)
    parser.add_argument('-w', '--web_hdfs', type=str, required=True, help='HTTP URL for hdfs name nodes')
    parser.add_argument('-p', '--hdfs_file_path', type=str, required=True, help='The HDFS path to the attom_jetstream directory.')
    parser.add_argument('-e', '--api_host', type=str, required=True, help='API Host')
    parser.add_argument('-k', '--api_key', type=str, required=True, help='API Key')
    parser.add_argument('-f', '--api_file', type=str, required=True, help='API File')
    parser.add_argument('-l', '--log', type=str, required=False, help='Specify path to the log directory',
                        default='/etl/log/')
    parser.add_argument('-d', '--debug', action='store_true', required=False, help='Specify log level as DEBUG')
    parsed_args = parser.parse_args()

    return parsed_args


def create_dir_if_not_exists(dir):
    if not dir.exists():
        try:
            dir.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            sys.exit(str(e))


def get_hdfs_last_folder_date(file_path):
    http_session = requests.session()
    http_session.verify = False
    urllib3.disable_warnings()
    hdfs_path = args.web_hdfs
    hdfs_conn = KerberosClient(hdfs_path, session=http_session)
    log.info('Connecting to HDFS...')

    # Now that you have a connection to HDFS you can check the status of files/folders
    hdfs_obj = file_path

    # Retrieve a list of items from a folder
    hdfs_items = hdfs_conn.list(hdfs_obj)
    max_folder_date = 0
    folder_date = '00000000'

    for item in hdfs_items:
        if item.isdigit() and (max_folder_date < int(item)):
            max_folder_date = int(item)
            folder_date = item

    return folder_date


def determine_if_hdfs_folder_exists(folder_name):
    http_session = requests.session()
    http_session.verify = False
    urllib3.disable_warnings()
    hdfs_path = args.web_hdfs
    hdfs_conn = KerberosClient(hdfs_path, session=http_session)

    # Now that you have a connection to HDFS you can check the status of files/folders
    hdfs_obj = args.hdfs_file_path

    # Retrieve a list of items from a folder
    hdfs_items = hdfs_conn.list(hdfs_obj)
    existing_folder = 0

    for item in hdfs_items:
        if item == folder_name:
            existing_folder = 1

    return existing_folder


def write_files_from_url_to_hdfs(url, namefile):
    file_date = namefile.partition('-')[0]
    product = ''
    if str(namefile).__contains__('community-info'):
        product = 'community_info'
    elif str(namefile).__contains__('neighborhoods'):
        product = 'neighborhoods'

    http_session = requests.session()
    http_session.verify = False
    urllib3.disable_warnings()
    hdfs_hosts = args.web_hdfs
    kerb_client = KerberosClient(url=hdfs_hosts, session=http_session)

    if not determine_if_hdfs_folder_exists(product):
        kerb_client.makedirs(args.hdfs_file_path + product)
        log.info('A new product folder has been created in HDFS: ' + product)

    if not determine_if_hdfs_folder_exists(product + '/' + file_date):
        kerb_client.makedirs(args.hdfs_file_path + product + '/' + file_date)
        log.info('A new date folder has been created in HDFS: ' + product + '/' + file_date)

    resp = urlopen(url)
    zf = ZipFile(BytesIO(resp.read()))
    filenames = zf.namelist()
    log.info('Getting a list of all archived file names from the zip...')

    # Iterate over the file names
    for filename in filenames:
        # Check filename endswith tsv
        if filename.endswith('.tsv'):
            name = filename.rpartition('/')[2]
            log.info('Extracting ' + name)
            _file_path = args.hdfs_file_path + product + '/' + file_date + '/' + name
            if not _file_path.endswith('.bz2'):
                _file_path = _file_path + '.bz2'
            # Extract a single file from zip
            with kerb_client.write(_file_path, overwrite=True) as hdfs_wr, bz2.BZ2File(filename=hdfs_wr, mode='wb',
                                                                                       compresslevel=9) as bzip2_hdfs:
                bzip2_hdfs.write(zf.read(filename))
                log.info(name + '.bz2 file has been written in HDFS...')


if __name__ == '__main__':

    # get arguments
    args = parse_args()

    py_file = os.path.splitext(os.path.split(__file__)[1])[0]
    log = AmherstLogger(log_directory=args.log, log_file_app=py_file, vendor_name='JetstreamDepot',
                        vendor_product='ATTOMData')

    # create a new object of DataDeliveryClient class
    ddc_obj = ddc.DataDeliveryClient(args.api_host, args.api_key, args.api_file)

    # get last upload date
    files_upload_date = ddc_obj.get_last_updated_from_json()
    log.info("Last updated from asset manifest: " + files_upload_date)

    # get last upload date - neighborhoods
    last_folder_date_neighborhoods = get_hdfs_last_folder_date(args.hdfs_file_path+'neighborhoods/')
    log.info("Last Neighborhoods upload date from dir: " + last_folder_date_neighborhoods)

    # get last upload date - community_info
    last_folder_date_community_info = get_hdfs_last_folder_date(args.hdfs_file_path+'community_info/')
    log.info("Last Community Info upload date from dir: " + last_folder_date_community_info)

    if last_folder_date_community_info < last_folder_date_neighborhoods:
        last_folder_date = last_folder_date_neighborhoods
    else:
        last_folder_date = last_folder_date_community_info

    # determine if files should be downloaded (new upload date)
    if last_folder_date < files_upload_date:
        # get file URLs from asset manifest
        files_urls = ddc_obj.get_download_file_urls(last_folder_date)

        # get file names from URL
        files_names = ddc_obj.get_zip_filenames_from_urls(files_urls)

        for x in range(len(files_urls)):
            if str(files_names[x]).__contains__('neighborhoods') and not str(files_names[x]).__contains__('community-info'):
                log.info("Downloading " + files_names[x] + " content...")
                write_files_from_url_to_hdfs(files_urls[x], files_names[x])

        log.info("Process Complete")
    else:
        log.info("No new files to download")
