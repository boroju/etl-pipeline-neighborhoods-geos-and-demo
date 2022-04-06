import os
import sys
import argparse
import datadeliveryclient as ddc
import urllib3  # Needed to disable SSL warning messages
import requests  # Needed to create the HTTP Session
from amherst_common.amherst_logger import AmherstLogger
from hdfs.ext.kerberos import KerberosClient


def parse_args():
    """
    Argument parsing function
    :return: Namespace containing all of the command line arguments
    """
    # Setup argument parsing
    parser = argparse.ArgumentParser(description='Python program to check new Neighborhoods new files from ATTOM Jetstream API.',
                                     add_help=True)
    parser.add_argument('-w', '--web_hdfs', type=str, required=True, help='HTTP URL for hdfs name nodes')
    parser.add_argument('-p', '--hdfs_file_path', type=str, required=True,
                        help='The HDFS path to the attom_jetstream directory.')
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


def check_if_neighborhoods_new_files(zip_files):

    new_files = False

    for x in range(len(zip_files)):
        log.info("Checking " + zip_files[x] + " name...")

        if str(zip_files[x]).__contains__('neighborhoods') and not str(zip_files[x]).__contains__('community-info'):
            log.info('New data for Neighborhoods is available!')
            new_files = True
            return new_files

    return new_files


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
    last_folder_date_neighborhoods = get_hdfs_last_folder_date(args.hdfs_file_path + 'neighborhoods/')
    log.info("Last Neighborhoods upload date from dir: " + last_folder_date_neighborhoods)

    last_folder_date = last_folder_date_neighborhoods
    new_neighborhoods_flag = False

    # determine if files should be downloaded (new upload date)
    if last_folder_date < files_upload_date:
        # get file URLs from asset manifest
        files_urls = ddc_obj.get_download_file_urls(last_folder_date)

        # get file names from URL
        files_names = ddc_obj.get_zip_filenames_from_urls(files_urls)

        new_neighborhoods_flag = check_if_neighborhoods_new_files(files_names)

        if new_neighborhoods_flag:
            print('New Neighborhoods: TRUE')
        else:
            print('New Neighborhoods: FALSE')

        log.info("Process Complete")
    else:
        log.info("No new files to download...")
