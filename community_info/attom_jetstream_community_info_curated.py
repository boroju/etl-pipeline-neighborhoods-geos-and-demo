from amherst_common.amherst_logger import AmherstLogger
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType
import pyspark.sql.functions as F
import argparse
import json
import os


def main(args):
    stage_folder = args.stage
    stage_schema_file = args.stage_schema
    community_info_output_folder = args.community_info_output

    log.info(f'Stage Folder: {stage_folder}')
    log.info(f'Community Info Output Folder: {community_info_output_folder}')
    log.info(f'Log Folder: {args.log}')
    log.info(f'Schema File: {stage_schema_file}')

    log.info('Extracting from input folder(s).')
    # TODO: may change this approach.
    # TODO: Also, dropping _bad_data here now, but will move this operation elsewhere in the future.
    stage_df = extract(stage_folder, stage_schema_file).drop('create_timestamp', 'load_date', '_bad_data')

    log.info('Running transformations.')

    community_info = transform(stage_df)

    log.info('Loading community info data to HDFS.')
    load(community_info, community_info_output_folder)


def extract(source_path: str, schema_path: str = None):
    """
    Extract the source data from raw.
    """
    if schema_path:
        schema = get_schema(schema_path)
        return spark.read.schema(schema).orc(source_path)
    else:
        return spark.read.orc(source_path)


# TODO: Move this to a common/library module since used in multiple modules.
def get_schema(schema_path: str):
    """
    Given a path to a json schema file, this function will return a StructType from it.
    """
    with open(schema_path, 'r') as file:
        json_dict = json.load(file)

    return StructType.fromJson(json_dict)


def transform(data: DataFrame):
    """
    Transform the source data.
    """
    log.info('Running community info transformations.')
    community_info = transform_community_info(data)

    return community_info


def transform_community_info(data: DataFrame):
    """
    Transform the source data.
    """
    df: DataFrame = (data.alias('a').selectExpr('current_timestamp() as create_timestamp', 'a.*'))

    community_info = df

    return community_info


def load(transformed_data, destination_path):
    """
    Load the transformed data to it's destination.
    """
    transformed_data \
        .write \
        .orc(destination_path, mode='overwrite')


def chain(self, f):
    """
    NOTE: This needs to be added to a library.

    Function that allows easily chaining together DataFrame transformations. Needs to be added to DataFrame class.
    """
    return f(self)


def nullif(col_name, value):
    return F.when(F.col(col_name) != value, F.col(col_name)).otherwise(None)


def parse_args():
    """
    Argument parsing function
    :return: Namespace containing all of the command line arguments
    """
    # Setup argument parsing
    parser = argparse.ArgumentParser(
        description='Spark program to process stage Community Info files from Attom Jetstream.')
    parser.add_argument('--stage', type=str, required=True, help='Specify path to the stage community info folder')
    parser.add_argument('--stage_schema', type=str, required=True,
                        help='Specify path to the stage community info schema file')
    parser.add_argument('--community_info_output', type=str, required=True,
                        help='Specify path to the output ORC file(s)')
    parser.add_argument('-l', '--log', type=str, required=False, help='Specify path to the log directory',
                        default='/etl/log/')
    parser.add_argument('-d', '--debug', action='store_true', required=False, help='Specify log level as DEBUG')
    parsed_args = parser.parse_args()

    return parsed_args


if __name__ == '__main__':
    args = parse_args()

    py_file = os.path.splitext(os.path.split(__file__)[1])[0]
    log = AmherstLogger(log_directory=args.log, log_file_app=py_file, vendor_name='ATTOM Jetstream',
                        vendor_product='Community Info')

    log.info('Process started.')

    spark = SparkSession.builder \
        .enableHiveSupport() \
        .getOrCreate()

    # Add our helper function to DataFrame class
    DataFrame.chain = chain

    main(args)
    log.info('Process complete.')
