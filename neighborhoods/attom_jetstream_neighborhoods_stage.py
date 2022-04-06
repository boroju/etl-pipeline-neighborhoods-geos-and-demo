from amherst_common.amherst_logger import AmherstLogger
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.types import *
from datetime import datetime
import pyspark.sql.functions as F
import pytz
import argparse
import json
import os
import pathlib


def main(args):
    raw_folder = args.input
    stage_folder = args.output
    bad_folder = args.bad_data
    schema_file = args.schema

    log.info(f'Raw Folder: {raw_folder}')
    log.info(f'Stage Folder: {stage_folder}')
    log.info(f'Bad Folder: {bad_folder}')
    log.info(f'Log Folder: {args.log}')
    log.info(f'Schema File: {schema_file}')

    log.info('Extracting from raw folder.')
    df = extract(raw_folder, schema_file)
    df = add_source_file_columns(df)

    log.info('Running transformations.')
    transformed_df = transform(df)

    # ORC files for bad data
    if bad_folder:
        log.info('Load bad records.')
        bad_records = (
            transformed_df
                .where(F.col('_bad_data').isNotNull())
                .cache()
                .select('_bad_data')
        )
        load_bad(bad_records, bad_folder)

    log.info('Loading data to HDFS.')
    final_df = transformed_df.where(F.col('_bad_data').isNull()).drop('_bad_data')
    # create ORC files for good data
    load(final_df, stage_folder)
    log.info('Update table partitions.')
    # Update Hive table partitions
    update('attomjetstream_stage.neighborhoods_hist')


def extract(source_path: str, schema_path: str):
    """
    Extract the source data from raw.
    """
    source_path = source_path + '/neighborhoods*'
    # TODO: Additional validation (schema, etc.)
    schema = get_schema(schema_path)
    return spark.read.csv(source_path, schema=schema, mode='PERMISSIVE', sep='\t', ignoreLeadingWhiteSpace=True,
                          ignoreTrailingWhiteSpace=True)


# TODO: Move this to a common/library module since used in multiple modules.
def get_schema(schema_path: str):
    """
    Given a path to a json schema file, this function will return a StructType from it.
    """
    json_dict = {}
    with open(schema_path, 'r') as file:
        json_dict = json.load(file)

    return StructType.fromJson(json_dict)


# TODO: Add to common lib
def add_source_file_columns(df: DataFrame):
    return df.withColumn('file_name', F.regexp_extract(F.input_file_name(), '^(.*\\/)(.+)', 2)) \
        .withColumn('parent_folder_name', F.regexp_extract(F.input_file_name(), '^(.*\\/)(.*)(?=\\/)', 2))


def transform(data):
    """
    Transform the source data.
    """
    data.schema.fieldNames().remove('file_name')
    data.schema.fieldNames().remove('parent_folder_name')
    df = (data
          .withColumn('geo_id', F.col('geo_id'))
          .withColumn('name', F.col('name'))
          .withColumn('state_abbr', F.col('state_abbr'))
          .withColumn('state_fips_code', F.col('state_fips_code'))
          .withColumn('metro', F.col('metro'))
          .withColumn('incorporated_place', F.col('incorporated_place'))
          .withColumn('county', F.col('county'))
          .withColumn('county_fips_code', F.col('county_fips_code'))
          .withColumn('longitude', F.col('longitude'))
          .withColumn('latitude', F.col('latitude'))
          .withColumn('geoloc', F.col('geoloc'))
          )

    # TODO: It appears spark will try to convert to utc for you which can screw things up if you already converted to utc
    return df.select(F.current_timestamp().alias('created_datetime'),
                     F.lit(load_date).cast('date').alias('load_date'),
                     F.col('file_name').alias('geo_type'),
                     F.col('file_name').alias('neighborhood_type'),
                     F.col('*'))


def load(transformed_data, destination_path):
    """
    Load the transformed data to it's destination.
    """
    transformed_data.write.orc(destination_path, partitionBy='load_date', mode='overwrite', compression='snappy')


def load_bad(df, destination_path):
    """
    Load the bad records to the bad folder.
    """
    df \
        .write \
        .mode(saveMode='overwrite') \
        .text(destination_path, compression='none')


def update(table):
    """
    Update the Hive table partitions.
    """
    msck_cmd = 'MSCK REPAIR TABLE ' + table
    log.info(f'Running command: {msck_cmd}')
    spark.sql(msck_cmd)
    log.info(f'Completed command: {msck_cmd}')


def chain(self, f):
    """
    NOTE: This needs to be added to a library.

    Function that allows easily chaining together DataFrame transformations. Needs to be added to DataFrame class.
    """
    return f(self)


def parse_args():
    """
    Argument parsing function
    :return: Namespace containing all of the command line arguments
    """
    # Setup argument parsing
    parser = argparse.ArgumentParser(
        description='Spark program to process raw Neighborhoods files from Attom Jetstream.')
    parser.add_argument('-i', '--input', type=str, required=True, help='Specify path to the input file(s)')
    parser.add_argument('-s', '--schema', type=str, required=True, help='Specify path to the Schema.json')
    parser.add_argument('-o', '--output', type=str, required=True, help='Specify path to the output ORC file(s)')
    parser.add_argument('-salt', type=int, required=False, default=12,
                        help='Use salt to better distribute data during repartitions')
    parser.add_argument('-b', '--bad_data', type=str, required=True,
                        help='Specify path to the output ORC file(s) for bad records')
    parser.add_argument('-l', '--log', type=str, required=False, help='Specify path to the log directory',
                        default='/etl/log/')
    parser.add_argument('-d', '--debug', action='store_true', required=False, help='Specify log level as DEBUG')
    parser.add_argument('--start_date', type=lambda d: datetime.strptime(d, '%Y-%m-%d'), required=False,
                        help='Start date corresponding to raw folder name in format yyyy-MM-dd')
    parser.add_argument('--end_date', type=lambda d: datetime.strptime(d, '%Y-%m-%d'), required=False,
                        help='End date corresponding to raw folder name in format yyyy-MM-dd')
    parsed_args = parser.parse_args()

    return parsed_args


def get_date_from_input_folder(folder_path: str) -> datetime:
    deepest_folder = pathlib.Path(folder_path).stem
    return datetime.strptime(deepest_folder, '%Y%m%d')


if __name__ == '__main__':
    args = parse_args()
    salt = args.salt

    py_file = os.path.splitext(os.path.split(__file__)[1])[0]
    log = AmherstLogger(log_directory=args.log, log_file_app=py_file, vendor_name='ATTOM Jetstream',
                        vendor_product='Neighborhoods')

    log.info('Process started.')

    spark = SparkSession.builder \
        .enableHiveSupport() \
        .getOrCreate()

    # Add our helper function to DataFrame class
    DataFrame.chain = chain

    # get date vendor data was downloaded
    try:
        load_date = get_date_from_input_folder(args.input)
    except ValueError as err:
        log.warning("Input folder doesn't contain a date. Using datetime.utcnow() instead.")
        load_date = pytz.utc.localize(datetime.utcnow())

    log.info(f'load_date: {datetime.strftime(load_date, "%Y-%m-%d")}')

    main(args)
    log.info('Process complete.')
