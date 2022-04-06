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
    neighborhoods_output_folder = args.neighborhoods_output

    log.info(f'Stage Folder: {stage_folder}')
    log.info(f'Neighborhoods Output Folder: {neighborhoods_output_folder}')
    log.info(f'Log Folder: {args.log}')
    log.info(f'Schema File: {stage_schema_file}')

    log.info('Extracting from input folder(s).')
    # TODO: may change this approach.
    # TODO: Also, dropping _bad_data here now, but will move this operation elsewhere in the future.
    stage_df = extract(stage_folder, stage_schema_file).drop('created_datetime', 'load_date', '_bad_data')

    log.info('Running transformations.')

    neighborhoods = transform(stage_df)

    log.info('Loading neighborhoods data to HDFS.')
    load(neighborhoods, neighborhoods_output_folder)


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
    log.info('Running neighborhoods transformations.')
    neighborhoods = transform_neighborhoods(data)

    return neighborhoods


def transform_neighborhoods(data: DataFrame):
    """
    Transform the source data.
    """
    df: DataFrame = (data
                     .withColumn('geo_type',
                                 F.when(F.col('geo_type').startswith('neighborhoods-1m'), F.lit('neighborhood_lev_m'))
                                 .when(F.col('geo_type').startswith('neighborhoods-2n'), F.lit('neighborhood_lev_n'))
                                 .when(F.col('geo_type').startswith('neighborhoods-3b'), F.lit('neighborhood_lev_b'))
                                 .when(F.col('geo_type').startswith('neighborhoods-4s'), F.lit('neighborhood_lev_s'))
                                 )
                     .withColumn('neighborhood_type',
                                 F.when(F.col('neighborhood_type').startswith('neighborhoods-1m'), F.lit('Neighborhood Level 1'))
                                 .when(F.col('neighborhood_type').startswith('neighborhoods-2n'), F.lit('Neighborhood Level 2'))
                                 .when(F.col('neighborhood_type').startswith('neighborhoods-3b'), F.lit('Neighborhood Level 3'))
                                 .when(F.col('neighborhood_type').startswith('neighborhoods-4s'), F.lit('Neighborhood Level 4'))
                                 )
                     .withColumn('geo_id', nullif('geo_id', ''))
                     .withColumn('name', nullif('name', ''))
                     .withColumn('fips_code',
                                 F.concat(F.lit(F.lpad(F.col('state_fips_code'), 2, "0")),
                                          F.lit(F.lpad(F.col('county_fips_code'), 3, "0"))
                                          ))
                     .withColumn('state_abbr', nullif('state_abbr', ''))
                     .withColumn('state_fips_code', F.lpad(F.col('state_fips_code'), 2, "0"))
                     .withColumn('metro', nullif('metro', ''))
                     .withColumn('incorporated_place', nullif('incorporated_place', ''))
                     .withColumn('county', nullif('county', ''))
                     .withColumn('county_fips_code', F.lpad(F.col('county_fips_code'), 3, "0"))
                     .withColumn('longitude', nullif('longitude', '0'))
                     .withColumn('latitude', nullif('latitude', '0'))
                     .withColumn('geoloc', F.regexp_replace(F.col('geoloc'), 'SRID=4326;', '')))

    neighborhoods = df.alias('a').selectExpr('current_timestamp() as created_datetime', 'a.*')

    return neighborhoods


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
        description='Spark program to process stage Neighborhoods files from Attom Jetstream.')
    parser.add_argument('--stage', type=str, required=True, help='Specify path to the stage neighborhoods folder')
    parser.add_argument('--stage_schema', type=str, required=True,
                        help='Specify path to the stage neighborhoods schema file')
    parser.add_argument('--neighborhoods_output', type=str, required=True,
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
                        vendor_product='Neighborhoods')

    log.info('Process started.')

    spark = SparkSession.builder \
        .enableHiveSupport() \
        .getOrCreate()

    # Add our helper function to DataFrame class
    DataFrame.chain = chain

    main(args)
    log.info('Process complete.')
