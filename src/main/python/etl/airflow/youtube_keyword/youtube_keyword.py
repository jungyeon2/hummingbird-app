import os
import csv
import logging
import luigi
import psycopg2
import luigi.contrib.hive
import ihr_etl.commons.hdfs as hdfs
from datetime import datetime, date
from luigi import configuration
from ihrcelery.client import get_client
from ihr_etl.commons.mssql import QueryTemplate
from ihr_etl.commons.redshift import hive_to_redshift

logger = logging.getLogger('luigi-interface')
working_dir = os.path.dirname(os.path.realpath(__file__))
staging_dir = '/tmp/stg/device_token'
dim_type = 'dim_device_token'

configuration.LuigiConfigParser.add_config_path(working_dir)

print working_dir

config = configuration.get_config()

PG_SERVER = config.get('pgauth1', 'SERVER')
PG_DATABASE = config.get('pgauth1', 'DATABASE')
PG_PORT = config.get('pgauth1', 'PORT')
PG_USER = config.get('pgauth1', 'USER')
PG_PASSWORD = config.get('pgauth1', 'PASSWORD')
PG_CONN_STRING = "host={0} port={1} dbname={2} user={3} password={4}".format(
    PG_SERVER, PG_PORT, PG_DATABASE, PG_USER, PG_PASSWORD)



class RunDeviceToken(luigi.Task):
    """ Run Class For Dim device_token"""
    def run(self):
        logger.debug("DONE!")

    def requires(self):
        """..."""
        return ImportDimDeviceTokenIntoHive()

    def on_success(self):
        """ Remove temporary files """
        output = "{0}/{1}.loaded".format(staging_dir, dim_type)
        if os.path.isfile(output):
            os.remove(output)


class ImportDimDeviceTokenIntoHive(luigi.Task):
    """ Run Class to load local device_token csv into HIVE and redshift """
    def run(self):
        """..."""
        stg_location = "/data/logs/{0}".format(dim_type)
        dest_location = "{0}/{1}.csv".format(stg_location, dim_type)

        # Clear the way
        luigi.contrib.hive.run_hive_cmd("use ihr_stg; drop table if exists {0};".format(dim_type))
        luigi.contrib.hive.run_hive_cmd("use ihr_dwh; truncate table {0};".format(dim_type))

        tmp_file = "{0}/{1}.csv".format(staging_dir, dim_type)

        hdfs.makeHdfsDir(stg_location)
        hdfs.removeHdfsFile(dest_location)
        hdfs.putHdfsFile(tmp_file, dest_location)

        if os.path.exists(tmp_file):
            logger.info("Cleaning Up Local Temp File: {0}".format(tmp_file))
            os.remove(tmp_file)

        template = QueryTemplate(working_dir)
        create_query = template.get_template("create_{0}".format(dim_type))
        logger.debug("{create_query}".format(create_query=create_query))
        luigi.contrib.hive.run_hive_cmd(create_query)

        # Load into ihr_dwh as ORC
        orc_query = template.get_template("load_{0}_into_orc".format(dim_type))
        logger.debug("{orc_query}".format(orc_query=orc_query))
        luigi.contrib.hive.run_hive_cmd(orc_query)

        # Create our .loaded file
        output = "{0}/{1}.loaded".format(staging_dir, dim_type)
        open(output, 'a').close()

    def requires(self):
        """..."""
        return ExportDimDeviceTokenFromPG()

    def output(self):
        """..."""
        output = "{0}/{1}.loaded".format(staging_dir, dim_type)
        logger.debug("Looking for: {0}".format(output))
        return luigi.LocalTarget(output)

    def on_success(self):
        """..."""
        celery = get_client()
        celery.send_task(
            'ihrcelery.unified.tasks.send_data', (
                True,
                '{0}'.format(dim_type),
                datetime.now().strftime('%Y-%m-%d'),
                True))
        logger.debug("Data being sent to celery [{0}].".format(datetime.now().strftime('%Y-%m-%d')))


class ExportDimDeviceTokenFromPG(luigi.Task):
    """ Exports Data From Postgres to Local"""

    def run(self):
        """..."""
        output = "{0}/{1}.csv".format(staging_dir, dim_type)
        if os.path.isfile(output):
            os.remove(output)   # Delete any stragglers

        template = QueryTemplate(working_dir)
        pg_query = template.get_template("load_pg_device_token")
        conn = psycopg2.connect(PG_CONN_STRING)
        cursor = conn.cursor()
        cursor.execute(pg_query)
        with open(output, 'w') as f:
            csv.writer(f, quoting=csv.QUOTE_NONNUMERIC, escapechar='\\').writerows(cursor)

    def output(self):
        """..."""
        output = "{0}/{1}.csv".format(staging_dir, dim_type)
        return luigi.LocalTarget(output)

if __name__ == "__main__":
    luigi.run(main_task_cls=RunDeviceToken)
