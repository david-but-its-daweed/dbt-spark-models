import argparse
import json
import logging
from datetime import date

from future.backports.datetime import timedelta

from infra.dbtjoom.invocation import run, compile
from infra.dbtjoom.find import find_nodes
from infra.dbtjoom.load import load_spark_profile
from infra.dbtjoom.thrift_client import ThriftClient
import sys

from infra.dbtjoom.drop import drop_model_output

logger = logging.getLogger('main')
logging.basicConfig(
    format='%(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    stream=sys.stdout,
    level=logging.INFO,
)

def parse_args():
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("-s", "--select", nargs="*", help="Specify models to select")
    parser.add_argument("-m", "--models", "--model", nargs="*", dest="models", help="Specify models")
    parser.add_argument("--exclude", nargs="*", help="Specify models to exclude")
    parser.add_argument("--resource-type", help="Specify the resource type")
    parser.add_argument("--full-refresh", action="store_true", help="Full refresh, drop incremental models too")
    parser.add_argument("--retry", action="store_true", help="If set - skip models that has succeed in the previous run")
    parser.add_argument("--changed-only", action="store_true", help="If set - run only models that has been changed comparing with main branch")
    parser.add_argument("--dryrun", action="store_true", help="Enable dry run mode (default: False)")
    parser.add_argument("--drop", action="store_true", help="Drop existing location")

    args, other_args = parser.parse_known_args()

    return args, other_args

def dbt_vars_to_str(dbt_vars):
    return json.dumps(dbt_vars, separators=(',', ':'))

def run_dbt(args, other_args):
    dbt_vars = {
        'start_date_ymd': str(date.today() - timedelta(1)),
        'end_date_ymd': str(date.today()),
        'table_name': 'table_name',
    }
    compile('--vars', dbt_vars_to_str(dbt_vars))
    nodes = find_nodes(
        select=args.select,
        exclude=args.exclude,
        models=args.models,
        resource_type=args.resource_type,
        changed_only=args.changed_only,
        failed_only=args.retry,
    )
    if not nodes:
        logger.info("No models found")
        return

    logger.info("Models to execute:")
    for node in nodes:
        logger.info(f' - {node.relation_name}')

    if args.drop:
        client = ThriftClient.from_profile(load_spark_profile())
        try:
            for node in nodes:
                drop_model_output(node, client,
                                  dryrun=args.dryrun,
                                  junk=True,
                                  spark_staging_only=False,
                                  include_incremental=args.full_refresh
                                  )
        finally:
            client.close_connection()

    dbt_vars.update({
        'dev_nodes_to_override': ','.join([node.unique_id for node in nodes]),
        'dbt_default_production_schema': load_spark_profile('./production/profiles/profiles.yml').schema
    })

    dbt_args = ['--select', ' '.join(node.name for node in nodes), '--vars', dbt_vars_to_str(dbt_vars)]
    if args.full_refresh:
        dbt_args.append('--full-refresh')
    dbt_args.extend(other_args)

    logger.info(f"dbt run {' '.join(dbt_args)}")
    if args.dryrun:
        compile(*dbt_args)
    else:
        run(*dbt_args)


if __name__ == '__main__':
    args, other_args = parse_args()
    logger.info(f"Args: {args}")
    logger.info(f"Args for DBT: {other_args}")
    run_dbt(args, other_args)
