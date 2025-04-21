import sys
import logging

from datetime import date, timedelta
from infra.dbtjoom.load import load_dbt_run_results, load_spark_profile
from infra.dbtjoom.drop import drop_model_output
from infra.dbtjoom.thrift_client import ThriftClient
from infra.dbtjoom.invocation import  compile
from infra.dbtjoom.find import find_nodes
import argparse
import logging
import sys
import json


logging.basicConfig(
    format='%(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    stream=sys.stdout,
    level=logging.INFO,
)

logger = logging.getLogger("cleanup_table_locations")


def dbt_vars_to_str(dbt_vars):
    return json.dumps(dbt_vars, separators=(',', ':'))


def parse_args():
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("-s", "--select", nargs="*", help="Specify models to select")
    parser.add_argument("-m", "--models", "--model", nargs="*", dest="models", help="Specify models")
    parser.add_argument("--exclude", nargs="*", help="Specify models to exclude")
    parser.add_argument("--resource-type", help="Specify the resource type")
    parser.add_argument("--full-refresh", action="store_true", help="Full refresh, drop incremental models too")
    parser.add_argument("--dryrun", action="store_true", help="Enable dry run mode (default: False)")
    parser.add_argument('--platform', type=str, required=True, help="Platform model to run. If not spark, then script just do nothing")
    parser.add_argument('--profiles_dir', type=str, required=True, help="Profiles dir with thrift settings")
    parser.add_argument('--profile', type=str, required=True, help="Name of the profile")
    args, other_args = parser.parse_known_args()

    return args, other_args


def main(args):
    """
    get model list (must be one model only)
    check run results
    if run state is error
        and the error is not "LOCATION ALREADY EXISTS"
        and materialized == table or materialized=incremental and full reload

        drop tabel location
    if model is
    :param args:
    :return:
    """
    dbt_vars = {
        'start_date_ymd': str(date.today()),
        'end_date_ymd': str(date.today() - timedelta(1)),
        'table_name': 'table_name',
    }
    compile('--vars', dbt_vars_to_str(dbt_vars))
    nodes = find_nodes(
        select=args.select,
        exclude=args.exclude,
        models=args.models,
        resource_type=args.resource_type,
    )

    nodes = {
        node.unique_id: node
        for node in nodes
        if node.is_table or node.is_incremental and args.full_refresh
    }

    logger.info("Nodes to check:")
    for node in nodes.values():
        logger.info(f" - {node.unique_id}")

    if not nodes:
        logger.info(f"No nodes selected")
        return

    results = load_dbt_run_results()
    if not results:
        logger.info("No run results found")
        return

    client = None
    try:
        for res in results.results:
            if res.unique_id not in nodes:
                continue

            if res.status == 'success':
                continue

            # todo: test what exactly must be in res.message
            if 'Location already exists' in res.message:
                continue

            if client is None:
                client = ThriftClient.from_profile(load_spark_profile(
                    f'{args.profiles_dir}/profiles.yml',
                    args.profile
                ))

            # Some error has occurred during execution, need to drop all created files
            logger.info(f"Model {res.unique_id} failed with err='{res.message}'. Drop output")
            drop_model_output(
                nodes[res.unique_id],
                client=client,
                junk=False,
                spark_staging_only=False,
                include_incremental=args.full_refresh,
                dryrun=args.dryrun
            )
    finally:
        if client is not None:
            client.close_connection()


if __name__ == '__main__':
    args, _ = parse_args()
    print(f'Args: {args}')
    if args.platform != "spark":
        logger.info(f"Platform is {args.platform}, not spark. Exiting")
        exit(0)

    main(args)
