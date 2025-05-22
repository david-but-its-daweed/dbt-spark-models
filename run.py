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
    force=True
)

def parse_args():
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("-s", "--select", nargs="*", help="Specify models to select")
    parser.add_argument("-m", "--models", "--model", nargs="*", dest="models", help="Specify models")
    parser.add_argument("--exclude", nargs="*", help="Specify models to exclude")
    parser.add_argument("--resource-type", help="Specify the resource type")
    parser.add_argument("-f", "--full-refresh", action="store_true", help="Full refresh, drop incremental models too")
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

    if  args.select or args.exclude or args.models or args.resource_type:
        # nodes are selected explicitly, we need to recalculate them regardless if they have been changed or not
        nodes = find_nodes(
            select=args.select,
            exclude=args.exclude,
            models=args.models,
            resource_type=args.resource_type,
            failed_only=args.retry,
        )
    elif args.changed_only:
        # no nodes specified, just get changed ones
        nodes = find_nodes(
            changed_only=True,
            failed_only=args.retry,
            select_dependencies='fill_gaps'
        )
    else:
        # no selector specified, do nothing
        nodes = []

    if not nodes:
        logger.info("No models found or no selector specified. Exit")
        return

    logger.info("Models to execute:")
    for node in nodes:
        logger.info(f' - {node.relation_name}')

    if args.drop:
        client = ThriftClient.from_profile(load_spark_profile())
        try:
            for node in nodes:
                drop_model_output(
                    node,
                    client,
                    dryrun=args.dryrun,
                    junk=True,
                    spark_staging_only=False,
                    include_incremental=args.full_refresh
                )
        finally:
            client.close_connection()

    # What nodes must be written to and read from the junk schema: 
    # 1. All nodes requested for calculation
    # 2. All nodes that have been changed and their children 
    nodes_in_junk = (
        {n.unique_id for n in nodes}
        | {n.unique_id for n in find_nodes(changed_only=True, select_dependencies='all_children')}
    )
    dbt_vars.update({
        'dev_nodes_to_override': ','.join(nodes_in_junk),
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

"""
todo - проверить
Ты меняешь B. Вызываешь ./run.sh - то у тебя в джанк считается B; A берется с прода
Ты меняешь B. Вызываешь ./run.sh --models +B - то у тебя в джанк считается A и B
Ты меняешь B. Вызываешь ./run.sh --models +C - то в джанке A, B, C
Ты меняешь B. Вызываешь ./run.sh --model D - то в джанк считается B, C, D; A - берется с прода
Ты меняешь B. Вызываешь ./run.sh --model B - то в джанк считается B; A - берется с прода (как в 1м случае)
Ты ничего не меняешь. Вызываешь ./run.sh --model B - то в джанк считается B; A - берется с прода (как в 1м случае)
Ты ничего не меняешь. Вызываешь ./run.sh --models +B - то у тебя в джанк считается A и B (как во 2м случае)
Ты меняешь B, D. Вызываешь ./run.sh- в джанк считается B, C, D
"""

if __name__ == '__main__':
    args, other_args = parse_args()
    logger.info(f"Args: {args}")
    logger.info(f"Args for DBT: {other_args}")
    run_dbt(args, other_args)
