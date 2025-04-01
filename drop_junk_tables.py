import os
from typing import *
from datetime import date, timedelta
from pyhive import hive
import subprocess
import argparse
import sys
import json
import yaml
import logging


logging.basicConfig(
    format='%(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    stream=sys.stdout,
    level=logging.INFO,
)

logger = logging.getLogger("cleanup_table_locations")



def run(command, throw_on_fail: bool = True) -> str:
    logger.info(f"Run `{command}`")
    try:
        result = subprocess.run(
            command,
            shell=True,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT
        )
        return result.stdout.decode('utf8')
    except subprocess.CalledProcessError as e:
        if throw_on_fail:
            raise
        logger.error(f"Error executing command: {e}")


def load_spark_profile():
    file_path = os.path.expanduser('~/.dbt/profiles.yml')
    logger.info(f"Loading spark profile from `{file_path}`")
    with open(file_path, 'r') as file:
        data = yaml.safe_load(file)

    return data['spark']['outputs']['build']


def select_model_ids(selector: str) -> Iterable[Dict]:
    for line in run(f"dbt ls --output=json {selector}").splitlines():
        try:
            yield json.loads(line)
        except:
            logger.info(line)
            continue


def load_table_names(model_ids: List[str]) -> List[str]:
    with open('target/manifest.json') as f:
        manifest = json.load(f)

    return [manifest['nodes'][mid]['relation_name'] for mid in model_ids]


def query_location(hive_cursor, query):
    db_location = None
    try:
        hive_cursor.execute(query)
        for row in hive_cursor.fetchall():
            if 'Location' in row[0]:
                db_location = row[1].strip()
                break
    except Exception as e:
        if 'TABLE_OR_VIEW_NOT_FOUND' in str(e):
            logger.warning(f"Table not found")
        else:
            logger.warning(f"Error with `{query}`: {e}")

    return db_location


def get_table_location(hive_cursor, db_name, table_name) -> Optional[str]:
    table_location = query_location(hive_cursor, f"describe formatted {db_name}.{table_name}")
    if table_location is not None:
        return table_location

    db_location = query_location(hive_cursor, f"describe database {db_name}")
    if db_location is not None:
        return f'{db_location}/{table_name}'

    return None


def drop_table(table_name: str, conn: hive.Connection, dryrun: bool=True):
    logger.info(f"\nDrop {table_name}\n--")

    cursor = conn.cursor()

    table_location = get_table_location(cursor, *table_name.split('.'))
    query = f"DROP TABLE IF EXISTS `{table_name}`"
    logger.info(f' - Exec `{query}`')
    if not dryrun:
        cursor.execute(query)

    if table_location is None:
        logger.info(f" - Location for `{table_name}` not found")
        return

    logger.info(f" - Found location for `{table_name}`: `{table_location}`")
    if not table_location.startswith("s3://joom-analytics-users/"):
        logger.info(f" - Location is not in joom-analytics-users")
        return

    logger.info(f" - Drop location `{table_location}`")
    cmd = f"aws s3 rm --recursive {table_location} {'--dryrun' if dryrun else ''}"
    run(cmd)


def load_last_run_succeed_model() -> List[str]:
    try:
        with open('target/run_results.json') as f:
            run_results = json.load(f)

        for res in run_results['results']:
            if res['status'] == 'success':
                yield res['unique_id']
    except:
        return


def main(
        selector: str,
        dryrun: bool = True,
        full_refresh: bool = False,
        retry: bool = False,
):
    if full_refresh:
        logger.info(f"Loading table models for `{selector}`")
    else:
        logger.info(f"Loading non-incremental table models for `{selector}`")

    to_skip = set()
    if retry:
        to_skip = set(load_last_run_succeed_model())

    model_ids = list(
        m['unique_id']
        for m in select_model_ids(selector)
        if m['unique_id'] not in to_skip and m['resource_type'] == 'model' and (
            m['config']['materialized'] == 'table'
            or full_refresh and m['config']['materialized'] == 'incremental'
        )
    )

    if not model_ids:
        logger.info("No models found")
        return

    logger.info("Models to drop locations for:")
    for mid in model_ids:
        logger.info(f' - {mid}')

    logger.info("Loading names for found models")
    vars = {
        'start_date_ymd': str(date.today() - timedelta(1)),
        'end_date_ymd': str(date.today()),
        'table_name': '',
    }
    run(f"dbt compile --vars='{json.dumps(vars, separators=(',', ':'))}'")
    relation_names = load_table_names(model_ids)
    relation_names_to_drop = []

    for name in relation_names:
        if name.startswith("junk"):
            relation_names_to_drop.append(name)
        else:
            logger.info(f" - {name} skipped (not junk)")

    if not relation_names_to_drop:
        logger.info("No relations found")
        return

    logger.info("Found relations:")
    for name in relation_names_to_drop:
        logger.info(f" - {name}")

    profile = load_spark_profile()
    logger.warning(f"Connect to thrift: {profile['host']}:{[profile['port']]}")
    conn = hive.Connection(host=profile['host'], port=int(profile['port']))
    try:
        for name in relation_names_to_drop:
            drop_table(name, conn, dryrun=dryrun)
    finally:
        conn.close()


def parse_args():
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("-s", "--select", nargs="*", help="Specify models to select")
    parser.add_argument("-m", "--models", "--model", nargs="*", dest="models", help="Specify models")
    parser.add_argument("--exclude", nargs="*", help="Specify models to exclude")
    parser.add_argument("--selector", help="Specify a named selector")
    parser.add_argument("--resource-type", help="Specify the resource type")
    parser.add_argument("--full-refresh", action="store_true", help="Full refresh, drop incremental models too")
    parser.add_argument("--retry", action="store_true", help="If set - skip models that has succeed in the previous run")
    parser.add_argument("--dryrun", action="store_true", help="Enable dry run mode (default: False)")

    args, _ = parser.parse_known_args()

    return args


def build_dbt_selector_cli(args):
    res = []

    def add_to_res_if_set(arg_name, arg_cli=None):
        arg_cli = arg_cli or arg_name

        arg_val = getattr(args, arg_name)
        if not isinstance(arg_val, list):
            arg_val = [arg_val]

        if getattr(args, arg_name) is not None:
            res.extend([f'--{arg_cli}'] + arg_val)

    add_to_res_if_set("select")
    add_to_res_if_set("exclude")
    add_to_res_if_set("models")
    add_to_res_if_set("selector")
    add_to_res_if_set("resource_type", "resource-type")

    return ' '.join(res)



if __name__ == '__main__':
    args = parse_args()
    logger.info(f"Args: {args}")
    main(
        selector=build_dbt_selector_cli(args),
        dryrun=args.dryrun,
        full_refresh=args.full_refresh,
        retry=args.retry,
    )
