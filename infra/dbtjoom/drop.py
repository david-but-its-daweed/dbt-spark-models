from infra.dbtjoom.thrift_client import ThriftClient
from infra.dbtjoom.types import Node
from infra.dbtjoom.utils import logger
from infra.dbtjoom.s3 import aws_s3_rm


def drop_model_output(
        node: Node,
        client: ThriftClient,
        junk: bool,
        spark_staging_only: bool,
        include_incremental: bool,
        dryrun: bool = False
):
    if node.is_incremental and not include_incremental:
        logger.info(f"Model {node.unique_id} is incremental and --full-refresh is not specified. Skip dropping location")
        return

    if not node.is_table and not(node.is_incremental and include_incremental):
        logger.info(f"Model {node.unique_id} is not a table. Skip dropping location")
        return

    if junk and not node.relation_name.startswith("junk"):
        logger.info(f"Model {node.unique_id} writes into {node.relation_name} not junk*. Location will not be dropped")
        return

    table_location = client.get_table_location(node.relation_name)
    if table_location is None:
        db_location = client.get_db_location(node.schema)
        if db_location is None:
            logger.info(f"Model {node.unique_id}, can't determine table location. Location will not be dropped")
            return
        table_location = f'{db_location}/{node.table_name}'

    if junk and not table_location.startswith("s3://joom-analytics-users"):
        logger.info(
            f"Model {node.unique_id} writes into {table_location}, not joom-analytics-users. Location will not be dropped")
        return

    aws_s3_rm(table_location, dryrun=dryrun, spark_staging_only=spark_staging_only)
    client.drop_if_exists(node.relation_name)
