from infra.dbtjoom.utils import run
from logging import getLogger


logger = getLogger("s3")


def aws_s3_rm(location: str, dryrun: bool = True, spark_staging_only: bool = False):
    if spark_staging_only:
        to_delete = [
            '__magic',
            '.spark-staging-*'
        ]
        for d in to_delete:
            cmd = f"aws s3 rm --recursive --exclude '*' --include '{d}' {location} {'--dryrun' if dryrun else ''}"
            logger.warning(cmd)
            run(cmd)

    else:
        cmd = f"aws s3 rm --recursive {location} {'--dryrun' if dryrun else ''}"
        logger.warning(cmd)
        run(cmd)
