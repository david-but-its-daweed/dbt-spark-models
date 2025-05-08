import json
from typing import *

from dbt.cli.main import dbtRunner


def invoke(cmd, *args, **kwargs):
    cli_args = [cmd]
    cli_args.extend(args)
    for k, v in kwargs.items():
        cli_args.extend([f'--{k}', v])

    dbt = dbtRunner()
    return dbt.invoke(cli_args)


def retry(*args, **kwargs):
    return invoke('retry', *args, **kwargs)


def run(*args, **kwargs):
    return invoke('run', *args, **kwargs)

def compile(*args, **kwargs):
    return invoke('compile', *args, **kwargs)


def ls(
        *,
        select: Optional[str] = None,
        exclude: Optional[str] = None,
        models: Optional[str] = None,
        model: Optional[str] = None,
        resource_type: Optional[str] = None,
        profiles_dir: Optional[str] = None
) -> List[str]:
    args = ['ls', '--output', 'json']

    def _add(k, v):
        if v is not None:
            if not isinstance(v, list):
                v = [v]
            args.extend([k] + v)


    _add('--select', select)
    _add('--exclude', exclude)
    _add('--models', models)
    _add('--model', model)
    _add('--resource_type', resource_type)
    _add('--profiles-dir', profiles_dir)

    dbt = dbtRunner()
    result = dbt.invoke(args)
    for r in result.result:
        yield json.loads(r)['unique_id']



