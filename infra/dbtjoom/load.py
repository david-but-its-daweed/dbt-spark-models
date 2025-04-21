import json
import os

import yaml

from infra.dbtjoom.types import DbtRunResults, DbtRunMetadata, DbtRunResult, Timing, DbtManifest, Node, DependsOn, \
    SparkThriftProfile


def load_dbt_run_results(file_path: str = 'target/run_results.json') -> DbtRunResults:
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return DbtRunResults(
        metadata=DbtRunMetadata(**data["metadata"]),
        results=[DbtRunResult(
            status=res.get('status'),
            timing=[Timing(**t) for t in res.get('timing', [])],
            thread_id=res.get('thread_id'),
            execution_time=res.get('execution_time'),
            adapter_response=res.get('adapter_response'),
            message=res.get('message'),
            failures=res.get('failures'),
            unique_id=res.get('unique_id'),
            compiled=res.get('compiled'),
            compiled_code=res.get('compiled_code'),
            relation_name=res.get('relation_name'),
        ) for res in data["results"]],
        elapsed_time=data["elapsed_time"],
        args=data["args"]
    )


def load_manifest(file_path: str = 'target/manifest.json') -> DbtManifest:
    with open(file_path, "r", encoding="utf-8") as f:
        manifest = json.load(f)

    nodes = {}
    for id_, node in manifest['nodes'].items():
        nodes[id_] = Node(
            name=node.get('name'),
            resource_type=node.get('resource_type'),
            package_name=node.get('package_name'),
            original_file_path=node.get('original_file_path'),
            path=node.get('path'),
            unique_id=node.get('unique_id'),
            alias=node.get('alias'),
            config=node.get('config'),
            tags=node.get('tags', []),
            depends_on=DependsOn(
                nodes=node.get('depends_on', {}).get('nodes', []),
                macros=node.get('depends_on', {}).get('macros', [])
            ),
            relation_name=node.get('relation_name'),
            schema=node.get('schema'),
            children=manifest['child_map'].get(id_, []),
        )

    return DbtManifest(nodes=nodes)


def load_spark_profile(file_path: str = '~/.dbt/profiles.yml', name: str = 'spark'):
    file_path = os.path.expanduser(file_path)
    with open(file_path, 'r') as file:
        data = yaml.safe_load(file)
    data = next(iter(data[name]['outputs'].values()))
    assert data['type'] == 'spark'
    assert data['method'] == 'thrift'
    return SparkThriftProfile(
        host=data['host'],
        port=int(data['port']),
        schema=data['schema'],
        threads=int(data['threads'])
    )
