from dataclasses import dataclass
from typing import List, Dict, Optional


@dataclass
class Timing:
    name: str
    started_at: str
    completed_at: str

@dataclass
class DbtRunResult:
    status: str
    timing: List[Timing]
    thread_id: str
    execution_time: float
    adapter_response: str
    message: str
    failures: Optional[int]
    unique_id: str
    compiled: bool
    compiled_code: str
    relation_name: str

@dataclass
class DbtRunMetadata:
    dbt_schema_version: str
    dbt_version: str
    generated_at: str
    invocation_id: str
    env: Dict[str, str]

@dataclass
class DbtRunResults:
    metadata: DbtRunMetadata
    results: List[DbtRunResult]
    elapsed_time: float
    args: dict


@dataclass
class DependsOn:
    macros: List[str]
    nodes: List[str]

@dataclass
class Node:
    name: str
    resource_type: str
    package_name: str
    original_file_path: str
    path: str
    unique_id: str
    alias: str
    config: dict
    tags: List[str]
    depends_on: DependsOn
    relation_name: str
    schema: str
    children: str

    @property
    def owner(self):
        return self.config.get('meta', {}).get('model_owner')

    @property
    def materialized(self):
        return self.config['materialized']

    @property
    def is_table(self):
        return self.materialized == 'table'

    @property
    def is_view(self):
        return self.materialized == 'view'

    @property
    def is_incremental(self):
        return self.materialized == 'incremental'

    @property
    def file_format(self):
        return self.config.get('file_format')

    @property
    def table_name(self):
        return self.alias or self.config.get('alias') or self.name

    @property
    def is_model(self):
        return self.resource_type == 'model'


@dataclass
class DbtManifest:
    nodes: Dict[str, Node]


@dataclass
class SparkThriftProfile:
    host: str
    port: int
    schema: str
    threads: int
