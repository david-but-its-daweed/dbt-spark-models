from typing import Optional, Set, Union, List

from infra.dbtjoom.invocation import ls
from infra.dbtjoom.load import load_manifest, load_dbt_run_results
from infra.dbtjoom.types import Node
from infra.dbtjoom.utils import get_changed_files


def find_children_models(model: Node, all_models: List[Node]):
    all_children_models = set()
    children_models = filter(lambda m: m.unique_id in model.children, all_models)
    for child in children_models:
        all_children_models.add(child.unique_id)
        all_children_models.update(find_children_models(child, all_models))
    return all_children_models


def is_changed(model: Node):
    files = get_changed_files()
    return model.original_file_path in files


def find_nodes(
        *,
        select: Optional[str] = None,
        exclude: Optional[str] = None,
        models: Optional[str] = None,
        model: Optional[str] = None,
        resource_type: Optional[str] = None,
        select_ids: Optional[Set[str]] = None,
        exclude_ids: Optional[Set[str]] = None,
        materialized: Optional[Union[str, Set[str]]] = None,
        changed_only: bool = False,
        failed_only: bool =  False
) -> List[Node]:
    manifest = load_manifest()
    if select or exclude or models or model or resource_type:
        ids = ls(
            select=select,
            exclude=exclude,
            models=models,
            model=model,
            resource_type=resource_type,
        )
    else:
        return []

    if select_ids is not None:
        ids = ids.intersection(select_ids)

    if exclude_ids is not None:
        ids = ids.difference(exclude_ids)

    if isinstance(materialized, str):
        materialized = {materialized}

    nodes = []
    for id_ in ids:
        if id_ in manifest.nodes and (materialized is None or manifest.nodes[id_].materialized in materialized):
            nodes.append(manifest.nodes[id_])

    if changed_only:
        changed_files = get_changed_files()
        nodes = [n for n in nodes if n.original_file_path in changed_files]

    if failed_only:
        failed = {res.unique_id for res in load_dbt_run_results().results if res.status != "success"}
        nodes = [n for n in nodes if n.unique_id in failed]

    return nodes
