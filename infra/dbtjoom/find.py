from typing import *

from infra.dbtjoom.invocation import ls
from infra.dbtjoom.load import load_manifest, load_dbt_run_results
from infra.dbtjoom.types import Node
from infra.dbtjoom.utils import get_changed_files


def find_children_models(model: Node, all_models: List[Node]) -> Set[str]:
    all_children_models = set()
    children_models = filter(lambda m: m.unique_id in model.children, all_models)
    for child in children_models:
        all_children_models.add(child.unique_id)
        all_children_models.update(find_children_models(child, all_models))
    return all_children_models


def fill_gaps_between_nodes(all_nodes: Dict[str, Node], node_unique_ids: Set[str]) -> Set[str]:
    """
    Add all intermediate nodes between nodes listed in node_unique_ids
    Use-case
    - we have a dependency path: A -> B -> C
    - we have changed A and C and requested to recalc them
    - need to recalculate the whole path including B, so this function must find this B
    """
    result = set(node_unique_ids)

    def dfs(current_id: str, target_ids: Set[str], visited_ids: Set[str], path: List[str]):
        if current_id in visited_ids:
            return

        visited_ids.add(current_id)
        path.append(current_id)

        if current_id in target_ids and current_id != path[0]:
            # found a path between two target nodes, add to result
            result.update(path)

        for child_id in all_nodes[current_id].children:
            if child_id in all_nodes:
                dfs(child_id, target_ids, visited_ids, path.copy())

    for src in node_unique_ids:
        dfs(src, node_unique_ids, set(), [])

    return result


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
        materialized: Optional[Union[str, Set[str]]] = None,
        changed_only: bool = False,
        select_dependencies: Optional[Literal['fill_gaps', 'all_children']] = None,
        failed_only: bool = False,
        profiles_dir: str = None,
) -> List[Node]:
    manifest = load_manifest()

    if select or exclude or models or model or resource_type:
        ids = ls(
            select=select,
            exclude=exclude,
            models=models,
            model=model,
            resource_type=resource_type,
            profiles_dir=profiles_dir
        )
    elif changed_only:
        # all nodes, will be filtered later
        ids = ls()
    else:
        # no selector specified
        return []

    if isinstance(materialized, str):
        materialized = {materialized}

    nodes = []
    for id_ in ids:
        if id_ in manifest.nodes and (materialized is None or manifest.nodes[id_].materialized in materialized):
            nodes.append(manifest.nodes[id_])

    if changed_only:
        changed_files = get_changed_files()
        nodes = [n for n in nodes if n.original_file_path in changed_files]

    if select_dependencies == 'fill_gaps':
        node_ids = {n.unique_id for n in nodes}
        node_ids = fill_gaps_between_nodes(manifest.nodes, node_ids)
        nodes = [manifest.nodes[_id] for _id in node_ids]

    elif select_dependencies == 'all_children':
        node_ids = set()
        for node in nodes:
            node_ids.update(find_children_models(node, list(manifest.nodes.values())))
        nodes = [manifest.nodes[_id] for _id in node_ids]

    if failed_only:
        failed = {res.unique_id for res in load_dbt_run_results().results if res.status != "success"}
        nodes = [n for n in nodes if n.unique_id in failed]

    return nodes
