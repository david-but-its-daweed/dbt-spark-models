import json
from git import Repo
from typing import List, Dict, Set, Optional


class Node:
    unique_id: str
    name: str
    relation_name: str
    child_map: List[str]
    original_file_path: str
    prod_schema: str
    materialized: str

    def __init__(self, unique_id, name, relation_name, child_map, original_file_path, prod_schema, materialized):
        self.unique_id = unique_id
        self.name = name
        self.relation_name = relation_name
        self.child_map = child_map
        self.original_file_path = original_file_path
        self.prod_schema = prod_schema
        self.materialized = materialized


def get_changed_files():
    repo = Repo()
    changes = set()
    changes.update([diff.b_path for diff in repo.index.diff('origin/master')])
    changes.update([diff.b_path for diff in repo.index.diff(None)])
    changes.update([diff for diff in repo.untracked_files])
    return changes

def get_models_to_execute():
    with open("target/run_results.json") as run_results_file:
        run_results = json.load(run_results_file)
        model_ids = []
        for i in run_results['results']:
            model_ids.append(i['unique_id'])

        with open("target/manifest.json") as manifest_file:
            manifest = json.load(manifest_file)
            models = []
            for unique_id in model_ids:
                node = manifest['nodes'][unique_id]
                schema = node['config']['schema']
                models.append(Node(
                    unique_id = unique_id,
                    name = node['name'],
                    relation_name = node['relation_name'],
                    child_map = manifest['child_map'][unique_id],
                    original_file_path = node['original_file_path'],
                    prod_schema = schema if schema is not None else 'models',
                    materialized = node['config']['materialized']
                ))
            return models


def find_children_models(model: Node, all_models: List[Node]):
    all_children_models = set()
    children_models = filter(lambda m: m.unique_id in model.child_map, all_models)
    for child in children_models:
        all_children_models.add(child.unique_id)
        all_children_models.update(find_children_models(child, all_models))
    return all_children_models


def tables_to_copy_from_prod():
    models_to_execute = get_models_to_execute()
    changed_files = get_changed_files()
    changed_models = filter(lambda m: m.original_file_path in changed_files, models_to_execute)

    model_ids_to_rebuild = set()
    for changed_model in changed_models:
        model_ids_to_rebuild.add(changed_model.unique_id)
        model_ids_to_rebuild.update(find_children_models(changed_model, models_to_execute))


    tables_to_copy_from_prod = filter(lambda m:
                                      m.unique_id not in model_ids_to_rebuild and m.materialized in ['table', 'incremental'],
                                      models_to_execute)

    return ",".join(map(lambda t: f"{t.relation_name}:{t.prod_schema}.{t.name}", tables_to_copy_from_prod))


if __name__ == '__main__':
    print(tables_to_copy_from_prod())
