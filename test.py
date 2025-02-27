import json
import sys
import requests
import itertools

BASE_URL = "https://platform-manager.joom.ai/api/fact_table_update/v1/manual_tables"

if __name__ == '__main__':
    args = sys.argv[1:]
    if len(args) != 3:
        raise Exception("Using: check_manial_tables.py [platform] [manual tables filepath] [manifest filepath]")
    
    platform = args[0]
    manual_tables_path = args[1]
    manifest_path = args[2]
    
    with open(manual_tables_path) as file:
        manual_tables = [line.rstrip() for line in file][1:]
    
    with open(manifest_path) as file:
        manifest = json.load(file)

    # Sources
    sources = set([f"{source['schema']}.{source['name']}" for source in manifest['sources'].values()])
    
    # All models that have childs tasks
    nodes = manifest['nodes']
    all_deps = [node['depends_on']['nodes'] for node in nodes.values() if 'depends_on' in node and 'nodes' in node['depends_on']]
    flatten_deps = list(itertools.chain(*all_deps))
    model_deps_nodes = [nodes[item] for item in flatten_deps if item.startswith("model.") and item in nodes]
    model_deps_tables = set([ f"{node['schema']}.{node['alias']}" for node in model_deps_nodes ])

    all_tables = sources.union(model_deps_tables)

    payload = {
        "platform": platform,
        "tables": list(all_tables.difference(manual_tables))
    }

    response = requests.post(BASE_URL, json=payload)
    if response.status_code != 404:
        print(f"The next table must be included in {manual_tables_path} file:")
        print("\n ".join(response.json()['tables']))
        exit(1)
