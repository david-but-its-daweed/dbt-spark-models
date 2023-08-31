from typing import List, Dict, Set, Optional


def find_models(manifest_file) -> List[str]:
    import json
    manifest = json.load(manifest_file)

    print('Models with no owner:')
    for (key, node) in manifest['nodes'].items():
        if node['resource_type'] in ['model', 'snapshot']:
            if 'model_owner' not in node['meta']:
                print(node['unique_id'])



if __name__ == '__main__':
    with open("target/manifest.json") as file:

        find_models(file)
