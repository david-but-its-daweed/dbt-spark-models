from typing import List, Dict, Set, Optional

def find_models(manifest_file) -> List[str]:
    import json
    manifest = json.load(manifest_file)

    print('Models with no owner:')
    files = []
    for (key, node) in manifest['nodes'].items():
        if node['resource_type'] in ['model', 'snapshot']:
            if 'model_owner' not in node['meta']:
                filename = node['original_file_path']
                files.append(filename)
                print(filename)

    if len(files) > 0:
        exit(1)



if __name__ == '__main__':
    with open("target/manifest.json") as file:

        find_models(file)
