from typing import List, Dict, Set, Optional


def find_sources(manifest_file) -> List[str]:
    import json
    manifest = json.load(manifest_file)

    unused_sources = set(manifest['sources'].keys())

    for (key, node) in manifest['nodes'].items():
        for dependency in node.get('depends_on', {}).get('nodes', []):
            if dependency in unused_sources:
                unused_sources.remove(dependency)

    if len(unused_sources) > 0:
        print('Unused sources:')
        [print(it) for it in sorted(list(unused_sources))]
        exit(1)


if __name__ == '__main__':
    with open("target/manifest.json") as file:
        find_sources(file)
