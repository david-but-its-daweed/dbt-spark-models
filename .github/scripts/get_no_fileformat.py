from infra.dbtjoom.load import load_manifest
from itertools import groupby
import subprocess
from typing import *


def get_changed_files(base_branch="origin/master") -> List[str]:
    result = subprocess.run(
        ["git", "diff", "--name-only", "--diff-filter=AM", base_branch],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=True,
        text=True,
    )
    return result.stdout.strip().split("\n") if result.stdout else []


if __name__ == '__main__':
    manifest = load_manifest(r'target/manifest.json')
    changed_files = get_changed_files()

    def key(n):
        return 'unkn' if n.owner is None else n.owner

    bad_models_cnt = 0
    for owner, nodes in groupby(sorted(manifest.nodes.values(), key=key), key=key):
        first = True
        for node in nodes:
            ok = (
                    node.file_format is not None
                    or node.materialized == 'view'
                    or node.resource_type != 'model'
                    or node.original_file_path not in changed_files
            )

            if ok:
                continue

            if first:
                print(owner)
                first = False

            bad_models_cnt += 1
            print(f' - {node.name}, {node.resource_type}, {node.original_file_path}')

    exit(int(bad_models_cnt > 0))