from logging import getLogger
import subprocess

from git import Repo

logger = getLogger("utils")


def run(command, throw_on_fail: bool = True) -> str:
    logger.info(f"Run `{command}`")
    try:
        result = subprocess.run(
            command,
            shell=True,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT
        )
        return result.stdout.decode('utf8')
    except subprocess.CalledProcessError as e:
        if throw_on_fail:
            raise
        logger.error(f"Error executing command: {e}")



def get_changed_files():
    repo = Repo()
    changes = set()
    changes.update([diff.b_path for diff in repo.index.diff('origin/master')])
    changes.update([diff.b_path for diff in repo.index.diff(None)])
    changes.update([diff for diff in repo.untracked_files])
    return changes


