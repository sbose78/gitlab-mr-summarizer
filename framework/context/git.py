from framework.interfaces import ContextGenerator
import tempfile
import subprocess
import os

class GitCloneContextGenerator(ContextGenerator):
    def __init__(self, repo_url, branch=None):
        self.repo_url = repo_url
        self.branch = branch

    def requires(self):
        return set()

    def provides(self):
        return {'repo_dir'}

    def generate(self, context):
        if 'repo_dir' in context:
            return  # Already cloned
        temp_dir = tempfile.mkdtemp(prefix="repo_")
        clone_cmd = ["git", "clone", self.repo_url, temp_dir]
        if self.branch:
            clone_cmd += ["-b", self.branch]
        subprocess.run(clone_cmd, check=True)
        context['repo_dir'] = temp_dir

    def cleanup(self):
        if self.temp_dir and os.path.exists(self.temp_dir):
            import shutil
            shutil.rmtree(self.temp_dir) 