from framework.interfaces import ContextGenerator
import subprocess
import os
import json

class DBTParseContextGenerator(ContextGenerator):
    def requires(self):
        return {'repo_dir'}

    def provides(self):
        return {'manifest_path', 'manifest_json'}

    def generate(self, context):
        if 'manifest_path' in context and 'manifest_json' in context:
            return  # Already parsed
        repo_dir = context['repo_dir']
        subprocess.run(["dbt", "deps"], cwd=repo_dir, check=False)
        subprocess.run(["dbt", "parse"], cwd=repo_dir, check=True)
        manifest_path = os.path.join(repo_dir, "target", "manifest.json")
        if not os.path.exists(manifest_path):
            raise FileNotFoundError("manifest.json not found after dbt parse")
        context['manifest_path'] = manifest_path
        # Also load and provide manifest_json
        with open(manifest_path, 'r') as f:
            context['manifest_json'] = json.load(f) 