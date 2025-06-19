from framework.interfaces import ContextGenerator
import os
import yaml

class CISummaryContextGenerator(ContextGenerator):
    def requires(self):
        return {'repo_dir'}

    def provides(self):
        return {'ci_summary'}

    def generate(self, context):
        repo_dir = context['repo_dir']
        ci_path = None
        for fname in [".gitlab-ci.yml", ".gitlab-ci.yaml"]:
            candidate = os.path.join(repo_dir, fname)
            if os.path.exists(candidate):
                ci_path = candidate
                break
        if not ci_path:
            context['ci_summary'] = {}
            return
        with open(ci_path) as f:
            ci_content = yaml.safe_load(f)
        # Minimal summary: just pass the parsed YAML for now
        context['ci_summary'] = ci_content 