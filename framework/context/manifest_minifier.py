from framework.interfaces import ContextGenerator
import json
import os

class ManifestSummaryContextGenerator(ContextGenerator):
    def requires(self):
        return {'manifest_path'}

    def provides(self):
        return {'manifest_summary'}

    def generate(self, context):
        manifest_path = context['manifest_path']
        with open(manifest_path, 'r') as f:
            manifest = json.load(f)
        models = [
            {
                'name': m.get('name', ''),
                'schema': m.get('schema', ''),
                'documented': bool(m.get('description'))
            }
            for m in manifest.get('nodes', {}).values()
            if m.get('resource_type') == 'model' and m.get('schema', '').lower() != 'dbtlogs'
        ]
        schemas = {}
        doc_completeness = {}
        for m in models:
            schema = m['schema']
            schemas[schema] = schemas.get(schema, 0) + 1
            if schema not in doc_completeness:
                doc_completeness[schema] = {'documented': 0, 'undocumented': 0}
            if m['documented']:
                doc_completeness[schema]['documented'] += 1
            else:
                doc_completeness[schema]['undocumented'] += 1
        summary = {
            'model_counts': schemas,
            'documentation_completeness': doc_completeness
        }
        context['manifest_summary'] = json.dumps(summary) 
        print(f"Manifest summary: {len(context['manifest_summary'])}")