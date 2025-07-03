from framework.interfaces import ContextGenerator
import os

class CustomReviewContextGenerator(ContextGenerator):
    """Reads custom review criteria from AIBOT.md if it exists."""
    
    def requires(self):
        return {'repo_dir'}

    def provides(self):
        return {'custom_review_criteria'}

    def generate(self, context):
        repo_dir = context['repo_dir']
        aibot_path = os.path.join(repo_dir, 'AIBOT.md')
        
        if os.path.exists(aibot_path):
            with open(aibot_path, 'r') as f:
                content = f.read().strip()
                # Look for a section specifically for review criteria
                if '## Review Criteria' in content:
                    sections = content.split('## ')
                    for section in sections:
                        if section.startswith('Review Criteria'):
                            context['custom_review_criteria'] = section.replace('Review Criteria', '').strip()
                            return
                # If no specific section found, use the whole content
                context['custom_review_criteria'] = content
        else:
            context['custom_review_criteria'] = '' 