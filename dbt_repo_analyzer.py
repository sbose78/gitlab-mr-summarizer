#!/usr/bin/env python3
"""
GitLab Webhook Handler for Data Product Promotions
Processes MR changes and analyzes dbt manifest files using Gemini API
"""

import os
import json
import tempfile
import shutil
import subprocess
import hmac
import hashlib
from typing import Dict, List, Optional, Tuple
from pathlib import Path
import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta

import httpx
from fastapi import FastAPI, HTTPException, Request, BackgroundTasks, Header
from pydantic import BaseModel
import vertexai
from vertexai.generative_models import GenerativeModel, GenerationConfig
from google.oauth2 import service_account
import requests

# Configure logging
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Enable debug logging for httpx if debug mode
if LOG_LEVEL == "DEBUG":
    logging.getLogger("httpx").setLevel(logging.DEBUG)

# Configuration
GITLAB_TOKEN = os.getenv("GITLAB_API_TOKEN")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
GEMINI_SERVICE_ACCOUNT = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
GEMINI_LOCATION = os.getenv("GEMINI_LOCATION", "us-central1")
GITLAB_WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")
GITLAB_BASE_URL = "https://gitlab.cee.redhat.com"
REPO_CLONE_COOLDOWN_MINUTES = int(os.getenv("REPO_CLONE_COOLDOWN_MINUTES", "1"))
DISABLE_MR_COMMENTS = os.getenv("DISABLE_MR_COMMENTS", "").lower() in ("true", "1", "yes")

if not GITLAB_TOKEN:
    raise ValueError("GITLAB_TOKEN environment variable is required")

if not GEMINI_API_KEY and not GEMINI_SERVICE_ACCOUNT:
    raise ValueError("Either GEMINI_API_KEY or GOOGLE_APPLICATION_CREDENTIALS environment variable is required")

if not GITLAB_WEBHOOK_SECRET:
    logger.warning("GITLAB_WEBHOOK_SECRET not set - webhook validation disabled (not recommended for production)")

if DISABLE_MR_COMMENTS:
    logger.info("MR comment updates are disabled - running in test mode")

logger.info(f"Repository clone cooldown set to {REPO_CLONE_COOLDOWN_MINUTES} minutes")

# Configure Gemini AI Client
if GEMINI_API_KEY:
    # Use API key authentication
    genai_client = genai.Client(api_key=GEMINI_API_KEY)
    logger.info("Using Gemini API key authentication")
else:
    # Use service account authentication
    credentials = service_account.Credentials.from_service_account_file(GEMINI_SERVICE_ACCOUNT)
    # Extract project ID from service account credentials
    with open(GEMINI_SERVICE_ACCOUNT) as f:
        service_account_info = json.load(f)
        project_id = service_account_info.get('project_id')
        if not project_id:
            raise ValueError("Project ID not found in service account credentials file")
    
    vertexai.init(project=project_id, location=GEMINI_LOCATION, credentials=credentials)
    model = GenerativeModel("models/gemini-2.5-flash-preview-05-20")
    generation_config = GenerationConfig(temperature=0.1, max_output_tokens=8000)
    logger.info(f"Using Vertex AI authentication for project {project_id}")

# Repository clone cooldown tracking
repo_clone_cache = {}

app = FastAPI(title="Data Product Promotion Handler")

# Add these constants at the top of the file (after imports)
PROMPT_MANIFEST_ANALYSIS = '''You must respond with ONLY a JSON object, no other text or explanation.

Analyze this dbt manifest file and provide a summary in JSON format:
{
    "model_counts": {
        "total": <int>,
        "by_schema": {"schema_name": <int>}
    },
    "documentation_completeness": {
        "by_schema": {"schema_name": {"documented": <int>, "undocumented": <int>}}
    }
}
- Only count models, do not list them.
- Skip models in the 'dbtlogs' schema.
- For documentation completeness, count documented and undocumented models/columns per schema.
- Respond with ONLY the JSON object, no other text.

Manifest Content:
{manifest_content}

Remember: Respond with ONLY the JSON object, no other text.'''

# Default prompt for manifest analysis: ask for Markdown tables directly
DEFAULT_PROMPT_MANIFEST_ANALYSIS = (
    'Respond ONLY with two Markdown tables:\n'
    '1. Model counts by schema (excluding dbtlogs)\n'
    '2. Documentation completeness by schema (documented/undocumented)\n'
    'Do not include any other text or explanation.\n'
    '\nManifest Content:\n{manifest_content}\n'
)

def load_manifest_prompt(manifest_content: str) -> str:
    # Always use the default prompt, no env var or external loading
    return DEFAULT_PROMPT_MANIFEST_ANALYSIS.format(manifest_content=manifest_content)

# Default prompt for CI analysis: ask for Markdown tables directly
DEFAULT_PROMPT_CI_ANALYSIS = (
    'Respond ONLY with a Markdown table summarizing the environments, tests, and deployments, and a section for metadata publishing. '
    'Do not include any other text or explanation.'
    '\nCI Configuration:\n{ci_content}\n'
)

def load_ci_prompt(ci_content: str) -> str:
    # Always use the default prompt, no env var or external loading
    return DEFAULT_PROMPT_CI_ANALYSIS.format(ci_content=ci_content)

MD_HEADING_PROMOTION = "## 🚀 Data Product Promotion Analysis"
MD_HEADING_MANIFEST = "### dbt Project Analysis"
MD_HEADING_CI = "### GitLab CI/CD Pipeline Analysis"
MD_HEADING_ENV_OVERVIEW = "#### 1. Environment Overview"
MD_HEADING_METADATA = "#### 2. Metadata Publishing"

@dataclass
class PromotionInfo:
    """Information about a data product promotion"""
    product_name: str
    product_type: str  # 'source' or 'aggregate'
    environment: str   # 'prod' or 'pre-prod'
    dbt_repo_url: str
    mr_iid: int
    project_id: int


class WebhookPayload(BaseModel):
    """GitLab webhook payload model"""
    object_kind: str
    project: Dict
    object_attributes: Dict
    changes: Optional[Dict] = None


class GitLabAPI:
    """GitLab API client"""
    
    def __init__(self, token: str, base_url: str):
        self.token = token
        self.base_url = base_url
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
    
    async def get_mr_changes(self, project_id: int, mr_iid: int) -> List[Dict]:
        """Get MR file changes"""
        url = f"{self.base_url}/api/v4/projects/{project_id}/merge_requests/{mr_iid}/changes"
        logger.debug(f"Fetching MR changes from: {url}")
        
        async with httpx.AsyncClient(verify=False) as client:
            response = await client.get(url, headers=self.headers)
            logger.debug(f"GitLab API response status: {response.status_code}")
            
            if response.status_code != 200:
                logger.error(f"Failed to get MR changes: {response.text}")
                raise HTTPException(status_code=response.status_code, 
                                  detail=f"Failed to get MR changes: {response.text}")
            
            changes = response.json().get("changes", [])
            logger.debug(f"Found {len(changes)} file changes")
            return changes
    
    async def create_or_update_comment(self, project_id: int, mr_iid: int, content: str):
        """Create or update a comment on the MR"""
        if DISABLE_MR_COMMENTS:
            logger.info(f"[TEST MODE] Would have posted comment to MR {mr_iid}:\n{content}")
            return

        # First, try to find existing bot comment
        existing_comment = await self._find_bot_comment(project_id, mr_iid)
        
        if existing_comment:
            logger.debug(f"Found existing comment with ID: {existing_comment['id']}")
            await self._update_comment(project_id, mr_iid, existing_comment["id"], content)
        else:
            logger.debug("No existing comment found, creating new one")
            await self._create_comment(project_id, mr_iid, content)
    
    async def _find_bot_comment(self, project_id: int, mr_iid: int) -> Optional[Dict]:
        """Find existing bot comment"""
        url = f"{self.base_url}/api/v4/projects/{project_id}/merge_requests/{mr_iid}/notes"
        
        async with httpx.AsyncClient(verify=False) as client:
            response = await client.get(url, headers=self.headers)
            if response.status_code == 200:
                notes = response.json()
                for note in notes:
                    if note.get("body", "").startswith("## 🚀 Data Product Promotion Analysis"):
                        return note
        return None
    
    async def _create_comment(self, project_id: int, mr_iid: int, content: str):
        """Create a new comment"""
        url = f"{self.base_url}/api/v4/projects/{project_id}/merge_requests/{mr_iid}/notes"
        data = {"body": content}
        
        async with httpx.AsyncClient(verify=False) as client:
            response = await client.post(url, headers=self.headers, json=data)
            if response.status_code not in [200, 201]:
                logger.error(f"Failed to create comment: {response.text}")
    
    async def _update_comment(self, project_id: int, mr_iid: int, note_id: int, content: str):
        """Update existing comment"""
        url = f"{self.base_url}/api/v4/projects/{project_id}/merge_requests/{mr_iid}/notes/{note_id}"
        data = {"body": content}
        
        async with httpx.AsyncClient(verify=False) as client:
            response = await client.put(url, headers=self.headers, json=data)
            if response.status_code not in [200, 201]:
                logger.error(f"Failed to update comment: {response.text}")
                logger.debug(f"Update comment URL: {url}")
                logger.debug(f"Response status: {response.status_code}")
                # If update fails, try creating a new comment instead
                logger.info("Update failed, creating new comment instead")
                await self._create_comment(project_id, mr_iid, content)
            else:
                logger.debug(f"Successfully updated comment {note_id}")

    async def get_project_info(self, project_id: int) -> Optional[Dict]:
        """Get project information"""
        url = f"{self.base_url}/api/v4/projects/{project_id}"
        logger.debug(f"Fetching project information from: {url}")
        
        async with httpx.AsyncClient(verify=False) as client:
            response = await client.get(url, headers=self.headers)
            logger.debug(f"GitLab API response status: {response.status_code}")
            
            if response.status_code != 200:
                logger.error(f"Failed to get project information: {response.text}")
                return None
            
            project_info = response.json()
            logger.debug(f"Retrieved project information: {project_info}")
            return project_info


class GeminiAnalyzer:
    """Gemini AI analyzer for MR changes and dbt manifests"""
    
    def __init__(self, model: GenerativeModel, config: GenerationConfig):
        self.model = model
        self.config = config
    
    async def analyze_mr_for_promotion(self, changes: List[Dict]) -> Optional[PromotionInfo]:
        """Analyze MR changes to detect data product promotions"""
        try:
        # Extract file paths from changes
        file_changes = []
        for change in changes:
            if change.get("new_file") or change.get("renamed_file"):
                file_path = change.get("new_path", change.get("old_path", ""))
                file_changes.append({
                    "path": file_path,
                        "content": change.get("diff", "")
                    })
            
            prompt = f"""You must respond with ONLY a JSON object, no other text or explanation.

            Analyze these file changes from a GitLab merge request and determine if this is a data product promotion.
            Look for patterns like:
            1. Changes to dbt models in marts directory
            2. Changes to CI/CD configuration
            3. Changes to documentation
            
            File Changes:
        {json.dumps(file_changes, indent=2)}

            If this is a data product promotion, respond with ONLY this JSON:
            {{
                "is_promotion": true,
                "product_name": "name of the data product",
                "product_type": "source-aligned or aggregate",
                "environment": "prod or pre-prod"
            }}
            
            If this is not a data product promotion, respond with ONLY this JSON:
        {{
                "is_promotion": false
            }}
            
            Remember: Respond with ONLY the JSON object, no other text."""
            
            logger.debug("Sending analysis request to Gemini API")
            response = await asyncio.to_thread(
                self.model.generate_content,
                prompt,
                generation_config=self.config
            )
            logger.debug(f"Gemini API response: {response.text[:200]}...")
            
            try:
                # Clean the response to ensure it's only JSON
                cleaned_response = response.text.strip()
                # Remove any markdown code block markers
                cleaned_response = cleaned_response.replace('```json', '').replace('```', '').strip()
                result = json.loads(cleaned_response)
                if result.get("is_promotion"):
                    # Log the raw product type from Gemini
                    logger.info(f"Raw product type from Gemini: {result['product_type']}")
                    
                    # Normalize product type
                    product_type = result["product_type"].lower().strip()
                    if "source" in product_type:
                        product_type = "source-aligned"
                    elif "aggregate" in product_type:
                        product_type = "aggregate"
                else:
                        logger.error(f"Unknown product type: {product_type}")
                        return None
                
                return PromotionInfo(
                        product_name=result["product_name"],
                    product_type=product_type,
                    environment=result["environment"],
                        dbt_repo_url="",  # Will be set later
                        mr_iid=0,  # Will be set later
                        project_id=0  # Will be set later
                )
            except json.JSONDecodeError:
                logger.error(f"Failed to parse Gemini response as JSON: {response.text}")
        
        return None
    
        except Exception as e:
            logger.error(f"Error analyzing MR with Gemini: {str(e)}")
            return None
        
    async def analyze_dbt_manifest(self, manifest_path: str) -> str:
        """Pre-process manifest in Python, send only summary to Gemini for Markdown tables. No section header in output."""
        try:
            import json
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
            prompt = (
                'Respond ONLY with two Markdown tables:\n'
                '1. Model counts by schema (excluding dbtlogs)\n'
                '2. Documentation completeness by schema (documented/undocumented)\n'
                'Do not include any other text or explanation.\n\n'
                f"Summary data: {json.dumps(summary, indent=2)}\n"
            )
            response = await asyncio.to_thread(
                self.model.generate_content,
                prompt,
                generation_config=self.config
            )
            # Do not add section header here
            return response.text.strip() + "\n"
        except (json.JSONDecodeError, FileNotFoundError) as e:
            logger.error(f"Error reading or parsing manifest file: {str(e)}")
            return "Error: Could not read or parse manifest file"
        except Exception as e:
            logger.error(f"Error analyzing manifest: {str(e)}")
            return f"Error: Failed to analyze manifest - {str(e)}"
    
    async def analyze_gitlab_ci(self, ci_file_path: str) -> str:
        """Detect environments, summarize tests/deployments per environment and metadata publishing with Gemini. No section header in output."""
        try:
                import yaml
            import json
            with open(ci_file_path, 'r') as f:
                ci_content = yaml.safe_load(f)
            env_jobs = {}
            metadata_jobs = {}
            for job_name, job in ci_content.items():
                if not isinstance(job, dict):
                    continue
                env = job.get('environment')
                if isinstance(env, dict):
                    env = env.get('name', '')
                if not isinstance(env, str):
                    env = str(env)
                script = job.get('script', [])
                if isinstance(script, list):
                    script = '\n'.join(script)
                if not env:
                    lowered = job_name.lower()
                    if 'dev' in lowered:
                        env = 'dev'
                    elif 'preprod' in lowered or 'pre-prod' in lowered:
                        env = 'pre-prod'
                if env:
                    if env not in env_jobs:
                        env_jobs[env] = {'tests': [], 'deployments': []}
                    if 'dbt test' in script or 'sqlfluff' in script:
                        env_jobs[env]['tests'].append({'job_name': job_name, 'script': script})
                    if 'dbt run' in script or 'deploy' in script:
                        env_jobs[env]['deployments'].append({'job_name': job_name, 'script': script})
                if 'atlan-dbt-artifact' in script or 'metadata' in job_name.lower() or 'artifact' in job_name.lower():
                    metadata_jobs[job_name] = script
            env_summaries = {}
            for env, jobs in env_jobs.items():
                if not env or env.lower() == 'none':
                    continue
                test_scripts = '\n\n'.join([f"Job: {j['job_name']}\n{j['script']}" for j in jobs['tests']])
                import yaml as _yaml
                deploy_defs = '\n\n'.join([
                    f"Job: {j['job_name']}\n" + _yaml.dump(ci_content[j['job_name']], default_flow_style=False)
                    for j in jobs['deployments'] if j['job_name'] in ci_content
                ])
                test_summary = ''
                deploy_summary = ''
                if test_scripts:
                    test_prompt = (
                        'Summarize the following CI job scripts as a single short description of the tests performed '
                        '(e.g., dbt test, data quality, unit, linting). '
                        'Do not include job names, just a readable summary.'
                        f'\n\nScripts:\n{test_scripts}\n'
                    )
                    test_response = await asyncio.to_thread(
                        self.model.generate_content,
                        test_prompt,
                        generation_config=self.config
                    )
                    test_summary = test_response.text.strip()
                if deploy_defs:
                    deploy_prompt = (
                        'Summarize the following CI job definitions as a single short description of the deployments performed '
                        '(e.g., dbt run, documentation generation, deployment target). '
                        'For each deployment, determine if it is manual or automated based on the job definition (e.g., presence of "when: manual"). '
                        'Include this information in the summary. Do not include job names, just a readable summary.'
                        f'\n\nJob definitions:\n{deploy_defs}\n'
                    )
                    deploy_response = await asyncio.to_thread(
                        self.model.generate_content,
                        deploy_prompt,
                        generation_config=self.config
                    )
                    deploy_summary = deploy_response.text.strip()
                env_summaries[env] = {
                    'tests': test_summary,
                    'deployments': deploy_summary
                }
            metadata_summary = ''
            if metadata_jobs:
                meta_prompt = (
                    'Summarize in 1-2 sentences how the following CI jobs handle metadata publishing or artifact upload. '
                    'Focus on what files are uploaded and to where (e.g., S3, Atlan, etc).'
                    f'\n\nJobs:\n' + '\n\n'.join([f"Job: {name}\n{script}" for name, script in metadata_jobs.items()]) + '\n'
                )
                meta_response = await asyncio.to_thread(
                    self.model.generate_content,
                    meta_prompt,
                    generation_config=self.config
                )
                metadata_summary = meta_response.text.strip()
            output = "### Testing and Deployment\n\n"
            output += "| Environment | Tests (e.g., dbt test, data quality, unit) | Deployments (Automated/Manual) |\n|-------------|---------------------------------------------|-------------------------------|\n"
            for env, summary in env_summaries.items():
                output += f"| {env} | {summary['tests']} | {summary['deployments']} |\n"
            output += "\n"
            output += f"### Metadata Publishing\n\n{metadata_summary}\n"
            return output
        except (yaml.YAMLError, FileNotFoundError) as e:
            logger.error(f"Error reading or parsing CI file: {str(e)}")
            return "Error: Could not read or parse CI configuration file"
        except Exception as e:
            logger.error(f"Error analyzing CI with Gemini: {str(e)}")
            return f"Error: Failed to analyze CI configuration - {str(e)}"


class DBTRunner:
    """Handle dbt operations"""
    
    @staticmethod
    def is_repo_in_cooldown(repo_url: str) -> bool:
        """Check if repository is in cooldown period"""
        if repo_url not in repo_clone_cache:
            return False
        
        last_clone_time = repo_clone_cache[repo_url]
        cooldown_period = timedelta(minutes=REPO_CLONE_COOLDOWN_MINUTES)
        
        if datetime.now() - last_clone_time < cooldown_period:
            remaining_time = cooldown_period - (datetime.now() - last_clone_time)
            logger.info(f"Repository {repo_url} is in cooldown for {remaining_time.total_seconds():.0f} more seconds")
            return True
        
        return False
    
    @staticmethod
    def update_repo_clone_time(repo_url: str):
        """Update the last clone time for a repository"""
        repo_clone_cache[repo_url] = datetime.now()
        logger.debug(f"Updated clone time for {repo_url}")
    
    @staticmethod
    async def clone_and_parse(repo_url: str, gitlab_token: str = None) -> Tuple[bool, str, Optional[str], Optional[str]]:
        """Clone repo and run dbt parse, return (success, message, manifest_path, ci_file_path)"""
        
        # Validate repository URL
        if not repo_url or not repo_url.strip():
            logger.error("Repository URL is empty")
            return False, "Repository URL is empty", None, None
            
        # Log repository URL details
        logger.info(f"Attempting to clone repository:")
        logger.info(f"  URL: {repo_url}")
        logger.info(f"  Using GitLab token: {'Yes' if gitlab_token else 'No'}")
        
        # Check if repository is in cooldown period
        if DBTRunner.is_repo_in_cooldown(repo_url):
            cooldown_remaining = REPO_CLONE_COOLDOWN_MINUTES - (
                (datetime.now() - repo_clone_cache[repo_url]).total_seconds() / 60
            )
            cooldown_message = f"Repository clone skipped due to cooldown. Please wait {cooldown_remaining:.1f} more minutes before retrying."
            logger.warning(f"Clone attempt blocked by cooldown: {repo_url}")
            return False, cooldown_message, None, None
        
        temp_dir = None
        try:
            # Update clone time at the start of the process
            DBTRunner.update_repo_clone_time(repo_url)
            
            # Create temporary directory in /tmp (mounted as emptyDir in OpenShift)
            temp_dir = tempfile.mkdtemp(prefix="dbt_", dir="/tmp")
            logger.info(f"Created temporary directory: {temp_dir}")
            
            # Clone repository with timeout (disable SSL verification)
            clone_cmd = ["git", "-c", "http.sslVerify=false", "clone", "--depth", "1", repo_url, temp_dir]
            logger.info(f"Cloning repository: {repo_url}")
            
            result = await asyncio.to_thread(
                subprocess.run, 
                clone_cmd, 
                capture_output=True, 
                text=True, 
                timeout=300  # 5 minute timeout
            )
            
            if result.returncode != 0:
                error_msg = f"Git clone failed: {result.stderr}"
                logger.error(error_msg)
                return False, error_msg, None, None
            
            logger.info(f"Successfully cloned repository to {temp_dir}")
            
            # Check for .gitlab-ci.yml file
            ci_file_path = None
            for ci_filename in [".gitlab-ci.yml", ".gitlab-ci.yaml"]:
                potential_path = os.path.join(temp_dir, ci_filename)
                if os.path.exists(potential_path):
                    ci_file_path = potential_path
                    logger.info(f"Found GitLab CI file: {ci_filename}")
                    break
            
            if not ci_file_path:
                logger.debug("No .gitlab-ci.yml file found in repository")
            
            # Set up dbt environment with comprehensive SSL bypass
            env = os.environ.copy()
            env["DBT_PROFILES_DIR"] = temp_dir
            # Disable SSL verification for any operations that might need it
            env["PYTHONHTTPSVERIFY"] = "0"
            env["CURL_CA_BUNDLE"] = ""
            env["REQUESTS_CA_BUNDLE"] = ""
            env["SSL_VERIFY"] = "false"
            env["GIT_SSL_NO_VERIFY"] = "true"
            
            # Configure git SSL settings in this session
            git_configs = [
                ["git", "config", "http.sslVerify", "false"],
                ["git", "config", "http.sslCAInfo", ""],
                ["git", "config", "http.sslCAPath", ""],
                ["git", "config", "http.sslCert", ""],
                ["git", "config", "http.sslKey", ""],
                ["git", "config", "http.sslCertPasswordProtected", "false"]
            ]
            
            for config_cmd in git_configs:
                try:
                    await asyncio.to_thread(
                        subprocess.run, 
                        config_cmd, 
                        cwd=temp_dir, 
                        capture_output=True, 
                        text=True,
                        timeout=30
                    )
                    logger.debug(f"Applied git config: {' '.join(config_cmd)}")
                except Exception as e:
                    logger.debug(f"Git config failed (continuing): {e}")
            
            # Check dbt_project.yml to see what profile is expected
            dbt_project_path = os.path.join(temp_dir, "dbt_project.yml")
            expected_profile = "default"  # fallback
            
            if os.path.exists(dbt_project_path):
                try:
                    import yaml
                    with open(dbt_project_path, 'r') as f:
                        dbt_project = yaml.safe_load(f)
                        expected_profile = dbt_project.get('profile', 'default')
                        logger.debug(f"Found dbt profile in project: {expected_profile}")
                except Exception as e:
                    logger.debug(f"Could not read dbt_project.yml: {e}")
            
            # Create profiles.yml with the expected profile name (Snowflake adapter)
            profiles_content = f"""
{expected_profile}:
  outputs:
    dev:
      type: snowflake
      account: dummy_account
      user: dummy_user
      password: dummy_password
      role: dummy_role
      database: dummy_database
      warehouse: dummy_warehouse
      schema: public
      threads: 1
  target: dev

default:
  outputs:
    dev:
      type: snowflake
      account: dummy_account
      user: dummy_user
      password: dummy_password
      role: dummy_role
      database: dummy_database
      warehouse: dummy_warehouse
      schema: public
      threads: 1
  target: dev

local:
  outputs:
    dev:
      type: snowflake
      account: dummy_account
      user: dummy_user
      password: dummy_password
      role: dummy_role
      database: dummy_database
      warehouse: dummy_warehouse
      schema: public
      threads: 1
  target: dev
"""
            profiles_path = os.path.join(temp_dir, "profiles.yml")
            with open(profiles_path, 'w') as f:
                f.write(profiles_content)
            
            # Run dbt deps first to install package dependencies
            logger.info("Running dbt deps to install package dependencies...")
            deps_cmd = ["dbt", "deps", "--profiles-dir", temp_dir]
            result = await asyncio.to_thread(
                subprocess.run, 
                deps_cmd, 
                cwd=temp_dir, 
                capture_output=True, 
                text=True, 
                env=env,
                timeout=300  # 5 minute timeout for deps
            )
            
            if result.returncode != 0:
                logger.warning(f"dbt deps failed (continuing anyway): {result.stderr}")
                # Don't fail here - some projects might not need deps or have issues
                # but we can still try to parse
            else:
                logger.info("dbt deps completed successfully")
            
            # Run dbt parse with timeout
            parse_cmd = ["dbt", "parse", "--profiles-dir", temp_dir]
            logger.info("Running dbt parse...")
            result = await asyncio.to_thread(
                subprocess.run, 
                parse_cmd, 
                cwd=temp_dir, 
                capture_output=True, 
                text=True, 
                env=env,
                timeout=600  # 10 minute timeout
            )
            
            if result.returncode != 0:
                logger.error(f"dbt parse failed: {result.stderr}")
                logger.debug(f"dbt parse stdout: {result.stdout}")
                return False, f"dbt parse failed: {result.stderr}", None, None
            
            logger.info("dbt parse completed successfully")
            
            # Check for manifest.json
            manifest_path = os.path.join(temp_dir, "target", "manifest.json")
            if not os.path.exists(manifest_path):
                logger.error(f"manifest.json not found at {manifest_path}")
                return False, "manifest.json not found after dbt parse", None, None
            
            logger.info(f"Found manifest.json at {manifest_path}")
            return True, "dbt parse successful", manifest_path, ci_file_path
            
        except subprocess.TimeoutExpired:
            logger.error("dbt operations timed out")
            return False, "dbt operations timed out", None, None
        except Exception as e:
            logger.error(f"Error in dbt operations: {str(e)}")
            return False, f"Error in dbt operations: {str(e)}", None, None
        finally:
            # Cleanup will happen after manifest analysis
            pass


async def process_promotion(promotion: PromotionInfo, gitlab_api: GitLabAPI):
    """Process a data product promotion"""
    
    logger.info(f"Processing promotion for {promotion.product_name} to {promotion.environment}")
    
    # Update MR with initial status
    initial_comment = f"""## 🚀 Data Product Promotion Analysis

**Product**: {promotion.product_name}  
**Type**: {promotion.product_type}-aligned  
**Environment**: {promotion.environment}  

Cloning dbt repository and running analysis...
"""
    
    await gitlab_api.create_or_update_comment(
        promotion.project_id, promotion.mr_iid, initial_comment
    )
    
    # Clone repo and run dbt parse (GitLab token only used for API calls)
    success, message, manifest_path, ci_file_path = await DBTRunner.clone_and_parse(
        promotion.dbt_repo_url
    )
    
    if not success:
        error_comment = f"""## 🚀 Data Product Promotion Analysis

**Product**: {promotion.product_name}  
**Type**: {promotion.product_type}-aligned  
**Environment**: {promotion.environment}  

**Error**: {message}
"""
        await gitlab_api.create_or_update_comment(
            promotion.project_id, promotion.mr_iid, error_comment
        )
        return
    
    # Analyze manifest with Gemini
    analyzer = GeminiAnalyzer(model, generation_config)
    manifest_summary = await analyzer.analyze_dbt_manifest(manifest_path)
    
    # Analyze GitLab CI if present
    ci_summary = ""
    if ci_file_path:
        ci_summary = await analyzer.analyze_gitlab_ci(ci_file_path)
        ci_section = f"""
### GitLab CI/CD Pipeline Analysis

{ci_summary}
"""
    else:
        ci_section = """

⚠️ No `.gitlab-ci.yml` file found in the repository
"""
    
    # Clean up temporary directory
    temp_dir = os.path.dirname(os.path.dirname(manifest_path))
    shutil.rmtree(temp_dir, ignore_errors=True)
    
    # Create final comment
    final_comment = f"""## 🚀 Data Product Promotion Analysis

**Product**: {promotion.product_name}  
**Type**: {promotion.product_type}-aligned  
**Environment**: {promotion.environment}  

### dbt Project Analysis

{manifest_summary}

{ci_section}

---
*Analysis completed by Data Product Promotion Bot*
"""
    
    await gitlab_api.create_or_update_comment(
        promotion.project_id, promotion.mr_iid, final_comment
    )


def verify_gitlab_signature(payload_body, signature_header):
    """Verify GitLab webhook signature."""
    if not GITLAB_WEBHOOK_SECRET:
        logger.warning("No webhook secret configured - skipping signature verification")
        return True
    
    if not signature_header:
        logger.error("No X-Gitlab-Token header found")
        return False
    
    # GitLab sends the token directly, not as HMAC
    return hmac.compare_digest(signature_header, GITLAB_WEBHOOK_SECRET)


@app.post("/webhook/gitlab")
async def handle_gitlab_webhook(
    request: Request, 
    background_tasks: BackgroundTasks,
    x_gitlab_token: Optional[str] = Header(None)
):
    """Handle GitLab webhook for MR events"""
    
    try:
        # Get raw payload for signature verification
        payload_bytes = await request.body()

        logger.debug(f"Received GitLab webhook payload of size {len(payload_bytes)} bytes")
        
        # Verify webhook signature if secret is configured
        if GITLAB_WEBHOOK_SECRET:
            if not x_gitlab_token:
                raise HTTPException(status_code=401, detail="Missing X-Gitlab-Token header")
            
            if not verify_gitlab_signature(payload_bytes, GITLAB_WEBHOOK_SECRET):
                raise HTTPException(status_code=401, detail="Invalid webhook signature")
        logger.debug("Webhook signature verified successfully")
        
        # Parse JSON payload
        try:
            payload = json.loads(payload_bytes.decode('utf-8'))
        except json.JSONDecodeError:
            raise HTTPException(status_code=400, detail="Invalid JSON payload")
        
        # Log full payload structure for debugging
        logger.info("Webhook payload structure:")
        logger.info(f"  Object kind: {payload.get('object_kind', 'unknown')}")
        logger.info(f"  Project data: {json.dumps(payload.get('project', {}), indent=2)}")
        logger.info(f"  Object attributes: {json.dumps(payload.get('object_attributes', {}), indent=2)}")

        logger.debug(f"Parsed GitLab webhook payload: {payload.get('object_kind', 'unknown')} event")
        
        # Only process merge request events
        if payload.get("object_kind") != "merge_request":
            return {"status": "ignored", "reason": "not a merge request event"}
        
        logger.debug("Processing merge request event")  
        # Only process opened or updated MRs
        mr_action = payload.get("object_attributes", {}).get("action")
        if mr_action not in ["open", "update"]:
            return {"status": "ignored", "reason": f"action '{mr_action}' not relevant"}
        
        logger.debug(f"Merge request action: {mr_action}")
        
        project_id = payload["project"]["id"]
        mr_iid = payload["object_attributes"]["iid"]
        
        # Get MR changes
        gitlab_api = GitLabAPI(GITLAB_TOKEN, GITLAB_BASE_URL)
        changes = await gitlab_api.get_mr_changes(project_id, mr_iid)

        logger.debug(f"Retrieved {len(changes)} changes for MR {mr_iid} in project {project_id}")
        
        # Analyze changes with Gemini first to get product information
        analyzer = GeminiAnalyzer(model, generation_config)
        promotion = await analyzer.analyze_mr_for_promotion(changes)

        logger.debug(f"Promotion analysis result: {promotion}")
        
        if not promotion:
            return {"status": "ignored", "reason": "no data product promotion detected"}
        
        # Set MR details
        promotion.mr_iid = mr_iid
        promotion.project_id = project_id
        
        # Construct the dbt repository URL based on product type
        base_url = "https://gitlab.cee.redhat.com/dataverse/data-products"
        # Map product type to correct path
        product_type_path = "source-aligned" if promotion.product_type == "source-aligned" else "aggregate"
        repo_url = f"{base_url}/{product_type_path}/{promotion.product_name}/{promotion.product_name}-dbt"
        
        # Set the repository URL
        promotion.dbt_repo_url = repo_url
        
        # Log repository URL details
        logger.info(f"Repository URL details:")
        logger.info(f"  Product name: {promotion.product_name}")
        logger.info(f"  Product type: {promotion.product_type}")
        logger.info(f"  Product type path: {product_type_path}")
        logger.info(f"  Full URL: {repo_url}")
        logger.info(f"  Project ID: {project_id}")
        logger.info(f"  MR IID: {mr_iid}")
        
        # Process promotion in background
        background_tasks.add_task(process_promotion, promotion, gitlab_api)
        
        return {
            "status": "processing",
            "product": promotion.product_name,
            "environment": promotion.environment
        }
        
    except Exception as e:
        logger.error(f"Error processing webhook: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "service": "data-product-promotion-handler"}


if __name__ == "__main__":
    import uvicorn
    
    # Get port from environment (OpenShift typically uses PORT env var)
    port = int(os.getenv("PORT", 8000))
    
    # Configure for production deployment
    uvicorn.run(
        app, 
        host="0.0.0.0", 
        port=port,
        workers=int(os.getenv("WORKERS", 1)),
        log_level=os.getenv("LOG_LEVEL", "info").lower(),
        access_log=True
    )