import json
import os
from typing import Dict, List, Optional, Callable, Any
import requests
from urllib.parse import urljoin
import logging
import traceback
import urllib3
import tempfile
import subprocess
import re
from dataclasses import dataclass

from fastapi import FastAPI, Request, HTTPException, Header
from fastapi.responses import JSONResponse
import uvicorn

from framework.pipeline import Pipeline
from framework.context import (
    GitCloneContextGenerator,
    DBTParseContextGenerator,
    CISummaryContextGenerator,
)
from framework.analysis.gemini import SimpleLLMAnalyzer
from framework.notification import GitLabMRCommentNotifier
from framework.interfaces import ContextGenerator
from vertexai.generative_models import GenerativeModel, GenerationConfig
from framework.context.mr_changes import MRChangesContextGenerator
from framework.context.manifest_minifier import ManifestSummaryContextGenerator
from framework.context.git import GitCloneContextGenerator
from framework.config.prompts import get_prompt_for_repo

# Set dummy Snowflake environment variables
os.environ.setdefault("SNOWFLAKE_USER", "doesntmatter")
os.environ.setdefault("SNOWFLAKE_PASSWORD", "doesntmatter")
os.environ.setdefault("SNOWFLAKE_ACCOUNT", "doesntmatter")
os.environ.setdefault("SNOWFLAKE_ROLE", "doesntmatter")
os.environ.setdefault("SNOWFLAKE_DATABASE", "doesntmatter")
os.environ.setdefault("SNOWFLAKE_WAREHOUSE", "doesntmatter")
os.environ.setdefault("SNOWFLAKE_SCHEMA", "doesntmatter")
# Ensure all git operations use insecure HTTPS
os.environ["GIT_SSL_NO_VERIFY"] = "1"

logger = logging.getLogger("webhook_service")
logging.basicConfig(level=logging.INFO)

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

app = FastAPI(title="DBT Repo Analyzer (Framework)")

# GitLab API configuration
GITLAB_API_BASE_URL = os.getenv("GITLAB_API_BASE_URL", "https://gitlab.cee.redhat.com")

# Instantiate Gemini model and config once
model = GenerativeModel("models/gemini-2.0-flash")
generation_config = GenerationConfig(temperature=0.1, max_output_tokens=8000)

@dataclass
class PipelineConfig:
    gitlab_token: str
    project_id: str
    project_path: str
    mr_iid: str = ""

def create_promotion_detection_pipeline(config: PipelineConfig) -> Pipeline:
    """Create a pipeline for detecting product promotions."""
    promotion_prompt = get_prompt_for_repo(config.project_path, "promotion")
    return Pipeline(
        context_generators=[],
        analyzers=[SimpleLLMAnalyzer(model, generation_config, promotion_prompt, {"mr_changes"}, result_key="promotion_result")],
        notifiers=[],
    )

def create_mr_summary_pipeline(config: PipelineConfig) -> Pipeline:
    """Create a pipeline for general MR summary analysis."""
    repo_url = config.project_path
    mr_summary_prompt = get_prompt_for_repo(repo_url, "mr_summary")
    return Pipeline(
        context_generators=[],
        analyzers=[SimpleLLMAnalyzer(model, generation_config, mr_summary_prompt, {"mr_changes", "title", "description", "source_branch", "target_branch", "commit_sha", "diff_content", "project_id", "mr_iid"}, result_key="final_md")],
        notifiers=[
            GitLabMRCommentNotifier(
                config.gitlab_token,
                base_url=GITLAB_API_BASE_URL,
                marker="🤖 Gemini-Generated MR Summary",
            ),
        ],
    )

def get_mr_changes(project_id: str, mr_iid: str, token: str, mr_details: Dict = None) -> Dict:
    """
    Fetch merge request changes from GitLab API.
    
    Args:
        project_id: The GitLab project ID
        mr_iid: The merge request IID
        token: GitLab API token
        mr_details: Optional MR details from webhook payload
        
    Returns:
        Dict containing the changes in the merge request
        
    Raises:
        HTTPException: If the API call fails
    """
    logger.info(f"Getting MR changes for project {project_id} and MR {mr_iid}")
    headers = {
        "PRIVATE-TOKEN": token,
        "Content-Type": "application/json"
    }
    
    # Get MR diffs
    diffs_url = urljoin(GITLAB_API_BASE_URL, f"/api/v4/projects/{project_id}/merge_requests/{mr_iid}/diffs")
    logger.info(f"Fetching MR diffs from URL: {diffs_url}")
    #logger.debug(f"Headers: {headers}")
    try:
        diffs_response = requests.get(diffs_url, headers=headers, verify=False)
        logger.info(f"MR diffs response status code: {diffs_response.status_code}")
        diffs_response.raise_for_status()
        try:
            diffs = diffs_response.json()
            # Format the changes into a more usable structure
            formatted_changes = {
                "files": [
                    {
                        "path": change["new_path"],
                        "diff": change["diff"],
                        "status": change["new_file"] and "added" or change["deleted_file"] and "deleted" or "modified"
                    }
                    for change in diffs
                ],
                "source_branch": mr_details.get("source_branch") if mr_details else None,
                "target_branch": mr_details.get("target_branch") if mr_details else None,
                "title": mr_details.get("title") if mr_details else None,
                "description": mr_details.get("description") if mr_details else None
            }
            logger.info(f"Formatted changes: {len(formatted_changes['files'])}")
            return formatted_changes
        except Exception as json_exc:
            logger.error(f"Failed to decode JSON from GitLab diffs response: {diffs_response.text}")
            raise HTTPException(
                status_code=500,
                detail=f"Failed to decode JSON from GitLab diffs: {str(json_exc)}. Response: {diffs_response.text}"
            )
    except requests.exceptions.RequestException as e:
        logger.error(f"Network error while fetching MR changes: {e}")
        raise HTTPException(
            status_code=502,
            detail="Could not connect to GitLab to fetch MR changes. Please check network connectivity and try again."
        )
    except Exception as e:
        logger.error(f"Unexpected error while fetching MR changes: {e}")
        raise HTTPException(
            status_code=500,
            detail="An unexpected error occurred while fetching MR changes."
        )

def get_dbt_repo_url(product_name: str, product_type: str) -> str:
    """
    Generate the dbt repository URL for a given product name and type.
    """
    base_url = "https://gitlab.cee.redhat.com/dataverse/data-products"
    product_type_path = "source-aligned" if (product_type == "source-aligned" or product_type == "source") else "aggregate"
    return f"{base_url}/{product_type_path}/{product_name}/{product_name}-dbt"

def extract_json_from_llm_output(text):
    """Extract JSON from LLM output, removing Markdown code block if present."""
    match = re.search(r"```(?:json)?\s*([\s\S]+?)\s*```", text)
    if match:
        json_str = match.group(1)
    else:
        json_str = text
    return json.loads(json_str)

@dataclass
class MRDetails:
    source_branch: str = ""
    target_branch: str = ""
    title: str = ""
    description: str = ""
    sha: Optional[str] = None
    last_commit: Optional[Dict[str, Any]] = None

    @classmethod
    def from_object_attrs(cls, object_attrs):
        return cls(
            source_branch=object_attrs.get("source_branch", ""),
            target_branch=object_attrs.get("target_branch", ""),
            title=object_attrs.get("title", ""),
            description=object_attrs.get("description", ""),
            sha=object_attrs.get("sha"),
            last_commit=object_attrs.get("last_commit"),
        )

def build_mr_context(config: PipelineConfig, mr_details, get_mr_changes_func):
    mr_changes = get_mr_changes_func(config.project_id, config.mr_iid, config.gitlab_token)
    diff_content = json.dumps(mr_changes.get('files', []), indent=2)
    return {
        "title": mr_details.title,
        "description": mr_details.description,
        "source_branch": mr_details.source_branch,
        "target_branch": mr_details.target_branch,
        "commit_sha": mr_details.sha or (mr_details.last_commit or {}).get("id") or "N/A",
        "mr_changes": mr_changes,
        "diff_content": diff_content,
        "project_id": config.project_id,
        "mr_iid": config.mr_iid,
    }

def handle_mr_event(config: PipelineConfig, get_mr_changes_func: Callable, mr_details: Dict) -> None:
    try:
        context = build_mr_context(config, mr_details, get_mr_changes_func)
        mr_summary_pipeline = create_mr_summary_pipeline(config)
        logger.info("Running MR summary pipeline...")
        mr_summary_pipeline.run(context)
        logger.info(f"MR summary pipeline: {mr_summary_pipeline}")

        if not config.project_path.endswith("dataverse-config/dataproduct-config"):
            logger.info(f"Ignoring MR event for non-dataproduct-config repository: {config.project_path}")
            return
        promotion_pipeline = create_promotion_detection_pipeline(config)
        promotion_result = promotion_pipeline.run(context)
        logger.info(f"Promotion result: {promotion_result}")
        promotion_result_raw = promotion_result.get("promotion_result")
        if promotion_result_raw:
            try:
                promotion_data = extract_json_from_llm_output(promotion_result_raw)
            except Exception as e:
                logger.error(f"Failed to parse promotion_result: {e}")
                promotion_data = {}
        else:
            promotion_data = {}
        product_name = promotion_data.get("product_name")
        product_type = promotion_data.get("product_type")
        if not product_name or not product_type:
            raise ValueError("Product name or type not found in promotion result.")
        if promotion_data.get("is_promotion", False):
            logger.info(f"Promotion detected: {promotion_data}")
            repo_url = get_dbt_repo_url(product_name, product_type)
            manifest_prompt = get_prompt_for_repo(repo_url, "manifest")
            ci_prompt = get_prompt_for_repo(repo_url, "ci")
            context_generators = [
                GitCloneContextGenerator(repo_url),
                DBTParseContextGenerator(),
                ManifestSummaryContextGenerator(),
                CISummaryContextGenerator(),
            ]
            pipeline = Pipeline(
                context_generators=context_generators,
                analyzers=[
                    SimpleLLMAnalyzer(model, generation_config, manifest_prompt, {"manifest_summary"}, result_key="manifest_summary"),
                    SimpleLLMAnalyzer(model, generation_config, ci_prompt, {"ci_summary"}, result_key="ci_summary"),
                ],
                notifiers=[
                    GitLabMRCommentNotifier(
                        config.gitlab_token,
                        base_url=GITLAB_API_BASE_URL,
                        marker="🚀 Data Product Promotion Analysis",
                    ),
                ],
            )
            logger.info(f"Running pipeline: {pipeline}")
            try:
                pipeline.run(context)
            finally:
                for gen in context_generators:
                    if hasattr(gen, "cleanup"):
                        try:
                            gen.cleanup()
                        except Exception as cleanup_exc:
                            logger.warning(f"Cleanup failed for {gen}: {cleanup_exc}")
    except Exception as e:
        logger.error(f"Exception in handle_mr_event: {e}")
        logger.error(traceback.format_exc())
        raise

def handle_note_event(config: PipelineConfig, get_mr_changes_func: Callable, note_content: str, mr_details: Dict) -> None:
    try:
        project_path = getattr(config, "project_path", "")
        if not project_path.endswith("dataverse-config/dataproduct-config"):
            logger.info(f"Ignoring note event for non-dataproduct-config repository: {project_path}")
            return
        if note_content.strip() == "/analyze-promotion":
            context = build_mr_context(config, mr_details, get_mr_changes_func)
            promotion_pipeline = create_promotion_detection_pipeline(config)
            promotion_result = promotion_pipeline.run(context)
            if promotion_result.get("is_promotion", True):
                logger.info(f"Promotion detected: {promotion_result}")
                promotion_result_raw = promotion_result.get("promotion_result")
                if promotion_result_raw:
                    try:
                        promotion_data = extract_json_from_llm_output(promotion_result_raw)
                    except Exception as e:
                        logger.error(f"Failed to parse promotion_result: {e}")
                        promotion_data = {}
                else:
                    promotion_data = {}
                product_name = promotion_data.get("product_name")
                product_type = promotion_data.get("product_type")
                if not product_name or not product_type:
                    raise ValueError("Product name or type not found in promotion result.")
                repo_url = get_dbt_repo_url(product_name, product_type)
                manifest_prompt = get_prompt_for_repo(repo_url, "manifest")
                ci_prompt = get_prompt_for_repo(repo_url, "ci")
                context_generators = [
                    GitCloneContextGenerator(repo_url),
                    DBTParseContextGenerator(),
                    CISummaryContextGenerator(),
                    ManifestSummaryContextGenerator(),
                ]
                pipeline = Pipeline(
                    context_generators=context_generators,
                    analyzers=[
                        SimpleLLMAnalyzer(model, generation_config, manifest_prompt, {"manifest_summary"}, result_key="manifest_summary"),
                        SimpleLLMAnalyzer(model, generation_config, ci_prompt, {"ci_summary"}, result_key="ci_summary"),
                    ],
                    notifiers=[
                        GitLabMRCommentNotifier(
                            config.gitlab_token,
                            base_url=GITLAB_API_BASE_URL,
                            marker="🚀 Data Product Promotion Analysis",
                        ),
                    ],
                )
                try:
                    pipeline.run(context)
                finally:
                    for gen in context_generators:
                        if hasattr(gen, "cleanup"):
                            try:
                                gen.cleanup()
                            except Exception as cleanup_exc:
                                logger.warning(f"Cleanup failed for {gen}: {cleanup_exc}")
        else:
            logger.info(f"Ignoring note event with content: {note_content}")
    except Exception as e:
        logger.error(f"Exception in handle_note_event: {e}")
        logger.error(traceback.format_exc())
        raise

@app.get("/health")
def health_check():
    """Health check endpoint."""
    # Get commit ID from environment variable or git
    commit_id = os.getenv("COMMIT_SHA")
    if not commit_id:
        try:
            # Try to get commit ID from git
            result = subprocess.run(
                ["git", "rev-parse", "HEAD"], 
                capture_output=True, 
                text=True, 
                cwd=os.path.dirname(__file__)
            )
            if result.returncode == 0:
                commit_id = result.stdout.strip()[:8]  # Short commit hash
            else:
                commit_id = "unknown"
        except Exception:
            commit_id = "unknown"
    
    return {
        "status": "healthy", 
        "service": "gitlab-mr-reviewer",
        "commit_id": commit_id,
        "version": "1.0.0"
    }

@app.post("/webhook/gitlab")
async def webhook(request: Request, x_gitlab_token: str = Header(None)):
    logger.info(f"Received webhook event: {request}")
    # Validate presence and value of GitLab webhook secret header
    expected_secret = os.getenv("WEBHOOK_SECRET")
    if x_gitlab_token is None:
        raise HTTPException(status_code=401, detail="Missing X-Gitlab-Token header")
    if expected_secret and x_gitlab_token != expected_secret:
        raise HTTPException(status_code=401, detail="Invalid X-Gitlab-Token header value")
    try:
        payload = await request.json()
        logger.info(f"Webhook payload: {json.dumps(payload, indent=2)}")
        event_type = request.headers.get("X-Gitlab-Event")
        if not event_type:
            raise HTTPException(status_code=400, detail="Missing X-Gitlab-Event header")
        config = PipelineConfig(
            gitlab_token=os.getenv("GITLAB_API_TOKEN"),
            project_id=str(payload["project"]["id"]),
            project_path=payload["project"]["web_url"],
        )
        if event_type == "Merge Request Hook":
            config.mr_iid = str(payload["object_attributes"]["iid"])
            object_attrs = payload.get("object_attributes", {})
            mr_details = MRDetails.from_object_attrs(object_attrs)
            logger.info(f"Extracted MR details from Merge Request Hook: {mr_details}")
            handle_mr_event(config, get_mr_changes, mr_details)
        elif event_type == "Note Hook":
            config.mr_iid = str(payload["merge_request"]["iid"])
            action = payload["object_attributes"].get("action")
            if action == "create":
                merge_request = payload.get("merge_request", {})
                mr_details = MRDetails.from_object_attrs(merge_request)
                note_content = payload["object_attributes"]["note"]
                handle_note_event(config, get_mr_changes, note_content, mr_details)
            else:
                logger.info(f"Ignoring note event with action: {action}")
        else:
            return JSONResponse(
                status_code=200,
                content={"message": f"Ignoring event type: {event_type}"}
            )
        return JSONResponse(
            status_code=200,
            content={"message": "Webhook processed successfully"}
        )
    except Exception as e:
        logger.error(f"Exception in webhook handler: {e}")
        logger.error(traceback.format_exc())

def main():
    """
    Run the FastAPI service.
    """
    uvicorn.run(
        "framework.webhook_service:app",
        host="0.0.0.0",
        port=int(os.getenv("PORT", "8000")),
        reload=True
    )

if __name__ == "__main__":
    main() 