from typing import Dict, Optional
import re
from dataclasses import dataclass

@dataclass
class PromptConfig:
    """Configuration for a repository's prompts."""
    mr_summary: str
    manifest: str
    ci: str
    promotion: Optional[str] = None

class PromptManager:
    """Manages repository-specific prompts and their retrieval."""
    
    def __init__(self):
        # Base prompts that are common across all repositories
        self.BASE_MR_SUMMARY_PROMPT = (
            "You are an expert technical writer. Summarize the following GitLab Merge Request (MR) in clear, structured Markdown, using exactly these four sections and headers (no numbering, no extra headings):\n\n"
            "## Overview\n"
            "Summarize the purpose and scope of the MR in 2-3 sentences. Mention the data product, environment, and any critical context.\n\n"
            "## Key Changes\n"
            "List the main configuration and code changes as bullet points. IGNORE .gitkeep changes. Be specific about what was added, changed, or removed. Use bold for important terms and backticks for file names or code.\n\n"
            "## Files Modified\n"
            "For each file changed, provide a bullet point with the file path in backticks, and a short description of its purpose or what was changed. Ignore .gitkeep changes.\n\n"
            "## Promotion Checklist\n"
            "If a promotion checklist is included, summarize its purpose and note if key items are marked complete. Use bullet points to summarize completed items in moderate details and incomplete items.\n\n "
            "If not present, say 'No promotion checklist included.'\n\n"
            "---\n"
            "### MR Details\n"
            "- Title: {title}\n"
            "- Description: {description}\n"
            "- Source Branch: {source_branch}\n"
            "- Target Branch: {target_branch}\n"
            "- Commit: {commit_sha}\n"
            "#### Diff Content:\n"
            "```diff\n{diff_content}\n```\n"
            "Be concise, use Markdown formatting, and do not include any text outside this structure."
        )

        # Initialize repository-specific prompts
        self._repo_prompts: Dict[str, PromptConfig] = {
            # Dataverse config repository
            "gitlab.cee.redhat.com/dataverse/dataverse-config/dataproduct-config": PromptConfig(
                mr_summary=self.BASE_MR_SUMMARY_PROMPT,
                manifest=(
                    "Respond ONLY with a section named DBT Project Analysis containing two Markdown tables:\n"
                    "1. Model counts by schema (excluding dbtlogs)\n"
                    "2. Documentation completeness by schema (documented/undocumented)\n"
                    "Do not include any other text or explanation.\n\n"
                    "Summary data: {manifest_summary}\n"
                ),
                ci=(
                    "Respond ONLY with a section named CI/CD Configuration Analysis containing"
                    "aMarkdown table ( 2 rows and 3 columns ) summarizing the environments(rows), tests(column), and deployments(column) with a comment on whether they are manual/automated. "
                    "and a section for metadata publishing (list the metadata jobs). "
                    "Please add a note explaining whether metdata for Atlan is being published (and how).\n\n"
                    "CI/CD config: {ci_summary}\n"
                ),
                promotion=(
                    "Rules for promotion detection:\n"
                    "    1. Look for additions of 'product.yaml' files to 'prod' or 'pre-prod' directories\n"
                    "    2. The pattern should be: dataproducts/[source|aggregate]/[product_name]/[prod|pre-prod]/product.yaml\n"
                    "    3. Extract the product name from the parent directory of the prod/pre-prod folder\n"
                    "    4. Determine if it's source-aligned or aggregate based on the path\n\n"
                    "File Changes:\n{mr_changes}\n\n"
                    "If this is a data product promotion, respond with ONLY the following JSON object (no code block, no explanation, no extra formatting):\n"
                    "{{\n"
                    "    \"is_promotion\": true,\n"
                    "    \"product_name\": \"name of the data product\",\n"
                    "    \"product_type\": \"source-aligned or aggregate\",\n"
                    "    \"environment\": \"prod or pre-prod\"\n"
                    "}}\n"
                    "If this is not a data product promotion, respond with ONLY this JSON object (no code block, no explanation, no extra formatting):\n"
                    "{{\n    \"is_promotion\": false\n}}\n"
                    "IMPORTANT: Your response MUST be ONLY the JSON object, with no code block, no explanation, and no extra formatting."
                )
            ),
            # DBT repositories pattern
            r"^gitlab\.cee\.redhat\.com/dataverse/data-products/.*-dbt$": PromptConfig(
                mr_summary=(
                    "You are a senior analytics engineer and code reviewer with deep experience in dbt and Snowflake. Review the following GitLab Merge Request (MR) for a dbt project. Focus only on the code and configuration changes (ignore the MR title and description).\n\n"
                    "**IMPORTANT:**  \n"
                    "- Your entire response must be **no more than 10000 words** (or approximately 2000 tokens).  \n"
                    "- Be concise and prioritize the most critical issues.  \n"
                    "- If you run out of space, summarize the remaining points and note that the review was truncated.\n\n"
                    "---\n\n"
                    "## 🟦 SQL Correctness & Performance\n\n"
                    "- **Errors & Anti-Patterns:**  \n"
                    "  Identify any SQL mistakes, anti-patterns, or inefficiencies.\n"
                    "- **Performance:**  \n"
                    "  Highlight queries or transformations that may cause performance issues in Snowflake (e.g., missing filters, unnecessary CTEs, lack of partitioning, suboptimal joins).\n"
                    "- **Deprecated Features:**  \n"
                    "  Point out any use of deprecated or discouraged dbt or SQL features from the dbt package versions used in the project.\n\n"
                    "---\n\n"
                    "## 🔄 DRY SQL & Code Reusability\n\n"
                    "- **Repeated SQL Patterns:**  \n"
                    "  Identify any SQL logic that appears multiple times across models. Suggest ways to make the code more maintainable through macros, CTEs, or intermediate models.\n"
                    "- **Macro Opportunities:**  \n"
                    "  Point out where custom macros could be created to encapsulate common transformations or business logic.\n"
                    "- **Model Hierarchy:**  \n"
                    "  Suggest if complex logic could be moved to upstream models to promote reusability and single source of truth.\n\n"
                    "---\n\n"
                    "## 🧪 Testing Coverage\n\n"
                    "- **Missing or Insufficient Tests:**  \n"
                    "  Call out if new or modified models, columns, or logic lack adequate tests.  \n"
                    "  _If tests are missing, recommend specific tests to add (e.g., `not_null`, `unique`, `accepted_values`, `relationship`)._\n"
                    "- **Existing Tests:**  \n"
                    "  Review if current tests still cover the intended logic after the changes.\n\n"
                    "---\n\n"
                    "## 📝 Documentation\n\n"
                    "- **New/Changed Objects:**  \n"
                    "  Check if new models, columns, or significant changes are documented according to dbt best practices.\n"
                    "- **Improvements:**  \n"
                    "  If documentation is missing or unclear, specify what needs to be added or improved.\n\n"
                    "---\n\n"
                    "## ⚙️ dbt & Snowflake Best Practices\n\n"
                    "- **dbt Conventions:**  \n"
                    "  Ensure correct use of `ref`, `source`, Jinja templating, and model materializations.\n"
                    "- **Snowflake-Specific:**  \n"
                    "  Call out any code that may not be portable or optimal for Snowflake (e.g., unsupported functions, improper use of semi-structured data, cost-inefficient patterns).\n"
                    "- **Macros:**  \n"
                    "  If macros are used, check for maintainability and Snowflake optimization.\n\n"
                    "---\n\n"
                    "## 🎯 Repository-Specific Review Criteria\n\n"
                    "{custom_review_criteria}\n\n"
                    "---\n\n"
                    "**Present your review in clear Markdown, using bullet points, bold text, and code formatting where appropriate. Do not include the MR title or description in your analysis—base your feedback solely on the code and configuration changes shown below.**\n\n"
                    "---\n\n"
                    "#### MR Diff:\n"
                    "```diff\n{diff_content}\n```\n"
                ),
                manifest=(
                    "Respond ONLY with a section named DBT Project Analysis containing:\n"
                    "1. A Markdown table of model counts by schema (excluding dbtlogs)\n"
                    "2. A Markdown table of documentation completeness by schema\n"
                    "Summary data: {manifest_summary}\n"
                ),
                ci=(
                    "Respond ONLY with a section named CI/CD Configuration Analysis containing:\n"
                    "1. A Markdown table (3 rows 2 columns) summarizing environments(rows), tests(column), and deployments(column) with a comment on whether they are manual/automated. \n"
                    "2. A section on metadata publishing and documentation generation\n\n"
                    "CI/CD config: {ci_summary}\n"
                )
            )
        }

    def get_prompt(self, repo_url: str, prompt_type: str) -> str:
        """
        Get the appropriate prompt for a given repository URL and prompt type.
        
        Args:
            repo_url: The repository URL
            prompt_type: One of 'mr_summary', 'manifest', 'ci', or 'promotion'
            
        Returns:
            The appropriate prompt template
            
        Raises:
            KeyError: If the prompt type is not found for the repository
        """
        # Extract the host and path from the URL
        if "://" in repo_url:
            _, path = repo_url.split("://", 1)
        else:
            path = repo_url
            
        # Remove any trailing slashes or merge request paths
        path = path.split("/-/")[0].rstrip("/")
        
        # First try exact match
        if path in self._repo_prompts:
            prompt_config = self._repo_prompts[path]
            if not hasattr(prompt_config, prompt_type):
                raise KeyError(f"No {prompt_type} prompt found for repository: {path}")
            return getattr(prompt_config, prompt_type)
        
        # Then try regex patterns
        for pattern, prompt_config in self._repo_prompts.items():
            if isinstance(pattern, str) and pattern.startswith("^"):
                if re.search(pattern, path):
                    if not hasattr(prompt_config, prompt_type):
                        raise KeyError(f"No {prompt_type} prompt found for repository: {path}")
                    return getattr(prompt_config, prompt_type)
        
        # Fallback to base prompt for mr_summary
        if prompt_type == "mr_summary":
            return self.BASE_MR_SUMMARY_PROMPT
            
        raise KeyError(f"No prompts found for repository: {path}")

    def add_repo_prompts(self, repo_path: str, prompts: PromptConfig) -> None:
        """
        Add or update prompts for a repository.
        
        Args:
            repo_path: The repository path
            prompts: A PromptConfig instance containing the prompts
        """
        self._repo_prompts[repo_path] = prompts

# Create a singleton instance
prompt_manager = PromptManager()

# For backward compatibility
def get_prompt_for_repo(repo_url: str, prompt_type: str) -> str:
    """Legacy function that uses the PromptManager singleton."""
    return prompt_manager.get_prompt(repo_url, prompt_type) 