import os
import json
import logging
from typing import Optional

from google.cloud import aiplatform
from google.oauth2 import service_account
from google.generativeai import GenerativeModel, GenerationConfig

logger = logging.getLogger(__name__)

# Environment variables
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
GEMINI_SERVICE_ACCOUNT = os.getenv("GEMINI_SERVICE_ACCOUNT")
GEMINI_LOCATION = os.getenv("GEMINI_LOCATION", "us-central1")

def initialize_gemini() -> tuple[GenerativeModel, GenerationConfig]:
    """
    Initialize Gemini client with either API key or service account authentication.
    
    Returns:
        tuple: (model, generation_config)
    """
    if GEMINI_API_KEY:
        # Use API key authentication
        from google.generativeai import configure
        configure(api_key=GEMINI_API_KEY)
        model = GenerativeModel("gemini-pro")
        logger.info("Using Gemini API key authentication")
    else:
        # Use service account authentication
        if not GEMINI_SERVICE_ACCOUNT:
            raise ValueError("Either GEMINI_API_KEY or GEMINI_SERVICE_ACCOUNT must be set")
            
        credentials = service_account.Credentials.from_service_account_file(GEMINI_SERVICE_ACCOUNT)
        
        # Extract project ID from service account credentials
        with open(GEMINI_SERVICE_ACCOUNT) as f:
            service_account_info = json.load(f)
            project_id = service_account_info.get('project_id')
            if not project_id:
                raise ValueError("Project ID not found in service account credentials file")
        
        aiplatform.init(project=project_id, location=GEMINI_LOCATION, credentials=credentials)
        model = GenerativeModel("models/gemini-2.5-flash-preview-05-20")
        logger.info(f"Using Vertex AI authentication for project {project_id}")
    
    # Configure generation parameters
    generation_config = GenerationConfig(
        temperature=0.1,
        max_output_tokens=8000
    )
    
    return model, generation_config 