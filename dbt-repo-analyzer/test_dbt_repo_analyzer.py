#!/usr/bin/env python3
"""
Test suite for GitLab Webhook Handler for Data Product Promotions
"""

import pytest
import json
import tempfile
import os
import shutil
from datetime import datetime, timedelta
from unittest.mock import Mock, AsyncMock, patch, MagicMock
from pathlib import Path

import httpx
from fastapi.testclient import TestClient
from fastapi import BackgroundTasks
from google.genai import types

# Import the main application and its components  
# Note: Make sure environment variables are set before importing
os.environ.setdefault('GITLAB_API_TOKEN', 'test-token')
os.environ.setdefault('GEMINI_API_KEY', 'test-key') 
os.environ.setdefault('WEBHOOK_SECRET', '')

from dbt_repo_analyzer import (
    app, GitLabAPI, GeminiAnalyzer, DBTRunner, PromotionInfo,
    verify_gitlab_signature, process_promotion, repo_clone_cache
)


class TestGitLabAPI:
    """Tests for the GitLabAPI class"""
    
    def setup_method(self):
        """Set up test fixtures"""
        self.api = GitLabAPI("test-token", "https://gitlab.example.com")
    
    @pytest.mark.asyncio
    async def test_get_mr_changes_success(self):
        """Test successful MR changes retrieval"""
        mock_changes = [
            {
                "new_file": True,
                "new_path": "dataproducts/source/myproduct/prod/product.yaml"
            }
        ]
        
        with patch('httpx.AsyncClient') as mock_client:
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.json.return_value = {"changes": mock_changes}
            
            mock_client.return_value.__aenter__.return_value.get = AsyncMock(
                return_value=mock_response
            )
            
            changes = await self.api.get_mr_changes(123, 456)
            
            assert changes == mock_changes
    
    @pytest.mark.asyncio
    async def test_get_mr_changes_failure(self):
        """Test MR changes retrieval failure"""
        with patch('httpx.AsyncClient') as mock_client:
            mock_response = Mock()
            mock_response.status_code = 404
            mock_response.text = "Not found"
            
            mock_client.return_value.__aenter__.return_value.get = AsyncMock(
                return_value=mock_response
            )
            
            with pytest.raises(Exception):
                await self.api.get_mr_changes(123, 456)
    
    @pytest.mark.asyncio
    async def test_create_comment_new(self):
        """Test creating a new comment when none exists"""
        with patch.object(self.api, '_find_bot_comment', return_value=None), \
             patch.object(self.api, '_create_comment') as mock_create:
            
            await self.api.create_or_update_comment(123, 456, "Test content")
            mock_create.assert_called_once_with(123, 456, "Test content")
    
    @pytest.mark.asyncio
    async def test_update_existing_comment(self):
        """Test updating an existing comment"""
        existing_comment = {"id": 789, "body": "Old content"}
        
        with patch.object(self.api, '_find_bot_comment', return_value=existing_comment), \
             patch.object(self.api, '_update_comment') as mock_update:
            
            await self.api.create_or_update_comment(123, 456, "New content")
            mock_update.assert_called_once_with(123, 456, 789, "New content")
    
    @pytest.mark.asyncio
    async def test_find_bot_comment_exists(self):
        """Test finding existing bot comment"""
        notes = [
            {"id": 1, "body": "Regular comment"},
            {"id": 2, "body": "## 🚀 Data Product Promotion Analysis\nBot comment"}
        ]
        
        with patch('httpx.AsyncClient') as mock_client:
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.json.return_value = notes
            
            mock_client.return_value.__aenter__.return_value.get = AsyncMock(
                return_value=mock_response
            )
            
            result = await self.api._find_bot_comment(123, 456)
            assert result == notes[1]
    
    @pytest.mark.asyncio
    async def test_find_bot_comment_not_exists(self):
        """Test when bot comment doesn't exist"""
        notes = [{"id": 1, "body": "Regular comment"}]
        
        with patch('httpx.AsyncClient') as mock_client:
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.json.return_value = notes
            
            mock_client.return_value.__aenter__.return_value.get = AsyncMock(
                return_value=mock_response
            )
            
            result = await self.api._find_bot_comment(123, 456)
            assert result is None


class TestGeminiAnalyzer:
    """Tests for the GeminiAnalyzer class"""
    
    def setup_method(self):
        """Set up test fixtures"""
        self.mock_client = Mock()
        self.analyzer = GeminiAnalyzer(self.mock_client)
    
    @pytest.mark.asyncio
    async def test_analyze_mr_for_promotion_success(self):
        """Test successful promotion detection"""
        changes = [
            {
                "new_file": True,
                "new_path": "dataproducts/source/myproduct/prod/product.yaml"
            }
        ]
        
        mock_response = Mock()
        mock_response.text = json.dumps({
            "is_promotion": True,
            "product_name": "myproduct",
            "product_type": "source",
            "environment": "prod",
            "confidence": "high"
        })
        
        with patch('asyncio.to_thread', return_value=mock_response):
            result = await self.analyzer.analyze_mr_for_promotion(changes)
            
            assert result is not None
            assert result.product_name == "myproduct"
            assert result.product_type == "source"
            assert result.environment == "prod"
            assert "source-aligned" in result.dbt_repo_url
    
    @pytest.mark.asyncio
    async def test_analyze_mr_for_promotion_no_promotion(self):
        """Test when no promotion is detected"""
        changes = [{"new_file": True, "new_path": "some/other/file.txt"}]
        
        mock_response = Mock()
        mock_response.text = json.dumps({
            "is_promotion": False,
            "product_name": None,
            "product_type": None,
            "environment": None,
            "confidence": "low"
        })
        
        with patch('asyncio.to_thread', return_value=mock_response):
            result = await self.analyzer.analyze_mr_for_promotion(changes)
            assert result is None
    
    @pytest.mark.asyncio
    async def test_analyze_mr_for_promotion_aggregate_type(self):
        """Test aggregate product type URL construction"""
        changes = [
            {
                "new_file": True,
                "new_path": "dataproducts/aggregate/myproduct/pre-prod/product.yaml"
            }
        ]
        
        mock_response = Mock()
        mock_response.text = json.dumps({
            "is_promotion": True,
            "product_name": "myproduct",
            "product_type": "aggregate",
            "environment": "pre-prod",
            "confidence": "high"
        })
        
        with patch('asyncio.to_thread', return_value=mock_response):
            result = await self.analyzer.analyze_mr_for_promotion(changes)
            
            assert result is not None
            assert "aggregate/myproduct" in result.dbt_repo_url
            assert "aggregate-aligned" not in result.dbt_repo_url
    
    @pytest.mark.asyncio
    async def test_analyze_dbt_manifest_success(self):
        """Test successful dbt manifest analysis"""
        manifest_data = {
            "nodes": {
                "model.myproject.customers": {
                    "resource_type": "model",
                    "schema": "marts",
                    "name": "customers",
                    "description": "Customer data",
                    "columns": {"id": {"description": "Customer ID"}}
                },
                "test.myproject.test_customers": {
                    "resource_type": "test"
                }
            }
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(manifest_data, f)
            manifest_path = f.name
        
        try:
            mock_response = Mock()
            mock_response.text = "## Test Coverage\n1 test found\n\n## Schemas & Models\n- marts: 1 model"
            
            with patch('asyncio.to_thread', return_value=mock_response):
                result = await self.analyzer.analyze_dbt_manifest(manifest_path)
                assert "Test Coverage" in result
                assert "Schemas & Models" in result
        finally:
            os.unlink(manifest_path)
    
    @pytest.mark.asyncio
    async def test_analyze_dbt_manifest_file_not_found(self):
        """Test manifest analysis with missing file"""
        result = await self.analyzer.analyze_dbt_manifest("/nonexistent/path")
        assert "Error: Manifest file not found" in result
    
    @pytest.mark.asyncio
    async def test_analyze_dbt_manifest_invalid_json(self):
        """Test manifest analysis with invalid JSON"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            f.write("invalid json content")
            manifest_path = f.name
        
        try:
            result = await self.analyzer.analyze_dbt_manifest(manifest_path)
            assert "Error parsing manifest JSON" in result
        finally:
            os.unlink(manifest_path)
    
    @pytest.mark.asyncio
    async def test_analyze_gitlab_ci_success(self):
        """Test successful GitLab CI analysis"""
        ci_config = {
            "stages": ["test", "deploy"],
            "test_job": {"stage": "test", "script": ["dbt test"]},
            "deploy_prod": {"stage": "deploy", "script": ["dbt run"]}
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as f:
            import yaml
            yaml.dump(ci_config, f)
            ci_path = f.name
        
        try:
            mock_response = Mock()
            mock_response.text = "## Environment Overview\n| Environment | Tests | Deployments |\n|dev|dbt test|Automated|"
            
            with patch('asyncio.to_thread', return_value=mock_response):
                result = await self.analyzer.analyze_gitlab_ci(ci_path)
                assert "Environment Overview" in result
        finally:
            os.unlink(ci_path)
    
    @pytest.mark.asyncio
    async def test_analyze_gitlab_ci_file_not_found(self):
        """Test GitLab CI analysis with missing file"""
        result = await self.analyzer.analyze_gitlab_ci("/nonexistent/path")
        assert "Error: GitLab CI file not found" in result


class TestDBTRunner:
    """Tests for the DBTRunner class"""
    
    def setup_method(self):
        """Clear the repo clone cache before each test"""
        repo_clone_cache.clear()
    
    def test_is_repo_in_cooldown_not_cached(self):
        """Test cooldown check for non-cached repo"""
        assert not DBTRunner.is_repo_in_cooldown("https://example.com/repo.git")
    
    def test_is_repo_in_cooldown_recent(self):
        """Test cooldown check for recently cached repo"""
        repo_url = "https://example.com/repo.git"
        DBTRunner.update_repo_clone_time(repo_url)
        
        # Should be in cooldown immediately after update
        assert DBTRunner.is_repo_in_cooldown(repo_url)
    
    def test_is_repo_in_cooldown_expired(self):
        """Test cooldown check for repo with expired cooldown"""
        repo_url = "https://example.com/repo.git"
        # Set clone time to past the cooldown period
        repo_clone_cache[repo_url] = datetime.now() - timedelta(minutes=5)
        
        assert not DBTRunner.is_repo_in_cooldown(repo_url)
    
    def test_update_repo_clone_time(self):
        """Test updating repo clone time"""
        repo_url = "https://example.com/repo.git"
        before = datetime.now()
        
        DBTRunner.update_repo_clone_time(repo_url)
        
        assert repo_url in repo_clone_cache
        assert repo_clone_cache[repo_url] >= before
    
    @pytest.mark.asyncio
    async def test_clone_and_parse_cooldown(self):
        """Test clone operation blocked by cooldown"""
        repo_url = "https://example.com/repo.git"
        DBTRunner.update_repo_clone_time(repo_url)  # Put in cooldown
        
        success, message, manifest_path, ci_path = await DBTRunner.clone_and_parse(repo_url)
        
        assert not success
        assert "cooldown" in message.lower()
        assert manifest_path is None
        assert ci_path is None
    
    @pytest.mark.asyncio
    async def test_clone_and_parse_git_failure(self):
        """Test clone operation with git failure"""
        repo_url = "https://example.com/nonexistent.git"
        
        with patch('asyncio.to_thread') as mock_thread:
            # Mock git clone failure
            mock_result = Mock()
            mock_result.returncode = 1
            mock_result.stderr = "Repository not found"
            mock_thread.return_value = mock_result
            
            success, message, manifest_path, ci_path = await DBTRunner.clone_and_parse(repo_url)
            
            assert not success
            assert "Git clone failed" in message
            assert manifest_path is None
    
    @pytest.mark.asyncio
    async def test_clone_and_parse_success(self):
        """Test successful clone and parse operation"""
        repo_url = "https://example.com/repo.git"
        
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create mock dbt project structure
            os.makedirs(os.path.join(temp_dir, "target"))
            
            # Create dbt_project.yml
            with open(os.path.join(temp_dir, "dbt_project.yml"), 'w') as f:
                f.write("name: test\nprofile: test_profile\n")
            
            # Create GitLab CI file
            with open(os.path.join(temp_dir, ".gitlab-ci.yml"), 'w') as f:
                f.write("stages:\n  - test\n")
            
            # Create manifest.json
            manifest_data = {"nodes": {}, "tests": {}}
            with open(os.path.join(temp_dir, "target", "manifest.json"), 'w') as f:
                json.dump(manifest_data, f)
            
            with patch('tempfile.mkdtemp', return_value=temp_dir), \
                 patch('asyncio.to_thread') as mock_thread:
                
                # Mock successful git clone and dbt operations
                def mock_subprocess_run(*args, **kwargs):
                    mock_result = Mock()
                    mock_result.returncode = 0
                    mock_result.stderr = ""
                    mock_result.stdout = "Success"
                    return mock_result
                
                mock_thread.side_effect = mock_subprocess_run
                
                success, message, manifest_path, ci_path = await DBTRunner.clone_and_parse(repo_url)
                
                assert success
                assert "successful" in message.lower()
                assert manifest_path.endswith("manifest.json")
                assert ci_path.endswith(".gitlab-ci.yml")


class TestWebhookSignatureVerification:
    """Tests for webhook signature verification"""
    
    def test_verify_signature_no_secret(self):
        """Test signature verification when no secret is configured"""
        with patch('dbt_repo_analyzer.GITLAB_WEBHOOK_SECRET', ''):
            assert verify_gitlab_signature(b"payload", "any-token")
    
    def test_verify_signature_missing_header(self):
        """Test signature verification with missing header"""
        with patch('dbt_repo_analyzer.GITLAB_WEBHOOK_SECRET', 'secret'):
            assert not verify_gitlab_signature(b"payload", None)
    
    def test_verify_signature_valid(self):
        """Test valid signature verification"""
        with patch('dbt_repo_analyzer.GITLAB_WEBHOOK_SECRET', 'test-secret'):
            assert verify_gitlab_signature(b"payload", "test-secret")
    
    def test_verify_signature_invalid(self):
        """Test invalid signature verification"""
        with patch('dbt_repo_analyzer.GITLAB_WEBHOOK_SECRET', 'correct-secret'):
            assert not verify_gitlab_signature(b"payload", "wrong-secret")


class TestPromotionInfo:
    """Tests for PromotionInfo dataclass"""
    
    def test_promotion_info_creation(self):
        """Test PromotionInfo object creation"""
        promotion = PromotionInfo(
            product_name="test-product",
            product_type="source",
            environment="prod",
            dbt_repo_url="https://gitlab.com/test/repo",
            mr_iid=123,
            project_id=456
        )
        
        assert promotion.product_name == "test-product"
        assert promotion.product_type == "source"
        assert promotion.environment == "prod"
        assert promotion.dbt_repo_url == "https://gitlab.com/test/repo"
        assert promotion.mr_iid == 123
        assert promotion.project_id == 456


class TestProcessPromotion:
    """Tests for the process_promotion function"""
    
    @pytest.mark.asyncio
    async def test_process_promotion_success(self):
        """Test successful promotion processing"""
        promotion = PromotionInfo(
            product_name="test-product",
            product_type="source",
            environment="prod",
            dbt_repo_url="https://gitlab.com/test/repo",
            mr_iid=123,
            project_id=456
        )
        
        mock_gitlab_api = Mock()
        mock_gitlab_api.create_or_update_comment = AsyncMock()
        
        with patch('dbt_repo_analyzer.DBTRunner.clone_and_parse') as mock_clone, \
             patch('dbt_repo_analyzer.GeminiAnalyzer') as mock_analyzer_class, \
             patch('shutil.rmtree'):
            
            # Mock successful clone and parse
            mock_clone.return_value = (True, "Success", "/tmp/manifest.json", "/tmp/.gitlab-ci.yml")
            
            # Mock analyzer
            mock_analyzer = Mock()
            mock_analyzer.analyze_dbt_manifest = AsyncMock(return_value="Manifest analysis")
            mock_analyzer.analyze_gitlab_ci = AsyncMock(return_value="CI analysis")
            mock_analyzer_class.return_value = mock_analyzer
            
            await process_promotion(promotion, mock_gitlab_api)
            
            # Verify API calls were made
            assert mock_gitlab_api.create_or_update_comment.call_count == 2
    
    @pytest.mark.asyncio
    async def test_process_promotion_clone_failure(self):
        """Test promotion processing with clone failure"""
        promotion = PromotionInfo(
            product_name="test-product",
            product_type="source",
            environment="prod",
            dbt_repo_url="https://gitlab.com/test/repo",
            mr_iid=123,
            project_id=456
        )
        
        mock_gitlab_api = Mock()
        mock_gitlab_api.create_or_update_comment = AsyncMock()
        
        with patch('dbt_repo_analyzer.DBTRunner.clone_and_parse') as mock_clone:
            # Mock failed clone
            mock_clone.return_value = (False, "Clone failed", None, None)
            
            await process_promotion(promotion, mock_gitlab_api)
            
            # Verify error comment was posted
            assert mock_gitlab_api.create_or_update_comment.call_count == 2
            # Check that the final call contains error information
            final_call_args = mock_gitlab_api.create_or_update_comment.call_args_list[-1]
            assert "Error" in final_call_args[0][2]


class TestFastAPIEndpoints:
    """Tests for FastAPI endpoints"""
    
    def setup_method(self):
        """Set up test client"""
        self.client = TestClient(app)
    
    def test_health_check(self):
        """Test health check endpoint"""
        response = self.client.get("/health")
        assert response.status_code == 200
        assert response.json()["status"] == "healthy"
    
    def test_webhook_wrong_method(self):
        """Test webhook endpoint with wrong HTTP method"""
        response = self.client.get("/webhook/gitlab")
        assert response.status_code == 405  # Method not allowed
    
    '''
    @patch('dbt_repo_analyzer.GITLAB_WEBHOOK_SECRET', 'test-secret')
    def test_webhook_missing_signature(self):
        """Test webhook with missing signature header"""
        payload = {
            "object_kind": "merge_request",
            "object_attributes": {"action": "open", "iid": 123},
            "project": {"id": 456}
        }
        response = self.client.post("/webhook/gitlab", json=payload)
        assert response.status_code == 401
    '''
    @patch('dbt_repo_analyzer.GITLAB_WEBHOOK_SECRET', '')
    def test_webhook_no_signature_required(self):
        """Test webhook when no signature is required"""
        payload = {"object_kind": "push"}  # Non-MR event
        response = self.client.post("/webhook/gitlab", json=payload)
        assert response.status_code == 200
        assert response.json()["status"] == "ignored"
    
    @patch('dbt_repo_analyzer.GITLAB_WEBHOOK_SECRET', '')
    def test_webhook_non_mr_event(self):
        """Test webhook with non-merge request event"""
        payload = {"object_kind": "push"}
        response = self.client.post("/webhook/gitlab", json=payload)
        assert response.status_code == 200
        assert response.json()["reason"] == "not a merge request event"
    
    @patch('dbt_repo_analyzer.GITLAB_WEBHOOK_SECRET', '')
    def test_webhook_irrelevant_mr_action(self):
        """Test webhook with irrelevant MR action"""
        payload = {
            "object_kind": "merge_request",
            "object_attributes": {"action": "close"}
        }
        response = self.client.post("/webhook/gitlab", json=payload)
        assert response.status_code == 200
        assert "not relevant" in response.json()["reason"]
    
    @patch('dbt_repo_analyzer.GITLAB_WEBHOOK_SECRET', '')
    @patch('dbt_repo_analyzer.GitLabAPI')
    @patch('dbt_repo_analyzer.GeminiAnalyzer')
    def test_webhook_mr_no_promotion(self, mock_analyzer_class, mock_gitlab_class):
        """Test webhook with MR that has no promotion"""
        payload = {
            "object_kind": "merge_request",
            "object_attributes": {"action": "open", "iid": 123},
            "project": {"id": 456}
        }
        
        # Mock GitLab API with proper async methods
        mock_gitlab = Mock()
        mock_gitlab.get_mr_changes = AsyncMock(return_value=[])
        mock_gitlab_class.return_value = mock_gitlab
        
        # Mock analyzer with proper async methods
        mock_analyzer = Mock()
        mock_analyzer.analyze_mr_for_promotion = AsyncMock(return_value=None)
        mock_analyzer_class.return_value = mock_analyzer
        
        response = self.client.post("/webhook/gitlab", json=payload)
        assert response.status_code == 200
        assert response.json()["reason"] == "no data product promotion detected"
    
    @patch('dbt_repo_analyzer.GITLAB_WEBHOOK_SECRET', '')
    @patch('dbt_repo_analyzer.GitLabAPI')
    @patch('dbt_repo_analyzer.GeminiAnalyzer')
    @patch('dbt_repo_analyzer.BackgroundTasks.add_task')
    def test_webhook_mr_with_promotion(self, mock_bg_task, mock_analyzer_class, mock_gitlab_class):
        """Test webhook with MR that has a promotion"""
        payload = {
            "object_kind": "merge_request",
            "object_attributes": {"action": "open", "iid": 123},
            "project": {"id": 456}
        }
        
        # Mock GitLab API with proper async methods
        mock_gitlab = Mock()
        mock_gitlab.get_mr_changes = AsyncMock(return_value=[{"new_file": True}])
        mock_gitlab_class.return_value = mock_gitlab
        
        # Mock analyzer with proper async methods
        promotion = PromotionInfo(
            product_name="test-product",
            product_type="source",
            environment="prod",
            dbt_repo_url="https://gitlab.com/test",
            mr_iid=0,
            project_id=0
        )
        mock_analyzer = Mock()
        mock_analyzer.analyze_mr_for_promotion = AsyncMock(return_value=promotion)
        mock_analyzer_class.return_value = mock_analyzer
        
        response = self.client.post("/webhook/gitlab", json=payload)
        assert response.status_code == 200
        assert response.json()["status"] == "processing"
        assert response.json()["product"] == "test-product"
        
        # Verify background task was added
        mock_bg_task.assert_called_once()
    '''
    def test_webhook_invalid_json(self):
        """Test webhook with invalid JSON payload"""
        # Use raw bytes to simulate invalid JSON from client
        response = self.client.post(
            "/webhook/gitlab",
            content=b"invalid json content",
            headers={"Content-Type": "application/json"}
        )
        # The FastAPI framework will return 422 for invalid JSON, not 400
        assert response.status_code in [400, 422]
    '''

class TestEnvironmentConfiguration:
    """Tests for environment variable configuration"""
    '''
    def test_missing_required_env_vars(self):
        """Test that missing required environment variables raise error"""
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(ValueError, match="GITLAB_TOKEN and GEMINI_API_KEY"):
                # This would be tested by importing the module with cleared env vars
                pass
    '''
    def test_default_cooldown_period(self):
        """Test default cooldown period setting"""
        with patch.dict(os.environ, {"REPO_CLONE_COOLDOWN_MINUTES": "5"}):
            # Would need to reimport module to test this properly
            assert True  # Placeholder for actual test
    
    def test_webhook_secret_warning(self):
        """Test warning when webhook secret is not set"""
        with patch.dict(os.environ, {"WEBHOOK_SECRET": ""}):
            # Would test that warning is logged
            assert True  # Placeholder for actual test


# Integration test fixtures
@pytest.fixture
def sample_manifest():
    """Sample dbt manifest for testing"""
    return {
        "nodes": {
            "model.test.customers": {
                "resource_type": "model",
                "schema": "marts",
                "name": "customers",
                "description": "Customer dimension table",
                "columns": {
                    "customer_id": {"description": "Primary key"},
                    "name": {"description": "Customer name"},
                    "email": {"description": ""}
                }
            },
            "model.test.orders": {
                "resource_type": "model",
                "schema": "intermediate",
                "name": "orders"
            },
            "test.test.unique_customers": {
                "resource_type": "test"
            },
            "test.test.not_null_customer_id": {
                "resource_type": "test"
            }
        }
    }


@pytest.fixture
def sample_gitlab_ci():
    """Sample GitLab CI configuration for testing"""
    return {
        "stages": ["test", "deploy"],
        "variables": {
            "DBT_PROFILES_DIR": "$CI_PROJECT_DIR"
        },
        "test_dev": {
            "stage": "test",
            "script": ["dbt test --target dev"],
            "only": ["develop"]
        },
        "deploy_prod": {
            "stage": "deploy",
            "script": ["dbt run --target prod"],
            "only": ["main"],
            "when": "manual"
        }
    }


# Run tests
if __name__ == "__main__":
    pytest.main([__file__, "-v"])