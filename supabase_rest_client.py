#!/usr/bin/env python3
"""
supabase_rest_client.py - Supabase REST API client for database operations (no psycopg2)
"""

import os
import logging
import requests
from typing import Optional, Dict, Any, List
from datetime import datetime

logger = logging.getLogger(__name__)


class SupabaseRestClient:
    """Supabase REST API client for database operations"""
    
    def __init__(self):
        self.supabase_url = os.environ.get("SUPABASE_URL")
        self.supabase_key = os.environ.get("SUPABASE_SERVICE_KEY")
        self.enabled = os.environ.get("ENABLE_DATABASE_TRACKING", "false").lower() == "true"
        
        if not self.enabled:
            logger.info("Database tracking disabled via ENABLE_DATABASE_TRACKING=false")
            self.base_url = None
            self.headers = None
            return
        
        if not self.supabase_url or not self.supabase_key:
            logger.warning("Supabase credentials not found, database tracking disabled")
            self.enabled = False
            self.base_url = None
            self.headers = None
            return
        
        self.base_url = f"{self.supabase_url}/rest/v1"
        self.headers = {
            "apikey": self.supabase_key,
            "Authorization": f"Bearer {self.supabase_key}",
            "Content-Type": "application/json",
            "Prefer": "return=representation"
        }
        
        logger.info("Supabase REST client initialized successfully")
    
    def is_enabled(self) -> bool:
        """Check if database tracking is enabled and working"""
        return self.enabled and self.base_url is not None
    
    def _make_request(self, method: str, endpoint: str, data: Optional[Dict] = None) -> Optional[Dict]:
        """Make HTTP request to Supabase REST API"""
        if not self.is_enabled():
            logger.debug("Database tracking not enabled, skipping request")
            return None
        
        try:
            url = f"{self.base_url}/{endpoint}"
            
            if method.upper() == "GET":
                response = requests.get(url, headers=self.headers)
            elif method.upper() == "POST":
                response = requests.post(url, headers=self.headers, json=data)
            elif method.upper() == "PATCH":
                response = requests.patch(url, headers=self.headers, json=data)
            elif method.upper() == "DELETE":
                response = requests.delete(url, headers=self.headers)
            else:
                logger.error(f"Unsupported HTTP method: {method}")
                return None
            
            response.raise_for_status()
            
            if response.content:
                return response.json()
            return {}
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Database request failed: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error in database request: {e}")
            return None
    
    def create_job(self, file_hash: str, original_filename: str, dataset_type: Optional[str] = None) -> Optional[Dict]:
        """Create a new job record"""
        data = {
            "file_hash": file_hash,
            "original_filename": original_filename,
            "dataset_type": dataset_type,
            "status": "queued"
        }
        
        result = self._make_request("POST", "jobs", data)
        if result and isinstance(result, list) and len(result) > 0:
            logger.info(f"Created job: {result[0].get('job_id')}")
            return result[0]
        return None
    
    def get_job(self, job_id: str) -> Optional[Dict]:
        """Get job by ID"""
        result = self._make_request("GET", f"jobs?job_id=eq.{job_id}")
        if result and isinstance(result, list) and len(result) > 0:
            return result[0]
        return None
    
    def update_job_status(self, job_id: str, status: str, error_msg: Optional[str] = None) -> bool:
        """Update job status"""
        data = {
            "status": status,
            "error_msg": error_msg
        }
        
        if status == "running":
            data["started_at"] = datetime.utcnow().isoformat()
        elif status in ["done", "failed", "error"]:
            data["finished_at"] = datetime.utcnow().isoformat()
        
        result = self._make_request("PATCH", f"jobs?job_id=eq.{job_id}", data)
        if result is not None:
            logger.info(f"Updated job {job_id} status to {status}")
            return True
        return False
    
    def get_queued_jobs(self) -> List[Dict]:
        """Get all queued jobs"""
        result = self._make_request("GET", "jobs?status=eq.queued&order=uploaded_at.asc")
        if result and isinstance(result, list):
            return result
        return []
    
    def get_jobs_by_status(self, status: str, limit: int = 100) -> List[Dict]:
        """Get jobs by status"""
        result = self._make_request("GET", f"jobs?status=eq.{status}&order=uploaded_at.desc&limit={limit}")
        if result and isinstance(result, list):
            return result
        return []
    
    def create_output(self, job_id: str, file_type: str, storage_path: str, file_size: Optional[int] = None) -> Optional[Dict]:
        """Create output record"""
        data = {
            "job_id": job_id,
            "file_type": file_type,
            "storage_path": storage_path,
            "file_size": file_size
        }
        
        result = self._make_request("POST", "outputs", data)
        if result and isinstance(result, list) and len(result) > 0:
            logger.info(f"Created output: {result[0].get('output_id')}")
            return result[0]
        return None
    
    def get_outputs_by_job(self, job_id: str) -> List[Dict]:
        """Get all outputs for a job"""
        result = self._make_request("GET", f"outputs?job_id=eq.{job_id}&order=created_at.asc")
        if result and isinstance(result, list):
            return result
        return []
    
    def get_output(self, output_id: str) -> Optional[Dict]:
        """Get output by ID"""
        result = self._make_request("GET", f"outputs?output_id=eq.{output_id}")
        if result and isinstance(result, list) and len(result) > 0:
            return result[0]
        return None
    
    def create_or_update_upload_file(self, file_hash: str, original_name: str, normalized_path: Optional[str] = None) -> Optional[Dict]:
        """Create or update upload file record"""
        # First try to get existing record
        existing = self._make_request("GET", f"upload_files?file_hash=eq.{file_hash}")
        
        if existing and isinstance(existing, list) and len(existing) > 0:
            # Update existing record
            data = {
                "last_used": datetime.utcnow().isoformat(),
                "usage_count": existing[0].get("usage_count", 0) + 1
            }
            if normalized_path:
                data["normalized_path"] = normalized_path
            
            result = self._make_request("PATCH", f"upload_files?file_hash=eq.{file_hash}", data)
            if result and isinstance(result, list) and len(result) > 0:
                logger.info(f"Updated upload file: {file_hash}")
                return result[0]
        else:
            # Create new record
            data = {
                "file_hash": file_hash,
                "original_name": original_name,
                "normalized_path": normalized_path,
                "usage_count": 1
            }
            
            result = self._make_request("POST", "upload_files", data)
            if result and isinstance(result, list) and len(result) > 0:
                logger.info(f"Created upload file: {file_hash}")
                return result[0]
        
        return None
    
    def get_upload_file(self, file_hash: str) -> Optional[Dict]:
        """Get upload file by hash"""
        result = self._make_request("GET", f"upload_files?file_hash=eq.{file_hash}")
        if result and isinstance(result, list) and len(result) > 0:
            return result[0]
        return None
    
    def get_recent_jobs_for_file(self, file_hash: str, limit: int = 5) -> List[Dict]:
        """Get recent jobs for a file hash"""
        result = self._make_request("GET", f"jobs?file_hash=eq.{file_hash}&order=uploaded_at.desc&limit={limit}")
        if result and isinstance(result, list):
            return result
        return []
    
    def health_check(self) -> Dict[str, Any]:
        """Check database connectivity"""
        health = {
            "enabled": self.enabled,
            "connected": False,
            "timestamp": None
        }
        
        if not self.enabled:
            return health
        
        try:
            # Test database connection by querying jobs table
            result = self._make_request("GET", "jobs?limit=1")
            health["connected"] = result is not None
            logger.info("Database health check passed")
        except Exception as e:
            logger.error(f"Database health check failed: {e}")
            health["connected"] = False
        
        health["timestamp"] = datetime.utcnow().isoformat()
        return health


# Global client instance
supabase_rest = SupabaseRestClient()
