#!/usr/bin/env python3
"""
supabase_storage_client.py - Simplified Supabase Storage client without psycopg2
"""

import os
import logging
from typing import Optional, Dict, Any, List
from supabase import create_client, Client

logger = logging.getLogger(__name__)


class SupabaseStorageClient:
    """Simplified Supabase client for storage operations only"""
    
    def __init__(self):
        self.supabase_url = os.environ.get("SUPABASE_URL")
        self.supabase_key = os.environ.get("SUPABASE_SERVICE_KEY")
        self.enabled = os.environ.get("ENABLE_SUPABASE_STORAGE", "false").lower() == "true"
        
        if not self.enabled:
            logger.info("Supabase Storage disabled via ENABLE_SUPABASE_STORAGE=false")
            self.supabase = None
            return
        
        if not self.supabase_url or not self.supabase_key:
            logger.warning("Supabase credentials not found, storage disabled")
            self.enabled = False
            self.supabase = None
            return
        
        try:
            self.supabase: Client = create_client(self.supabase_url, self.supabase_key)
            logger.info("Supabase Storage client initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize Supabase client: {e}")
            self.enabled = False
            self.supabase = None
    
    def is_enabled(self) -> bool:
        """Check if Supabase Storage is enabled and working"""
        return self.enabled and self.supabase is not None
    
    def upload_file(self, bucket: str, file_path: str, file_data: bytes, 
                   content_type: str = "application/octet-stream") -> bool:
        """Upload file to Supabase Storage"""
        if not self.is_enabled():
            logger.debug("Supabase Storage not enabled, skipping upload")
            return False
        
        try:
            # Ensure bucket exists
            try:
                self.supabase.storage.create_bucket(bucket, public=False)
                logger.info(f"Created bucket: {bucket}")
            except Exception:
                pass  # Bucket might already exist
            
            # Upload file
            result = self.supabase.storage.from_(bucket).upload(
                file_path, 
                file_data,
                file_options={"content-type": content_type}
            )
            
            if result.get("error"):
                logger.error(f"Upload failed: {result['error']}")
                return False
            
            logger.info(f"File uploaded to {bucket}/{file_path}")
            return True
            
        except Exception as e:
            logger.error(f"File upload failed: {e}")
            return False
    
    def download_file(self, bucket: str, file_path: str) -> Optional[bytes]:
        """Download file from Supabase Storage"""
        if not self.is_enabled():
            logger.debug("Supabase Storage not enabled, cannot download")
            return None
        
        try:
            result = self.supabase.storage.from_(bucket).download(file_path)
            logger.info(f"File downloaded from {bucket}/{file_path}")
            return result
        except Exception as e:
            logger.error(f"File download failed: {e}")
            return None
    
    def get_signed_url(self, bucket: str, file_path: str, expires_in: int = 3600) -> Optional[str]:
        """Generate signed URL for file download"""
        if not self.is_enabled():
            logger.debug("Supabase Storage not enabled, cannot generate signed URL")
            return None
        
        try:
            result = self.supabase.storage.from_(bucket).create_signed_url(
                file_path, 
                expires_in
            )
            
            if result.get("error"):
                logger.error(f"Signed URL generation failed: {result['error']}")
                return None
            
            logger.info(f"Signed URL generated for {bucket}/{file_path}")
            return result["signedURL"]
        except Exception as e:
            logger.error(f"Signed URL generation failed: {e}")
            return None
    
    def delete_file(self, bucket: str, file_path: str) -> bool:
        """Delete file from Supabase Storage"""
        if not self.is_enabled():
            logger.debug("Supabase Storage not enabled, cannot delete")
            return False
        
        try:
            result = self.supabase.storage.from_(bucket).remove([file_path])
            
            if result.get("error"):
                logger.error(f"File deletion failed: {result['error']}")
                return False
            
            logger.info(f"File deleted: {bucket}/{file_path}")
            return True
        except Exception as e:
            logger.error(f"File deletion failed: {e}")
            return False
    
    def list_files(self, bucket: str, folder: str = "") -> List[Dict[str, Any]]:
        """List files in Supabase Storage bucket"""
        if not self.is_enabled():
            logger.debug("Supabase Storage not enabled, cannot list files")
            return []
        
        try:
            result = self.supabase.storage.from_(bucket).list(folder)
            logger.info(f"Listed {len(result)} files in {bucket}/{folder}")
            return result
        except Exception as e:
            logger.error(f"File listing failed: {e}")
            return []
    
    def health_check(self) -> Dict[str, Any]:
        """Check storage connectivity"""
        health = {
            "enabled": self.enabled,
            "connected": False,
            "timestamp": None
        }
        
        if not self.enabled:
            return health
        
        try:
            # Test storage connection by listing uploads bucket
            self.list_files("uploads")
            health["connected"] = True
            logger.info("Supabase Storage health check passed")
        except Exception as e:
            logger.error(f"Supabase Storage health check failed: {e}")
            health["connected"] = False
        
        from datetime import datetime
        health["timestamp"] = datetime.utcnow().isoformat()
        
        return health


# Global client instance
supabase_storage = SupabaseStorageClient()
