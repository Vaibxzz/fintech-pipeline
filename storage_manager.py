#!/usr/bin/env python3
"""
storage_manager.py - File upload/download to Supabase Storage
"""

import os
import logging
from typing import Optional, Dict, Any, List
from pathlib import Path
from supabase_client import supabase_client

logger = logging.getLogger(__name__)


class StorageManager:
    """Manages file operations with Supabase Storage"""
    
    def __init__(self):
        self.client = supabase_client
    
    def upload_file(self, bucket: str, file_path: str, file_data: bytes, 
                   content_type: str = "application/octet-stream") -> str:
        """Upload file to Supabase Storage"""
        try:
            storage_path = self.client.upload_file(
                bucket, file_path, file_data, content_type
            )
            logger.info(f"File uploaded: {bucket}/{file_path}")
            return storage_path
        except Exception as e:
            logger.error(f"Upload failed for {bucket}/{file_path}: {e}")
            raise
    
    def download_file(self, bucket: str, file_path: str) -> bytes:
        """Download file from Supabase Storage"""
        try:
            file_data = self.client.download_file(bucket, file_path)
            logger.info(f"File downloaded: {bucket}/{file_path}")
            return file_data
        except Exception as e:
            logger.error(f"Download failed for {bucket}/{file_path}: {e}")
            raise
    
    def get_signed_url(self, bucket: str, file_path: str, expires_in: int = 3600) -> str:
        """Generate signed URL for file download"""
        try:
            signed_url = self.client.get_signed_url(bucket, file_path, expires_in)
            logger.info(f"Signed URL generated for {bucket}/{file_path}")
            return signed_url
        except Exception as e:
            logger.error(f"Signed URL generation failed for {bucket}/{file_path}: {e}")
            raise
    
    def delete_file(self, bucket: str, file_path: str) -> bool:
        """Delete file from Supabase Storage"""
        try:
            success = self.client.delete_file(bucket, file_path)
            if success:
                logger.info(f"File deleted: {bucket}/{file_path}")
            return success
        except Exception as e:
            logger.error(f"Delete failed for {bucket}/{file_path}: {e}")
            return False
    
    def list_files(self, bucket: str, folder: str = "") -> List[Dict[str, Any]]:
        """List files in Supabase Storage bucket"""
        try:
            files = self.client.list_files(bucket, folder)
            logger.info(f"Listed {len(files)} files in {bucket}/{folder}")
            return files
        except Exception as e:
            logger.error(f"List files failed for {bucket}/{folder}: {e}")
            return []
    
    def upload_local_file(self, local_path: str, bucket: str, 
                         storage_path: Optional[str] = None) -> str:
        """Upload local file to Supabase Storage"""
        try:
            if not os.path.exists(local_path):
                raise FileNotFoundError(f"Local file not found: {local_path}")
            
            # Use provided storage path or derive from local path
            if not storage_path:
                storage_path = os.path.basename(local_path)
            
            # Read file data
            with open(local_path, 'rb') as f:
                file_data = f.read()
            
            # Determine content type
            content_type = self._get_content_type(local_path)
            
            # Upload file
            return self.upload_file(bucket, storage_path, file_data, content_type)
            
        except Exception as e:
            logger.error(f"Local file upload failed for {local_path}: {e}")
            raise
    
    def download_to_local(self, bucket: str, file_path: str, 
                         local_path: str) -> str:
        """Download file from Supabase Storage to local path"""
        try:
            # Download file data
            file_data = self.download_file(bucket, file_path)
            
            # Ensure local directory exists
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            
            # Write to local file
            with open(local_path, 'wb') as f:
                f.write(file_data)
            
            logger.info(f"File downloaded to: {local_path}")
            return local_path
            
        except Exception as e:
            logger.error(f"Download to local failed for {bucket}/{file_path}: {e}")
            raise
    
    def get_file_info(self, bucket: str, file_path: str) -> Optional[Dict[str, Any]]:
        """Get file information from storage"""
        try:
            # List files in the parent directory to find the specific file
            parent_dir = os.path.dirname(file_path)
            files = self.list_files(bucket, parent_dir)
            
            filename = os.path.basename(file_path)
            for file_info in files:
                if file_info.get('name') == filename:
                    return file_info
            
            return None
            
        except Exception as e:
            logger.error(f"Get file info failed for {bucket}/{file_path}: {e}")
            return None
    
    def file_exists(self, bucket: str, file_path: str) -> bool:
        """Check if file exists in storage"""
        try:
            file_info = self.get_file_info(bucket, file_path)
            return file_info is not None
        except Exception as e:
            logger.error(f"File exists check failed for {bucket}/{file_path}: {e}")
            return False
    
    def get_storage_usage(self, bucket: str) -> Dict[str, Any]:
        """Get storage usage statistics for a bucket"""
        try:
            files = self.list_files(bucket)
            
            total_size = 0
            file_count = 0
            
            for file_info in files:
                if file_info.get('metadata', {}).get('size'):
                    total_size += file_info['metadata']['size']
                file_count += 1
            
            return {
                "bucket": bucket,
                "file_count": file_count,
                "total_size_bytes": total_size,
                "total_size_mb": round(total_size / (1024 * 1024), 2)
            }
            
        except Exception as e:
            logger.error(f"Storage usage calculation failed for {bucket}: {e}")
            return {
                "bucket": bucket,
                "file_count": 0,
                "total_size_bytes": 0,
                "total_size_mb": 0
            }
    
    def _get_content_type(self, file_path: str) -> str:
        """Get content type from file extension"""
        ext = Path(file_path).suffix.lower()
        
        content_types = {
            '.csv': 'text/csv',
            '.html': 'text/html',
            '.json': 'application/json',
            '.xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            '.xls': 'application/vnd.ms-excel',
            '.txt': 'text/plain',
            '.pdf': 'application/pdf',
            '.png': 'image/png',
            '.jpg': 'image/jpeg',
            '.jpeg': 'image/jpeg',
            '.gif': 'image/gif',
            '.svg': 'image/svg+xml'
        }
        
        return content_types.get(ext, 'application/octet-stream')
    
    def cleanup_old_files(self, bucket: str, days_old: int = 30) -> int:
        """Clean up files older than specified days"""
        try:
            from datetime import datetime, timedelta
            cutoff_date = datetime.utcnow() - timedelta(days=days_old)
            
            files = self.list_files(bucket)
            deleted_count = 0
            
            for file_info in files:
                created_at = file_info.get('created_at')
                if created_at:
                    # Parse the date string (format may vary)
                    try:
                        file_date = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                        if file_date < cutoff_date:
                            file_path = file_info.get('name', '')
                            if self.delete_file(bucket, file_path):
                                deleted_count += 1
                    except Exception as e:
                        logger.warning(f"Could not parse date for file {file_info.get('name')}: {e}")
            
            logger.info(f"Cleaned up {deleted_count} old files from {bucket}")
            return deleted_count
            
        except Exception as e:
            logger.error(f"Cleanup failed for {bucket}: {e}")
            return 0


# Global storage manager instance
storage_manager = StorageManager()
