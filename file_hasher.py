#!/usr/bin/env python3
"""
file_hasher.py - Phase 3: File hashing and duplicate detection with SHA-256
"""

import hashlib
import logging
import os
from pathlib import Path
from typing import Optional, Dict, List, Tuple
from datetime import datetime
from supabase_rest_client import supabase_rest

logger = logging.getLogger(__name__)


class FileHasher:
    """Handles file hashing and duplicate detection"""
    
    def __init__(self):
        self.enabled = os.environ.get("ENABLE_DUPLICATE_DETECTION", "true").lower() == "true"
        logger.info(f"File hashing {'enabled' if self.enabled else 'disabled'}")
    
    def is_enabled(self) -> bool:
        """Check if duplicate detection is enabled"""
        return self.enabled
    
    def compute_file_hash(self, file_path: str) -> str:
        """Compute SHA-256 hash of a file"""
        try:
            hash_sha256 = hashlib.sha256()
            with open(file_path, "rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hash_sha256.update(chunk)
            
            file_hash = hash_sha256.hexdigest()
            logger.info(f"Computed hash for {file_path}: {file_hash[:16]}...")
            return file_hash
            
        except Exception as e:
            logger.error(f"Failed to compute hash for {file_path}: {e}")
            raise
    
    def check_duplicate_file(self, file_hash: str) -> Tuple[bool, Optional[Dict]]:
        """Check if file hash exists in database"""
        if not self.enabled or not supabase_rest.is_enabled():
            return False, None
        
        try:
            upload_file = supabase_rest.get_upload_file(file_hash)
            if upload_file:
                logger.info(f"Duplicate file found: {upload_file['original_name']}")
                return True, upload_file
            return False, None
            
        except Exception as e:
            logger.error(f"Duplicate check failed for hash {file_hash[:16]}...: {e}")
            return False, None
    
    def get_recent_jobs_for_file(self, file_hash: str, limit: int = 5) -> List[Dict]:
        """Get recent jobs that processed this file"""
        if not self.enabled or not supabase_rest.is_enabled():
            return []
        
        try:
            jobs = supabase_rest.get_recent_jobs_for_file(file_hash, limit)
            logger.info(f"Found {len(jobs)} recent jobs for file hash {file_hash[:16]}...")
            return jobs
            
        except Exception as e:
            logger.error(f"Failed to get recent jobs for hash {file_hash[:16]}...: {e}")
            return []
    
    def record_file_upload(self, file_hash: str, original_name: str, normalized_path: Optional[str] = None) -> Optional[Dict]:
        """Record file upload in database"""
        if not self.enabled or not supabase_rest.is_enabled():
            return None
        
        try:
            upload_file = supabase_rest.create_or_update_upload_file(file_hash, original_name, normalized_path)
            if upload_file:
                logger.info(f"Recorded file upload: {original_name} -> {file_hash[:16]}...")
            return upload_file
            
        except Exception as e:
            logger.error(f"Failed to record file upload: {e}")
            return None
    
    def get_file_statistics(self, file_hash: str) -> Dict:
        """Get comprehensive file statistics"""
        stats = {
            "file_hash": file_hash,
            "is_duplicate": False,
            "upload_record": None,
            "recent_jobs": [],
            "total_usage": 0,
            "first_seen": None,
            "last_used": None,
            "confidence": "unknown"
        }
        
        if not self.enabled:
            stats["confidence"] = "disabled"
            return stats
        
        try:
            # Check for duplicate
            is_duplicate, upload_record = self.check_duplicate_file(file_hash)
            stats["is_duplicate"] = is_duplicate
            stats["upload_record"] = upload_record
            
            if upload_record:
                stats["total_usage"] = upload_record.get("usage_count", 0)
                stats["first_seen"] = upload_record.get("first_seen")
                stats["last_used"] = upload_record.get("last_used")
                
                # Get recent jobs
                recent_jobs = self.get_recent_jobs_for_file(file_hash, 5)
                stats["recent_jobs"] = recent_jobs
                
                # Calculate confidence based on usage and recency
                if stats["total_usage"] > 3:
                    stats["confidence"] = "high"
                elif stats["total_usage"] > 1:
                    stats["confidence"] = "medium"
                else:
                    stats["confidence"] = "low"
            else:
                stats["confidence"] = "new"
                
        except Exception as e:
            logger.error(f"Failed to get file statistics: {e}")
            stats["confidence"] = "error"
        
        return stats
    
    def generate_duplicate_report(self, file_hash: str) -> str:
        """Generate a human-readable duplicate report"""
        stats = self.get_file_statistics(file_hash)
        
        if not stats["is_duplicate"]:
            return "This is a new file that hasn't been processed before."
        
        report_lines = [
            f"⚠️ Duplicate File Detected",
            f"",
            f"File: {stats['upload_record']['original_name']}",
            f"Hash: {file_hash[:16]}...",
            f"Total Usage: {stats['total_usage']} times",
            f"First Seen: {stats['first_seen'] or 'Unknown'}",
            f"Last Used: {stats['last_used'] or 'Unknown'}",
            f"Confidence: {stats['confidence'].upper()}",
            f""
        ]
        
        if stats["recent_jobs"]:
            report_lines.append("Recent Processing Jobs:")
            for i, job in enumerate(stats["recent_jobs"][:3], 1):
                status_emoji = {
                    "done": "✅",
                    "running": "🔄", 
                    "failed": "❌",
                    "error": "⚠️"
                }.get(job.get("status", "unknown"), "❓")
                
                report_lines.append(f"  {i}. Job {job['job_id']} - {status_emoji} {job['status'].upper()}")
                report_lines.append(f"     Uploaded: {job.get('uploaded_at', 'Unknown')[:19]}")
                if job.get("error_msg"):
                    report_lines.append(f"     Error: {job['error_msg'][:100]}...")
        
        report_lines.extend([
            f"",
            f"You can:",
            f"• Reprocess anyway (creates new job)",
            f"• View existing results from previous jobs",
            f"• Cancel and use a different file"
        ])
        
        return "\n".join(report_lines)


# Global instance
file_hasher = FileHasher()
