#!/usr/bin/env python3
"""
job_manager_advanced.py - Phase 3: Enhanced job management with retry logic and better error handling
"""

import os
import logging
import time
import threading
import uuid
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Callable, Any
from enum import Enum
from dataclasses import dataclass
from supabase_rest_client import supabase_rest

logger = logging.getLogger(__name__)


class JobStatus(Enum):
    """Job status enumeration"""
    QUEUED = "queued"
    RUNNING = "running"
    DONE = "done"
    FAILED = "failed"
    ERROR = "error"
    RETRYING = "retrying"
    CANCELLED = "cancelled"


@dataclass
class JobRetryConfig:
    """Configuration for job retry logic"""
    max_retries: int = 3
    retry_delay: int = 30  # seconds
    backoff_multiplier: float = 2.0
    max_delay: int = 300  # 5 minutes


@dataclass
class JobContext:
    """Context information for a job"""
    job_id: str
    file_path: str
    file_hash: str
    original_filename: str
    dataset_type: Optional[str] = None
    retry_count: int = 0
    last_error: Optional[str] = None
    created_at: datetime = None
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    
    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.utcnow()


class AdvancedJobManager:
    """Enhanced job manager with retry logic and better error handling"""
    
    def __init__(self):
        self.enabled = os.environ.get("ENABLE_ADVANCED_JOBS", "true").lower() == "true"
        self.retry_config = JobRetryConfig()
        self.job_queue = []
        self.running_jobs = {}
        self.job_callbacks = {}
        self.worker_thread = None
        self.shutdown_event = threading.Event()
        
        # Start background worker
        if self.enabled:
            self._start_worker()
        
        logger.info(f"Advanced job manager {'enabled' if self.enabled else 'disabled'}")
    
    def is_enabled(self) -> bool:
        """Check if advanced job management is enabled"""
        return self.enabled
    
    def _start_worker(self):
        """Start background worker thread"""
        if self.worker_thread and self.worker_thread.is_alive():
            return
        
        self.worker_thread = threading.Thread(target=self._worker_loop, daemon=True)
        self.worker_thread.start()
        logger.info("Background job worker started")
    
    def _worker_loop(self):
        """Main worker loop for processing jobs"""
        while not self.shutdown_event.is_set():
            try:
                # Process queued jobs
                if self.job_queue:
                    job_context = self.job_queue.pop(0)
                    self._process_job(job_context)
                
                # Check for stuck jobs
                self._check_stuck_jobs()
                
                # Sleep briefly
                time.sleep(5)
                
            except Exception as e:
                logger.error(f"Worker loop error: {e}")
                time.sleep(10)
    
    def create_job(self, file_path: str, file_hash: str, original_filename: str, 
                   dataset_type: Optional[str] = None, callback: Optional[Callable] = None) -> str:
        """Create a new job with enhanced tracking"""
        job_id = str(uuid.uuid4())[:8]
        
        job_context = JobContext(
            job_id=job_id,
            file_path=file_path,
            file_hash=file_hash,
            original_filename=original_filename,
            dataset_type=dataset_type
        )
        
        # Store callback if provided
        if callback:
            self.job_callbacks[job_id] = callback
        
        # Add to queue
        self.job_queue.append(job_context)
        
        # Record in database if enabled
        if supabase_rest.is_enabled():
            try:
                supabase_rest.create_job(file_hash, original_filename, dataset_type)
                logger.info(f"Job {job_id} created and queued")
            except Exception as e:
                logger.error(f"Failed to create job in database: {e}")
        
        return job_id
    
    def _process_job(self, job_context: JobContext):
        """Process a single job with retry logic"""
        job_id = job_context.job_id
        
        try:
            # Mark as running
            self.running_jobs[job_id] = job_context
            job_context.started_at = datetime.utcnow()
            
            if supabase_rest.is_enabled():
                supabase_rest.update_job_status(job_id, JobStatus.RUNNING.value)
            
            logger.info(f"Processing job {job_id} (attempt {job_context.retry_count + 1})")
            
            # Execute job callback
            if job_id in self.job_callbacks:
                callback = self.job_callbacks[job_id]
                result = callback(job_context)
                
                # Mark as done
                job_context.finished_at = datetime.utcnow()
                if supabase_rest.is_enabled():
                    supabase_rest.update_job_status(job_id, JobStatus.DONE.value)
                
                logger.info(f"Job {job_id} completed successfully")
                
            else:
                raise ValueError(f"No callback found for job {job_id}")
            
        except Exception as e:
            error_msg = str(e)
            job_context.last_error = error_msg
            job_context.retry_count += 1
            
            logger.error(f"Job {job_id} failed (attempt {job_context.retry_count}): {error_msg}")
            
            # Check if we should retry
            if job_context.retry_count < self.retry_config.max_retries:
                # Calculate retry delay with exponential backoff
                delay = min(
                    self.retry_config.retry_delay * (self.retry_config.backoff_multiplier ** (job_context.retry_count - 1)),
                    self.retry_config.max_delay
                )
                
                logger.info(f"Retrying job {job_id} in {delay} seconds")
                
                if supabase_rest.is_enabled():
                    supabase_rest.update_job_status(job_id, JobStatus.RETRYING.value, f"Retrying in {delay}s: {error_msg}")
                
                # Schedule retry
                threading.Timer(delay, self._retry_job, args=[job_context]).start()
                
            else:
                # Max retries exceeded, mark as failed
                job_context.finished_at = datetime.utcnow()
                if supabase_rest.is_enabled():
                    supabase_rest.update_job_status(job_id, JobStatus.FAILED.value, f"Max retries exceeded: {error_msg}")
                
                logger.error(f"Job {job_id} failed permanently after {job_context.retry_count} attempts")
            
        finally:
            # Remove from running jobs
            if job_id in self.running_jobs:
                del self.running_jobs[job_id]
    
    def _retry_job(self, job_context: JobContext):
        """Retry a failed job"""
        logger.info(f"Retrying job {job_context.job_id}")
        self._process_job(job_context)
    
    def _check_stuck_jobs(self):
        """Check for jobs that have been running too long"""
        current_time = datetime.utcnow()
        stuck_threshold = timedelta(hours=2)  # 2 hours timeout
        
        stuck_jobs = []
        for job_id, job_context in self.running_jobs.items():
            if job_context.started_at and (current_time - job_context.started_at) > stuck_threshold:
                stuck_jobs.append(job_id)
        
        for job_id in stuck_jobs:
            logger.warning(f"Job {job_id} appears to be stuck, marking as failed")
            job_context = self.running_jobs[job_id]
            job_context.finished_at = current_time
            
            if supabase_rest.is_enabled():
                supabase_rest.update_job_status(job_id, JobStatus.FAILED.value, "Job timeout - stuck for too long")
            
            del self.running_jobs[job_id]
    
    def get_job_status(self, job_id: str) -> Optional[Dict]:
        """Get current job status"""
        # Check running jobs first
        if job_id in self.running_jobs:
            job_context = self.running_jobs[job_id]
            return {
                "job_id": job_id,
                "status": JobStatus.RUNNING.value,
                "started_at": job_context.started_at.isoformat() if job_context.started_at else None,
                "retry_count": job_context.retry_count,
                "last_error": job_context.last_error
            }
        
        # Check database
        if supabase_rest.is_enabled():
            try:
                job = supabase_rest.get_job(job_id)
                if job:
                    return {
                        "job_id": job_id,
                        "status": job.get("status"),
                        "uploaded_at": job.get("uploaded_at"),
                        "started_at": job.get("started_at"),
                        "finished_at": job.get("finished_at"),
                        "error_msg": job.get("error_msg"),
                        "dataset_type": job.get("dataset_type")
                    }
            except Exception as e:
                logger.error(f"Failed to get job status from database: {e}")
        
        return None
    
    def get_queue_status(self) -> Dict:
        """Get current queue status"""
        return {
            "enabled": self.enabled,
            "queued_jobs": len(self.job_queue),
            "running_jobs": len(self.running_jobs),
            "retry_config": {
                "max_retries": self.retry_config.max_retries,
                "retry_delay": self.retry_config.retry_delay,
                "backoff_multiplier": self.retry_config.backoff_multiplier,
                "max_delay": self.retry_config.max_delay
            }
        }
    
    def cancel_job(self, job_id: str) -> bool:
        """Cancel a job"""
        # Remove from queue
        self.job_queue = [job for job in self.job_queue if job.job_id != job_id]
        
        # Remove from running jobs
        if job_id in self.running_jobs:
            del self.running_jobs[job_id]
        
        # Update database
        if supabase_rest.is_enabled():
            try:
                supabase_rest.update_job_status(job_id, JobStatus.CANCELLED.value, "Job cancelled by user")
                logger.info(f"Job {job_id} cancelled")
                return True
            except Exception as e:
                logger.error(f"Failed to cancel job in database: {e}")
                return False
        
        return True
    
    def get_recent_jobs(self, limit: int = 10) -> List[Dict]:
        """Get recent jobs with enhanced information"""
        jobs = []
        
        # Get from database if enabled
        if supabase_rest.is_enabled():
            try:
                db_jobs = supabase_rest.get_jobs_by_status("done", limit // 2)
                db_jobs.extend(supabase_rest.get_jobs_by_status("failed", limit // 4))
                db_jobs.extend(supabase_rest.get_jobs_by_status("running", limit // 4))
                
                for job in db_jobs:
                    jobs.append({
                        "job_id": job["job_id"],
                        "status": job["status"],
                        "original_filename": job["original_filename"],
                        "uploaded_at": job["uploaded_at"],
                        "started_at": job["started_at"],
                        "finished_at": job["finished_at"],
                        "error_msg": job["error_msg"],
                        "dataset_type": job["dataset_type"],
                        "retry_count": 0,  # Would need to track this separately
                        "source": "database"
                    })
            except Exception as e:
                logger.error(f"Failed to get recent jobs from database: {e}")
        
        # Add running jobs
        for job_id, job_context in self.running_jobs.items():
            jobs.append({
                "job_id": job_id,
                "status": JobStatus.RUNNING.value,
                "original_filename": job_context.original_filename,
                "uploaded_at": job_context.created_at.isoformat(),
                "started_at": job_context.started_at.isoformat() if job_context.started_at else None,
                "finished_at": None,
                "error_msg": job_context.last_error,
                "dataset_type": job_context.dataset_type,
                "retry_count": job_context.retry_count,
                "source": "memory"
            })
        
        # Sort by upload time (most recent first)
        jobs.sort(key=lambda x: x["uploaded_at"], reverse=True)
        return jobs[:limit]
    
    def shutdown(self):
        """Shutdown the job manager"""
        logger.info("Shutting down advanced job manager")
        self.shutdown_event.set()
        
        if self.worker_thread and self.worker_thread.is_alive():
            self.worker_thread.join(timeout=10)
        
        logger.info("Advanced job manager shutdown complete")


# Global instance
advanced_job_manager = AdvancedJobManager()
