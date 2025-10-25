#!/usr/bin/env python3
"""
job_manager.py - Job CRUD operations and queue management
"""

import logging
import threading
import time
from typing import Optional, List, Dict, Any
from datetime import datetime
from database_models import Job, JobRepository, OutputRepository
from supabase_client import supabase_client

logger = logging.getLogger(__name__)


class JobManager:
    """Manages job lifecycle and queue operations"""
    
    def __init__(self):
        self._lock = threading.Lock()
        self._processing = False
        self._worker_thread: Optional[threading.Thread] = None
    
    def create_job(self, file_hash: str, original_filename: str, 
                   dataset_type: Optional[str] = None) -> Job:
        """Create a new job"""
        try:
            job = JobRepository.create_job(file_hash, original_filename, dataset_type)
            logger.info(f"Created job {job.job_id} for file {original_filename}")
            return job
        except Exception as e:
            logger.error(f"Failed to create job: {e}")
            raise
    
    def get_job(self, job_id: str) -> Optional[Job]:
        """Get job by ID"""
        try:
            return JobRepository.get_job(job_id)
        except Exception as e:
            logger.error(f"Failed to get job {job_id}: {e}")
            return None
    
    def update_job_status(self, job_id: str, status: str, 
                         error_msg: Optional[str] = None) -> bool:
        """Update job status"""
        try:
            success = JobRepository.update_job_status(job_id, status, error_msg)
            if success:
                logger.info(f"Updated job {job_id} status to {status}")
            return success
        except Exception as e:
            logger.error(f"Failed to update job {job_id} status: {e}")
            return False
    
    def get_queued_jobs(self) -> List[Job]:
        """Get all queued jobs"""
        try:
            return JobRepository.get_queued_jobs()
        except Exception as e:
            logger.error(f"Failed to get queued jobs: {e}")
            return []
    
    def get_jobs_by_status(self, status: str, limit: int = 100) -> List[Job]:
        """Get jobs by status"""
        try:
            return JobRepository.get_jobs_by_status(status, limit)
        except Exception as e:
            logger.error(f"Failed to get jobs by status {status}: {e}")
            return []
    
    def get_job_with_outputs(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Get job with its outputs"""
        try:
            job = self.get_job(job_id)
            if not job:
                return None
            
            outputs = OutputRepository.get_outputs_by_job(job_id)
            
            return {
                "job": job,
                "outputs": outputs
            }
        except Exception as e:
            logger.error(f"Failed to get job with outputs {job_id}: {e}")
            return None
    
    def start_worker(self):
        """Start background worker thread"""
        with self._lock:
            if self._processing:
                logger.info("Worker already running")
                return
            
            self._processing = True
            self._worker_thread = threading.Thread(target=self._worker_loop, daemon=True)
            self._worker_thread.start()
            logger.info("Background worker started")
    
    def stop_worker(self):
        """Stop background worker thread"""
        with self._lock:
            if not self._processing:
                return
            
            self._processing = False
            if self._worker_thread:
                self._worker_thread.join(timeout=5)
            logger.info("Background worker stopped")
    
    def _worker_loop(self):
        """Main worker loop"""
        logger.info("Worker loop started")
        
        while self._processing:
            try:
                # Get next queued job
                queued_jobs = self.get_queued_jobs()
                
                if not queued_jobs:
                    time.sleep(2)  # Wait 2 seconds before checking again
                    continue
                
                # Process the first queued job
                job = queued_jobs[0]
                self._process_job(job)
                
            except Exception as e:
                logger.error(f"Worker loop error: {e}")
                time.sleep(5)  # Wait 5 seconds on error
        
        logger.info("Worker loop ended")
    
    def _process_job(self, job: Job):
        """Process a single job"""
        logger.info(f"Processing job {job.job_id}")
        
        try:
            # Update status to running
            self.update_job_status(job.job_id, "running")
            
            # Import here to avoid circular imports
            import subprocess
            import os
            
            # Step 1: Process data
            logger.info(f"Job {job.job_id}: Starting data processing")
            cmd = [
                "python3", "process_data_fintech.py", 
                "--raw", f"uploads/{job.file_hash}.csv",  # Assuming normalized file
                "--out_dir", f"outputs/{job.job_id}",
                "--job_id", job.job_id
            ]
            
            proc = subprocess.run(
                cmd, 
                cwd=".", 
                capture_output=True, 
                text=True, 
                timeout=3600
            )
            
            if proc.returncode != 0:
                error_msg = f"Data processing failed: {proc.stderr[:1000]}"
                self.update_job_status(job.job_id, "failed", error_msg)
                return
            
            # Step 2: Generate dashboard
            logger.info(f"Job {job.job_id}: Generating dashboard")
            cmd2 = [
                "python3", "generate_dashboard.py",
                "--job_id", job.job_id
            ]
            
            proc2 = subprocess.run(
                cmd2,
                cwd=".",
                capture_output=True,
                text=True,
                timeout=300
            )
            
            if proc2.returncode != 0:
                error_msg = f"Dashboard generation failed: {proc2.stderr[:1000]}"
                self.update_job_status(job.job_id, "failed", error_msg)
                return
            
            # Step 3: Upload outputs to storage
            self._upload_job_outputs(job.job_id)
            
            # Mark as done
            self.update_job_status(job.job_id, "done")
            logger.info(f"Job {job.job_id} completed successfully")
            
        except subprocess.TimeoutExpired:
            error_msg = "Job processing timeout"
            self.update_job_status(job.job_id, "failed", error_msg)
            logger.error(f"Job {job.job_id} timed out")
            
        except Exception as e:
            error_msg = f"Unexpected error: {str(e)}"
            self.update_job_status(job.job_id, "error", error_msg)
            logger.error(f"Job {job.job_id} failed with error: {e}")
    
    def _upload_job_outputs(self, job_id: str):
        """Upload job outputs to Supabase Storage"""
        try:
            from storage_manager import StorageManager
            storage_manager = StorageManager()
            
            output_dir = f"outputs/{job_id}"
            if not os.path.exists(output_dir):
                logger.warning(f"Output directory {output_dir} not found")
                return
            
            # Upload each file in the output directory
            for filename in os.listdir(output_dir):
                file_path = os.path.join(output_dir, filename)
                if os.path.isfile(file_path):
                    # Determine file type
                    file_type = self._determine_file_type(filename)
                    
                    # Upload to storage
                    storage_path = f"outputs/{job_id}/{filename}"
                    with open(file_path, 'rb') as f:
                        file_data = f.read()
                    
                    storage_manager.upload_file(
                        "outputs", 
                        storage_path, 
                        file_data,
                        self._get_content_type(filename)
                    )
                    
                    # Record in database
                    OutputRepository.create_output(
                        job_id, 
                        file_type, 
                        storage_path, 
                        len(file_data)
                    )
                    
                    logger.info(f"Uploaded {filename} for job {job_id}")
        
        except Exception as e:
            logger.error(f"Failed to upload outputs for job {job_id}: {e}")
            raise
    
    def _determine_file_type(self, filename: str) -> str:
        """Determine file type from filename"""
        filename_lower = filename.lower()
        
        if "ct_analysis" in filename_lower:
            return "CT"
        elif "tus_analysis" in filename_lower:
            return "TUS"
        elif "dashboard" in filename_lower:
            return "dashboard"
        elif "audit" in filename_lower:
            return "audit"
        else:
            return "raw"
    
    def _get_content_type(self, filename: str) -> str:
        """Get content type from filename"""
        if filename.endswith('.csv'):
            return "text/csv"
        elif filename.endswith('.html'):
            return "text/html"
        elif filename.endswith('.json'):
            return "application/json"
        else:
            return "application/octet-stream"
    
    def get_job_statistics(self) -> Dict[str, Any]:
        """Get job statistics"""
        try:
            stats = {}
            
            for status in ['queued', 'running', 'done', 'failed', 'error']:
                jobs = self.get_jobs_by_status(status, limit=1000)
                stats[status] = len(jobs)
            
            return stats
        except Exception as e:
            logger.error(f"Failed to get job statistics: {e}")
            return {}


# Global job manager instance
job_manager = JobManager()
