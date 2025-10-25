#!/usr/bin/env python3
"""
database_models.py - SQLAlchemy models for database operations
"""

import uuid
from datetime import datetime
from typing import Optional, List, Dict, Any
from dataclasses import dataclass
from supabase_client import supabase_client


@dataclass
class Job:
    """Job model"""
    job_id: str
    status: str
    uploaded_at: datetime
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    file_hash: str = ""
    original_filename: str = ""
    dataset_type: Optional[str] = None
    error_msg: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


@dataclass
class Output:
    """Output model"""
    output_id: str
    job_id: str
    file_type: str
    storage_path: str
    file_size: Optional[int] = None
    created_at: Optional[datetime] = None


@dataclass
class UploadFile:
    """Upload file model"""
    file_hash: str
    original_name: str
    normalized_path: Optional[str] = None
    first_seen: Optional[datetime] = None
    last_used: Optional[datetime] = None
    usage_count: int = 1


class JobRepository:
    """Repository for job operations"""
    
    @staticmethod
    def create_job(file_hash: str, original_filename: str, 
                   dataset_type: Optional[str] = None) -> Job:
        """Create a new job"""
        query = """
        INSERT INTO jobs (file_hash, original_filename, dataset_type, status)
        VALUES (%s, %s, %s, 'queued')
        RETURNING job_id, status, uploaded_at, file_hash, original_filename, 
                  dataset_type, created_at, updated_at
        """
        
        result = supabase_client.execute_insert_returning(
            query, (file_hash, original_filename, dataset_type)
        )
        
        return Job(
            job_id=str(result['job_id']),
            status=result['status'],
            uploaded_at=result['uploaded_at'],
            file_hash=result['file_hash'],
            original_filename=result['original_filename'],
            dataset_type=result['dataset_type'],
            created_at=result['created_at'],
            updated_at=result['updated_at']
        )
    
    @staticmethod
    def get_job(job_id: str) -> Optional[Job]:
        """Get job by ID"""
        query = """
        SELECT job_id, status, uploaded_at, started_at, finished_at,
               file_hash, original_filename, dataset_type, error_msg,
               created_at, updated_at
        FROM jobs WHERE job_id = %s
        """
        
        results = supabase_client.execute_query(query, (job_id,))
        if not results:
            return None
        
        result = results[0]
        return Job(
            job_id=str(result['job_id']),
            status=result['status'],
            uploaded_at=result['uploaded_at'],
            started_at=result['started_at'],
            finished_at=result['finished_at'],
            file_hash=result['file_hash'],
            original_filename=result['original_filename'],
            dataset_type=result['dataset_type'],
            error_msg=result['error_msg'],
            created_at=result['created_at'],
            updated_at=result['updated_at']
        )
    
    @staticmethod
    def update_job_status(job_id: str, status: str, 
                         error_msg: Optional[str] = None) -> bool:
        """Update job status"""
        query = """
        UPDATE jobs 
        SET status = %s, error_msg = %s, updated_at = NOW()
        WHERE job_id = %s
        """
        
        if status == 'running':
            query = """
            UPDATE jobs 
            SET status = %s, started_at = NOW(), error_msg = %s, updated_at = NOW()
            WHERE job_id = %s
            """
        elif status in ['done', 'failed', 'error']:
            query = """
            UPDATE jobs 
            SET status = %s, finished_at = NOW(), error_msg = %s, updated_at = NOW()
            WHERE job_id = %s
            """
        
        rows_affected = supabase_client.execute_update(
            query, (status, error_msg, job_id)
        )
        return rows_affected > 0
    
    @staticmethod
    def get_queued_jobs() -> List[Job]:
        """Get all queued jobs"""
        query = """
        SELECT job_id, status, uploaded_at, started_at, finished_at,
               file_hash, original_filename, dataset_type, error_msg,
               created_at, updated_at
        FROM jobs WHERE status = 'queued'
        ORDER BY uploaded_at ASC
        """
        
        results = supabase_client.execute_query(query)
        return [
            Job(
                job_id=str(r['job_id']),
                status=r['status'],
                uploaded_at=r['uploaded_at'],
                started_at=r['started_at'],
                finished_at=r['finished_at'],
                file_hash=r['file_hash'],
                original_filename=r['original_filename'],
                dataset_type=r['dataset_type'],
                error_msg=r['error_msg'],
                created_at=r['created_at'],
                updated_at=r['updated_at']
            ) for r in results
        ]
    
    @staticmethod
    def get_jobs_by_status(status: str, limit: int = 100) -> List[Job]:
        """Get jobs by status"""
        query = """
        SELECT job_id, status, uploaded_at, started_at, finished_at,
               file_hash, original_filename, dataset_type, error_msg,
               created_at, updated_at
        FROM jobs WHERE status = %s
        ORDER BY uploaded_at DESC
        LIMIT %s
        """
        
        results = supabase_client.execute_query(query, (status, limit))
        return [
            Job(
                job_id=str(r['job_id']),
                status=r['status'],
                uploaded_at=r['uploaded_at'],
                started_at=r['started_at'],
                finished_at=r['finished_at'],
                file_hash=r['file_hash'],
                original_filename=r['original_filename'],
                dataset_type=r['dataset_type'],
                error_msg=r['error_msg'],
                created_at=r['created_at'],
                updated_at=r['updated_at']
            ) for r in results
        ]


class OutputRepository:
    """Repository for output operations"""
    
    @staticmethod
    def create_output(job_id: str, file_type: str, storage_path: str, 
                     file_size: Optional[int] = None) -> Output:
        """Create a new output record"""
        query = """
        INSERT INTO outputs (job_id, file_type, storage_path, file_size)
        VALUES (%s, %s, %s, %s)
        RETURNING output_id, job_id, file_type, storage_path, file_size, created_at
        """
        
        result = supabase_client.execute_insert_returning(
            query, (job_id, file_type, storage_path, file_size)
        )
        
        return Output(
            output_id=str(result['output_id']),
            job_id=str(result['job_id']),
            file_type=result['file_type'],
            storage_path=result['storage_path'],
            file_size=result['file_size'],
            created_at=result['created_at']
        )
    
    @staticmethod
    def get_outputs_by_job(job_id: str) -> List[Output]:
        """Get all outputs for a job"""
        query = """
        SELECT output_id, job_id, file_type, storage_path, file_size, created_at
        FROM outputs WHERE job_id = %s
        ORDER BY created_at ASC
        """
        
        results = supabase_client.execute_query(query, (job_id,))
        return [
            Output(
                output_id=str(r['output_id']),
                job_id=str(r['job_id']),
                file_type=r['file_type'],
                storage_path=r['storage_path'],
                file_size=r['file_size'],
                created_at=r['created_at']
            ) for r in results
        ]
    
    @staticmethod
    def get_output(output_id: str) -> Optional[Output]:
        """Get output by ID"""
        query = """
        SELECT output_id, job_id, file_type, storage_path, file_size, created_at
        FROM outputs WHERE output_id = %s
        """
        
        results = supabase_client.execute_query(query, (output_id,))
        if not results:
            return None
        
        result = results[0]
        return Output(
            output_id=str(result['output_id']),
            job_id=str(result['job_id']),
            file_type=result['file_type'],
            storage_path=result['storage_path'],
            file_size=result['file_size'],
            created_at=result['created_at']
        )


class UploadFileRepository:
    """Repository for upload file operations"""
    
    @staticmethod
    def create_or_update_upload_file(file_hash: str, original_name: str, 
                                   normalized_path: Optional[str] = None) -> UploadFile:
        """Create or update upload file record"""
        query = """
        INSERT INTO upload_files (file_hash, original_name, normalized_path, usage_count)
        VALUES (%s, %s, %s, 1)
        ON CONFLICT (file_hash) 
        DO UPDATE SET 
            last_used = NOW(),
            usage_count = upload_files.usage_count + 1,
            normalized_path = COALESCE(EXCLUDED.normalized_path, upload_files.normalized_path)
        RETURNING file_hash, original_name, normalized_path, first_seen, last_used, usage_count
        """
        
        result = supabase_client.execute_insert_returning(
            query, (file_hash, original_name, normalized_path)
        )
        
        return UploadFile(
            file_hash=result['file_hash'],
            original_name=result['original_name'],
            normalized_path=result['normalized_path'],
            first_seen=result['first_seen'],
            last_used=result['last_used'],
            usage_count=result['usage_count']
        )
    
    @staticmethod
    def get_upload_file(file_hash: str) -> Optional[UploadFile]:
        """Get upload file by hash"""
        query = """
        SELECT file_hash, original_name, normalized_path, first_seen, last_used, usage_count
        FROM upload_files WHERE file_hash = %s
        """
        
        results = supabase_client.execute_query(query, (file_hash,))
        if not results:
            return None
        
        result = results[0]
        return UploadFile(
            file_hash=result['file_hash'],
            original_name=result['original_name'],
            normalized_path=result['normalized_path'],
            first_seen=result['first_seen'],
            last_used=result['last_used'],
            usage_count=result['usage_count']
        )
    
    @staticmethod
    def get_recent_jobs_for_file(file_hash: str, limit: int = 5) -> List[Job]:
        """Get recent jobs for a file hash"""
        query = """
        SELECT job_id, status, uploaded_at, started_at, finished_at,
               file_hash, original_filename, dataset_type, error_msg,
               created_at, updated_at
        FROM jobs WHERE file_hash = %s
        ORDER BY uploaded_at DESC
        LIMIT %s
        """
        
        results = supabase_client.execute_query(query, (file_hash, limit))
        return [
            Job(
                job_id=str(r['job_id']),
                status=r['status'],
                uploaded_at=r['uploaded_at'],
                started_at=r['started_at'],
                finished_at=r['finished_at'],
                file_hash=r['file_hash'],
                original_filename=r['original_filename'],
                dataset_type=r['dataset_type'],
                error_msg=r['error_msg'],
                created_at=r['created_at'],
                updated_at=r['updated_at']
            ) for r in results
        ]
