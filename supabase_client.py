#!/usr/bin/env python3
"""
supabase_client.py - Supabase connection manager
"""

import os
import logging
from typing import Optional, Dict, Any, List
from contextlib import contextmanager
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.pool import SimpleConnectionPool
from supabase import create_client, Client
from config import config

logger = logging.getLogger(__name__)


class SupabaseClient:
    """Supabase client wrapper for database and storage operations"""
    
    def __init__(self):
        self.supabase: Client = create_client(
            config.supabase.url,
            config.supabase.service_key
        )
        
        # Connection pool for PostgreSQL
        self._connection_pool: Optional[SimpleConnectionPool] = None
        self._init_connection_pool()
    
    def _init_connection_pool(self):
        """Initialize PostgreSQL connection pool"""
        try:
            self._connection_pool = SimpleConnectionPool(
                minconn=1,
                maxconn=3,  # Free tier limit
                dsn=config.supabase.database_url
            )
            logger.info("PostgreSQL connection pool initialized")
        except Exception as e:
            logger.error(f"Failed to initialize connection pool: {e}")
            raise
    
    @contextmanager
    def get_db_connection(self):
        """Get database connection from pool"""
        conn = None
        try:
            conn = self._connection_pool.getconn()
            yield conn
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Database connection error: {e}")
            raise
        finally:
            if conn:
                self._connection_pool.putconn(conn)
    
    def execute_query(self, query: str, params: Optional[tuple] = None) -> List[Dict[str, Any]]:
        """Execute a SELECT query and return results"""
        with self.get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, params)
                return cursor.fetchall()
    
    def execute_update(self, query: str, params: Optional[tuple] = None) -> int:
        """Execute an INSERT/UPDATE/DELETE query and return affected rows"""
        with self.get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, params)
                conn.commit()
                return cursor.rowcount
    
    def execute_insert_returning(self, query: str, params: Optional[tuple] = None) -> Dict[str, Any]:
        """Execute INSERT with RETURNING clause"""
        with self.get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, params)
                conn.commit()
                return cursor.fetchone()
    
    def upload_file(self, bucket: str, file_path: str, file_data: bytes, 
                   content_type: str = "application/octet-stream") -> str:
        """Upload file to Supabase Storage"""
        try:
            # Ensure bucket exists
            try:
                self.supabase.storage.create_bucket(bucket, public=False)
            except Exception:
                pass  # Bucket might already exist
            
            # Upload file
            result = self.supabase.storage.from_(bucket).upload(
                file_path, 
                file_data,
                file_options={"content-type": content_type}
            )
            
            if result.get("error"):
                raise Exception(f"Upload failed: {result['error']}")
            
            logger.info(f"File uploaded to {bucket}/{file_path}")
            return f"{bucket}/{file_path}"
            
        except Exception as e:
            logger.error(f"File upload failed: {e}")
            raise
    
    def download_file(self, bucket: str, file_path: str) -> bytes:
        """Download file from Supabase Storage"""
        try:
            result = self.supabase.storage.from_(bucket).download(file_path)
            return result
        except Exception as e:
            logger.error(f"File download failed: {e}")
            raise
    
    def get_signed_url(self, bucket: str, file_path: str, expires_in: int = 3600) -> str:
        """Generate signed URL for file download"""
        try:
            result = self.supabase.storage.from_(bucket).create_signed_url(
                file_path, 
                expires_in
            )
            
            if result.get("error"):
                raise Exception(f"Signed URL generation failed: {result['error']}")
            
            return result["signedURL"]
        except Exception as e:
            logger.error(f"Signed URL generation failed: {e}")
            raise
    
    def delete_file(self, bucket: str, file_path: str) -> bool:
        """Delete file from Supabase Storage"""
        try:
            result = self.supabase.storage.from_(bucket).remove([file_path])
            
            if result.get("error"):
                raise Exception(f"File deletion failed: {result['error']}")
            
            logger.info(f"File deleted: {bucket}/{file_path}")
            return True
        except Exception as e:
            logger.error(f"File deletion failed: {e}")
            return False
    
    def list_files(self, bucket: str, folder: str = "") -> List[Dict[str, Any]]:
        """List files in Supabase Storage bucket"""
        try:
            result = self.supabase.storage.from_(bucket).list(folder)
            return result
        except Exception as e:
            logger.error(f"File listing failed: {e}")
            return []
    
    def health_check(self) -> Dict[str, Any]:
        """Check database and storage connectivity"""
        health = {
            "database": False,
            "storage": False,
            "timestamp": None
        }
        
        try:
            # Test database connection
            with self.get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1")
                    health["database"] = True
            
            # Test storage connection
            self.list_files("uploads")  # Try to list uploads bucket
            health["storage"] = True
            
        except Exception as e:
            logger.error(f"Health check failed: {e}")
        
        from datetime import datetime
        health["timestamp"] = datetime.utcnow().isoformat()
        
        return health


# Global client instance
supabase_client = SupabaseClient()
