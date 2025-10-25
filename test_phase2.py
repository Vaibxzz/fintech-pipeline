#!/usr/bin/env python3
"""
test_phase2.py - Test Phase 2 database integration
"""

import os
import sys
import logging
import tempfile
import requests
from pathlib import Path

# Add current directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from supabase_rest_client import supabase_rest
from supabase_storage_client import supabase_storage

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def test_database_connection():
    """Test database connectivity"""
    logger.info("Testing database connection...")
    
    health = supabase_rest.health_check()
    logger.info(f"Database health: {health}")
    
    if health["enabled"] and health["connected"]:
        logger.info("✅ Database connection successful")
        return True
    else:
        logger.warning("⚠️ Database connection failed or disabled")
        return False


def test_storage_connection():
    """Test storage connectivity"""
    logger.info("Testing storage connection...")
    
    health = supabase_storage.health_check()
    logger.info(f"Storage health: {health}")
    
    if health["enabled"] and health["connected"]:
        logger.info("✅ Storage connection successful")
        return True
    else:
        logger.warning("⚠️ Storage connection failed or disabled")
        return False


def test_job_operations():
    """Test job CRUD operations"""
    logger.info("Testing job operations...")
    
    if not supabase_rest.is_enabled():
        logger.warning("Database not enabled, skipping job operations test")
        return False
    
    try:
        # Create a test job
        test_file_hash = "test_hash_12345"
        test_filename = "test_file.csv"
        
        job = supabase_rest.create_job(test_file_hash, test_filename, "test_dataset")
        if job:
            job_id = job["job_id"]
            logger.info(f"✅ Created job: {job_id}")
            
            # Get the job
            retrieved_job = supabase_rest.get_job(job_id)
            if retrieved_job:
                logger.info("✅ Retrieved job successfully")
            else:
                logger.error("❌ Failed to retrieve job")
                return False
            
            # Update job status
            if supabase_rest.update_job_status(job_id, "running"):
                logger.info("✅ Updated job status to running")
            else:
                logger.error("❌ Failed to update job status")
                return False
            
            # Create output
            output = supabase_rest.create_output(job_id, "CT", "test/path.csv", 1024)
            if output:
                logger.info("✅ Created output successfully")
            else:
                logger.error("❌ Failed to create output")
                return False
            
            # Get outputs by job
            outputs = supabase_rest.get_outputs_by_job(job_id)
            if outputs:
                logger.info(f"✅ Retrieved {len(outputs)} outputs for job")
            else:
                logger.error("❌ Failed to retrieve outputs")
                return False
            
            # Update job to done
            if supabase_rest.update_job_status(job_id, "done"):
                logger.info("✅ Updated job status to done")
            else:
                logger.error("❌ Failed to update job status to done")
                return False
            
            return True
        else:
            logger.error("❌ Failed to create job")
            return False
            
    except Exception as e:
        logger.error(f"❌ Job operations test failed: {e}")
        return False


def test_upload_file_operations():
    """Test upload file operations"""
    logger.info("Testing upload file operations...")
    
    if not supabase_rest.is_enabled():
        logger.warning("Database not enabled, skipping upload file operations test")
        return False
    
    try:
        test_hash = "upload_test_hash_67890"
        test_name = "upload_test.csv"
        
        # Create upload file record
        upload_file = supabase_rest.create_or_update_upload_file(test_hash, test_name, "test/normalized.csv")
        if upload_file:
            logger.info("✅ Created upload file record")
        else:
            logger.error("❌ Failed to create upload file record")
            return False
        
        # Get upload file
        retrieved = supabase_rest.get_upload_file(test_hash)
        if retrieved:
            logger.info("✅ Retrieved upload file record")
        else:
            logger.error("❌ Failed to retrieve upload file record")
            return False
        
        # Update usage count
        updated = supabase_rest.create_or_update_upload_file(test_hash, test_name, "test/normalized_updated.csv")
        if updated and updated.get("usage_count", 0) > 1:
            logger.info("✅ Updated upload file usage count")
        else:
            logger.error("❌ Failed to update upload file usage count")
            return False
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Upload file operations test failed: {e}")
        return False


def test_storage_operations():
    """Test storage operations"""
    logger.info("Testing storage operations...")
    
    if not supabase_storage.is_enabled():
        logger.warning("Storage not enabled, skipping storage operations test")
        return False
    
    try:
        # Create test file
        test_content = b"test file content for phase 2"
        test_path = "test/phase2_test.txt"
        
        # Upload file
        if supabase_storage.upload_file("uploads", test_path, test_content, "text/plain"):
            logger.info("✅ Uploaded test file to storage")
        else:
            logger.error("❌ Failed to upload test file")
            return False
        
        # List files
        files = supabase_storage.list_files("uploads", "test")
        if files:
            logger.info(f"✅ Listed {len(files)} files in test directory")
        else:
            logger.error("❌ Failed to list files")
            return False
        
        # Download file
        downloaded = supabase_storage.download_file("uploads", test_path)
        if downloaded and downloaded == test_content:
            logger.info("✅ Downloaded and verified test file")
        else:
            logger.error("❌ Failed to download or verify test file")
            return False
        
        # Get signed URL
        signed_url = supabase_storage.get_signed_url("uploads", test_path)
        if signed_url:
            logger.info("✅ Generated signed URL")
            
            # Test signed URL
            response = requests.get(signed_url, timeout=10)
            if response.status_code == 200 and response.content == test_content:
                logger.info("✅ Signed URL works correctly")
            else:
                logger.error("❌ Signed URL failed")
                return False
        else:
            logger.error("❌ Failed to generate signed URL")
            return False
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Storage operations test failed: {e}")
        return False


def test_web_app_health():
    """Test web app health endpoint"""
    logger.info("Testing web app health endpoint...")
    
    try:
        # This would test the actual web app if it's running
        # For now, just test the health check logic
        from web_app_phase2 import health
        
        # Mock Flask app context
        from flask import Flask
        app = Flask(__name__)
        
        with app.app_context():
            health_response, status_code = health()
            
            if status_code == 200:
                logger.info("✅ Health endpoint logic works")
                return True
            else:
                logger.error(f"❌ Health endpoint returned status {status_code}")
                return False
                
    except Exception as e:
        logger.error(f"❌ Web app health test failed: {e}")
        return False


def main():
    """Run all Phase 2 tests"""
    logger.info("Starting Phase 2 tests...")
    
    tests = [
        ("Database Connection", test_database_connection),
        ("Storage Connection", test_storage_connection),
        ("Job Operations", test_job_operations),
        ("Upload File Operations", test_upload_file_operations),
        ("Storage Operations", test_storage_operations),
        ("Web App Health", test_web_app_health),
    ]
    
    results = {}
    
    for test_name, test_func in tests:
        logger.info(f"\n--- Running {test_name} Test ---")
        try:
            results[test_name] = test_func()
        except Exception as e:
            logger.error(f"❌ {test_name} test crashed: {e}")
            results[test_name] = False
    
    # Summary
    logger.info("\n" + "="*50)
    logger.info("PHASE 2 TEST SUMMARY")
    logger.info("="*50)
    
    passed = 0
    total = len(results)
    
    for test_name, result in results.items():
        status = "✅ PASS" if result else "❌ FAIL"
        logger.info(f"{test_name}: {status}")
        if result:
            passed += 1
    
    logger.info(f"\nOverall: {passed}/{total} tests passed")
    
    if passed == total:
        logger.info("🎉 All Phase 2 tests passed! Ready for deployment.")
        return True
    else:
        logger.warning(f"⚠️ {total - passed} tests failed. Check configuration.")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
