#!/usr/bin/env python3
"""
test_cloud_pipeline.py - Comprehensive tests for cloud pipeline
"""

import pytest
import tempfile
import os
import pandas as pd
import hashlib
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
import sys

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dataset_detector import DatasetDetector, DetectionResult
from preprocess_upload import compute_file_hash, check_duplicate_file, normalize_any_file
from job_manager import JobManager
from storage_manager import StorageManager
from database_models import Job, Output, UploadFile, JobRepository, OutputRepository, UploadFileRepository


class TestDatasetDetector:
    """Test dataset detection functionality"""
    
    def setup_method(self):
        """Setup test data"""
        self.detector = DatasetDetector()
        
        # Create test CSV with proper sensor data format
        self.test_data = pd.DataFrame({
            'Station_ID': ['CT', 'CT', 'TUS', 'TUS'] * 10,
            'Date_Time': pd.date_range('2024-01-01', periods=40, freq='H'),
            'PCode': ['P001', 'P002', 'P001', 'P002'] * 10,
            'Result': [10.5, 20.3, 15.7, 25.1] * 10
        })
        
        # Create test CSV with different column names
        self.test_data_alt = pd.DataFrame({
            'station': ['CT', 'CT', 'TUS', 'TUS'] * 10,
            'timestamp': pd.date_range('2024-01-01', periods=40, freq='H'),
            'parameter': ['P001', 'P002', 'P001', 'P002'] * 10,
            'value': [10.5, 20.3, 15.7, 25.1] * 10
        })
        
        # Create test CSV with minimal data
        self.test_data_minimal = pd.DataFrame({
            'col1': [1, 2, 3, 4],
            'col2': ['a', 'b', 'c', 'd'],
            'col3': [10.5, 20.3, 15.7, 25.1]
        })
    
    def test_strict_match_detection(self):
        """Test strict column matching"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            self.test_data.to_csv(f.name, index=False)
            
            result = self.detector._strict_match_detection(self.test_data)
            
            assert result is not None
            assert result.dataset_type == "sensor_data"
            assert result.confidence == 1.0
            assert result.strategy == "strict_match"
            assert len(result.detected_columns) == 4
            
            os.unlink(f.name)
    
    def test_pattern_match_detection(self):
        """Test pattern-based detection"""
        result = self.detector._pattern_match_detection(self.test_data_alt)
        
        assert result is not None
        assert result.dataset_type == "sensor_data"
        assert result.confidence > 0.5
        assert result.strategy == "pattern_match"
        assert len(result.detected_columns) >= 3
    
    def test_data_type_analysis(self):
        """Test data type analysis"""
        result = self.detector._data_type_analysis(self.test_data_minimal)
        
        assert result is not None
        assert result.strategy == "data_type_analysis"
        assert result.confidence > 0.0
    
    def test_heuristic_analysis(self):
        """Test heuristic analysis"""
        result = self.detector._heuristic_analysis(self.test_data_minimal)
        
        assert result is not None
        assert result.strategy == "heuristic_analysis"
        assert result.confidence > 0.0
    
    def test_full_detection_pipeline(self):
        """Test complete detection pipeline"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            self.test_data.to_csv(f.name, index=False)
            
            result = self.detector.detect_dataset_type(f.name)
            
            assert isinstance(result, DetectionResult)
            assert result.dataset_type in ["sensor_data", "unknown", "error"]
            assert 0.0 <= result.confidence <= 1.0
            assert result.strategy in ["strict_match", "pattern_match", "data_type_analysis", "heuristic_analysis", "fallback", "error"]
            
            os.unlink(f.name)
    
    def test_confidence_levels(self):
        """Test confidence level classification"""
        assert self.detector.get_confidence_level(0.95) == "high"
        assert self.detector.get_confidence_level(0.8) == "medium"
        assert self.detector.get_confidence_level(0.6) == "low"
        assert self.detector.get_confidence_level(0.3) == "very_low"
    
    def test_suggest_dataset_type(self):
        """Test dataset type suggestion"""
        high_conf_result = DetectionResult(
            dataset_type="sensor_data",
            confidence=0.9,
            strategy="strict_match",
            details={},
            required_columns=[],
            detected_columns={}
        )
        
        low_conf_result = DetectionResult(
            dataset_type="sensor_data",
            confidence=0.3,
            strategy="heuristic_analysis",
            details={},
            required_columns=[],
            detected_columns={}
        )
        
        assert self.detector.suggest_dataset_type(high_conf_result) == "sensor_data"
        assert self.detector.suggest_dataset_type(low_conf_result) == "likely_sensor_data"


class TestFileHashing:
    """Test file hashing functionality"""
    
    def test_compute_file_hash(self):
        """Test file hash computation"""
        with tempfile.NamedTemporaryFile(mode='w', delete=False) as f:
            f.write("test content")
            f.flush()
            
            hash1 = compute_file_hash(f.name)
            hash2 = compute_file_hash(f.name)
            
            # Same content should produce same hash
            assert hash1 == hash2
            assert len(hash1) == 64  # SHA-256 produces 64 character hex string
            
            os.unlink(f.name)
    
    def test_hash_consistency(self):
        """Test hash consistency across different file operations"""
        content = "test content for hashing"
        
        with tempfile.NamedTemporaryFile(mode='w', delete=False) as f1:
            f1.write(content)
            f1.flush()
            hash1 = compute_file_hash(f1.name)
            
        with tempfile.NamedTemporaryFile(mode='w', delete=False) as f2:
            f2.write(content)
            f2.flush()
            hash2 = compute_file_hash(f2.name)
            
        # Same content in different files should produce same hash
        assert hash1 == hash2
        
        os.unlink(f1.name)
        os.unlink(f2.name)


class TestUploadIdempotency:
    """Test upload idempotency functionality"""
    
    @patch('preprocess_upload.UploadFileRepository')
    def test_check_duplicate_file_exists(self, mock_repo):
        """Test duplicate file detection when file exists"""
        # Mock existing file
        mock_upload_file = Mock()
        mock_upload_file.file_hash = "test_hash"
        mock_repo.get_upload_file.return_value = mock_upload_file
        
        # Mock recent jobs
        mock_job = Mock()
        mock_job.job_id = "job123"
        mock_job.status = "done"
        mock_repo.get_recent_jobs_for_file.return_value = [mock_job]
        
        is_duplicate, info = check_duplicate_file("test_hash")
        
        assert is_duplicate is True
        assert "job123" in info
        assert "done" in info
    
    @patch('preprocess_upload.UploadFileRepository')
    def test_check_duplicate_file_not_exists(self, mock_repo):
        """Test duplicate file detection when file doesn't exist"""
        mock_repo.get_upload_file.return_value = None
        
        is_duplicate, info = check_duplicate_file("test_hash")
        
        assert is_duplicate is False
        assert info == ""
    
    @patch('preprocess_upload.UploadFileRepository')
    @patch('preprocess_upload.dataset_detector')
    @patch('preprocess_upload.storage_manager')
    def test_normalize_any_file_new(self, mock_storage, mock_detector, mock_repo):
        """Test file normalization for new file"""
        # Mock detection result
        mock_result = DetectionResult(
            dataset_type="sensor_data",
            confidence=0.9,
            strategy="strict_match",
            details={},
            required_columns=[],
            detected_columns={}
        )
        mock_detector.detect_dataset_type.return_value = mock_result
        
        # Mock no existing file
        mock_repo.get_upload_file.return_value = None
        
        # Create test data
        test_data = pd.DataFrame({
            'Station_ID': ['CT', 'CT'],
            'Date_Time': pd.date_range('2024-01-01', periods=2, freq='H'),
            'PCode': ['P001', 'P002'],
            'Result': [10.5, 20.3]
        })
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            test_data.to_csv(f.name, index=False)
            
            out_path, file_hash, is_duplicate = normalize_any_file(f.name)
            
            assert not is_duplicate
            assert file_hash is not None
            assert out_path.endswith('.normalized.csv')
            assert os.path.exists(out_path)
            
            # Cleanup
            os.unlink(f.name)
            if os.path.exists(out_path):
                os.unlink(out_path)


class TestJobManager:
    """Test job management functionality"""
    
    def setup_method(self):
        """Setup test environment"""
        self.job_manager = JobManager()
    
    @patch('job_manager.JobRepository')
    def test_create_job(self, mock_repo):
        """Test job creation"""
        mock_job = Mock()
        mock_job.job_id = "test_job_id"
        mock_repo.create_job.return_value = mock_job
        
        result = self.job_manager.create_job("test_hash", "test_file.csv")
        
        assert result == mock_job
        mock_repo.create_job.assert_called_once_with("test_hash", "test_file.csv", None)
    
    @patch('job_manager.JobRepository')
    def test_get_job(self, mock_repo):
        """Test job retrieval"""
        mock_job = Mock()
        mock_repo.get_job.return_value = mock_job
        
        result = self.job_manager.get_job("test_job_id")
        
        assert result == mock_job
        mock_repo.get_job.assert_called_once_with("test_job_id")
    
    @patch('job_manager.JobRepository')
    def test_update_job_status(self, mock_repo):
        """Test job status update"""
        mock_repo.update_job_status.return_value = True
        
        result = self.job_manager.update_job_status("test_job_id", "running")
        
        assert result is True
        mock_repo.update_job_status.assert_called_once_with("test_job_id", "running", None)
    
    @patch('job_manager.JobRepository')
    def test_get_queued_jobs(self, mock_repo):
        """Test getting queued jobs"""
        mock_jobs = [Mock(), Mock()]
        mock_repo.get_queued_jobs.return_value = mock_jobs
        
        result = self.job_manager.get_queued_jobs()
        
        assert result == mock_jobs
        mock_repo.get_queued_jobs.assert_called_once()
    
    def test_determine_file_type(self):
        """Test file type determination"""
        assert self.job_manager._determine_file_type("CT_Analysis_Output.csv") == "CT"
        assert self.job_manager._determine_file_type("TUS_Analysis_Output.csv") == "TUS"
        assert self.job_manager._determine_file_type("dashboard.html") == "dashboard"
        assert self.job_manager._determine_file_type("audit_lineage.csv") == "audit"
        assert self.job_manager._determine_file_type("unknown.txt") == "raw"
    
    def test_get_content_type(self):
        """Test content type determination"""
        assert self.job_manager._get_content_type("test.csv") == "text/csv"
        assert self.job_manager._get_content_type("test.html") == "text/html"
        assert self.job_manager._get_content_type("test.json") == "application/json"
        assert self.job_manager._get_content_type("test.unknown") == "application/octet-stream"


class TestStorageManager:
    """Test storage management functionality"""
    
    def setup_method(self):
        """Setup test environment"""
        self.storage_manager = StorageManager()
    
    @patch('storage_manager.storage_manager.client')
    def test_upload_file(self, mock_client):
        """Test file upload"""
        mock_client.upload_file.return_value = "uploads/test_file.csv"
        
        result = self.storage_manager.upload_file("uploads", "test_file.csv", b"test data")
        
        assert result == "uploads/test_file.csv"
        mock_client.upload_file.assert_called_once_with("uploads", "test_file.csv", b"test data", "application/octet-stream")
    
    @patch('storage_manager.storage_manager.client')
    def test_download_file(self, mock_client):
        """Test file download"""
        mock_client.download_file.return_value = b"test data"
        
        result = self.storage_manager.download_file("uploads", "test_file.csv")
        
        assert result == b"test data"
        mock_client.download_file.assert_called_once_with("uploads", "test_file.csv")
    
    @patch('storage_manager.storage_manager.client')
    def test_get_signed_url(self, mock_client):
        """Test signed URL generation"""
        mock_client.get_signed_url.return_value = "https://signed-url.com"
        
        result = self.storage_manager.get_signed_url("uploads", "test_file.csv")
        
        assert result == "https://signed-url.com"
        mock_client.get_signed_url.assert_called_once_with("uploads", "test_file.csv", 3600)
    
    def test_get_content_type(self):
        """Test content type determination"""
        assert self.storage_manager._get_content_type("test.csv") == "text/csv"
        assert self.storage_manager._get_content_type("test.html") == "text/html"
        assert self.storage_manager._get_content_type("test.xlsx") == "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        assert self.storage_manager._get_content_type("test.unknown") == "application/octet-stream"


class TestDatabaseModels:
    """Test database model functionality"""
    
    @patch('database_models.supabase_client')
    def test_job_repository_create_job(self, mock_client):
        """Test job creation in repository"""
        mock_result = {
            'job_id': 'test_job_id',
            'status': 'queued',
            'uploaded_at': '2024-01-01T00:00:00Z',
            'file_hash': 'test_hash',
            'original_filename': 'test.csv',
            'dataset_type': 'sensor_data',
            'created_at': '2024-01-01T00:00:00Z',
            'updated_at': '2024-01-01T00:00:00Z'
        }
        mock_client.execute_insert_returning.return_value = mock_result
        
        result = JobRepository.create_job("test_hash", "test.csv", "sensor_data")
        
        assert isinstance(result, Job)
        assert result.job_id == "test_job_id"
        assert result.status == "queued"
        assert result.file_hash == "test_hash"
    
    @patch('database_models.supabase_client')
    def test_output_repository_create_output(self, mock_client):
        """Test output creation in repository"""
        mock_result = {
            'output_id': 'test_output_id',
            'job_id': 'test_job_id',
            'file_type': 'CT',
            'storage_path': 'outputs/test_job_id/CT_Analysis_Output.csv',
            'file_size': 1024,
            'created_at': '2024-01-01T00:00:00Z'
        }
        mock_client.execute_insert_returning.return_value = mock_result
        
        result = OutputRepository.create_output("test_job_id", "CT", "outputs/test_job_id/CT_Analysis_Output.csv", 1024)
        
        assert isinstance(result, Output)
        assert result.output_id == "test_output_id"
        assert result.job_id == "test_job_id"
        assert result.file_type == "CT"


class TestIntegration:
    """Integration tests for complete pipeline"""
    
    @patch('preprocess_upload.UploadFileRepository')
    @patch('preprocess_upload.storage_manager')
    @patch('preprocess_upload.dataset_detector')
    def test_upload_processing_flow(self, mock_detector, mock_storage, mock_repo):
        """Test complete upload and processing flow"""
        # Mock detection result
        mock_result = DetectionResult(
            dataset_type="sensor_data",
            confidence=0.9,
            strategy="strict_match",
            details={},
            required_columns=[],
            detected_columns={}
        )
        mock_detector.detect_dataset_type.return_value = mock_result
        
        # Mock no existing file
        mock_repo.get_upload_file.return_value = None
        
        # Create test data
        test_data = pd.DataFrame({
            'Station_ID': ['CT', 'CT'],
            'Date_Time': pd.date_range('2024-01-01', periods=2, freq='H'),
            'PCode': ['P001', 'P002'],
            'Result': [10.5, 20.3]
        })
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            test_data.to_csv(f.name, index=False)
            
            # Test normalization
            out_path, file_hash, is_duplicate = normalize_any_file(f.name)
            
            assert not is_duplicate
            assert file_hash is not None
            assert out_path.endswith('.normalized.csv')
            
            # Cleanup
            os.unlink(f.name)
            if os.path.exists(out_path):
                os.unlink(out_path)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
