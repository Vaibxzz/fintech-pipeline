#!/usr/bin/env python3
"""
test_dataset_detection.py - Specific tests for dataset detection
"""

import pytest
import pandas as pd
import tempfile
import os
import sys

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dataset_detector import DatasetDetector, DetectionResult


class TestDatasetDetectionStrategies:
    """Test individual detection strategies"""
    
    def setup_method(self):
        """Setup test data"""
        self.detector = DatasetDetector()
    
    def test_strict_match_perfect(self):
        """Test strict matching with perfect column names"""
        df = pd.DataFrame({
            'Station_ID': ['CT', 'CT', 'TUS', 'TUS'],
            'Date_Time': pd.date_range('2024-01-01', periods=4, freq='H'),
            'PCode': ['P001', 'P002', 'P001', 'P002'],
            'Result': [10.5, 20.3, 15.7, 25.1]
        })
        
        result = self.detector._strict_match_detection(df)
        
        assert result is not None
        assert result.confidence == 1.0
        assert result.dataset_type == "sensor_data"
        assert len(result.detected_columns) == 4
    
    def test_strict_match_partial(self):
        """Test strict matching with partial column names"""
        df = pd.DataFrame({
            'Station_ID': ['CT', 'CT', 'TUS', 'TUS'],
            'Date_Time': pd.date_range('2024-01-01', periods=4, freq='H'),
            'PCode': ['P001', 'P002', 'P001', 'P002'],
            'Other_Column': [10.5, 20.3, 15.7, 25.1]  # Missing 'Result'
        })
        
        result = self.detector._strict_match_detection(df)
        
        assert result is not None
        assert result.confidence == 0.8  # 3 out of 4 columns match
        assert result.dataset_type == "sensor_data"
    
    def test_strict_match_none(self):
        """Test strict matching with no matching columns"""
        df = pd.DataFrame({
            'col1': ['CT', 'CT', 'TUS', 'TUS'],
            'col2': pd.date_range('2024-01-01', periods=4, freq='H'),
            'col3': ['P001', 'P002', 'P001', 'P002'],
            'col4': [10.5, 20.3, 15.7, 25.1]
        })
        
        result = self.detector._strict_match_detection(df)
        
        assert result is None
    
    def test_pattern_match_good(self):
        """Test pattern matching with good column names"""
        df = pd.DataFrame({
            'station_id': ['CT', 'CT', 'TUS', 'TUS'],
            'timestamp': pd.date_range('2024-01-01', periods=4, freq='H'),
            'parameter_code': ['P001', 'P002', 'P001', 'P002'],
            'measurement_value': [10.5, 20.3, 15.7, 25.1]
        })
        
        result = self.detector._pattern_match_detection(df)
        
        assert result is not None
        assert result.confidence > 0.5
        assert result.dataset_type == "sensor_data"
        assert len(result.detected_columns) >= 3
    
    def test_pattern_match_poor(self):
        """Test pattern matching with poor column names"""
        df = pd.DataFrame({
            'col1': ['CT', 'CT', 'TUS', 'TUS'],
            'col2': pd.date_range('2024-01-01', periods=4, freq='H'),
            'col3': ['P001', 'P002', 'P001', 'P002'],
            'col4': [10.5, 20.3, 15.7, 25.1]
        })
        
        result = self.detector._pattern_match_detection(df)
        
        assert result is None
    
    def test_data_type_analysis_good(self):
        """Test data type analysis with good data structure"""
        df = pd.DataFrame({
            'station': ['CT', 'CT', 'TUS', 'TUS'],
            'timestamp': pd.date_range('2024-01-01', periods=4, freq='H'),
            'parameter': ['P001', 'P002', 'P001', 'P002'],
            'value': [10.5, 20.3, 15.7, 25.1]
        })
        
        result = self.detector._data_type_analysis(df)
        
        assert result is not None
        assert result.confidence >= 0.5
        assert result.strategy == "data_type_analysis"
    
    def test_data_type_analysis_poor(self):
        """Test data type analysis with poor data structure"""
        df = pd.DataFrame({
            'col1': ['a', 'b', 'c', 'd'],
            'col2': ['x', 'y', 'z', 'w'],
            'col3': ['1', '2', '3', '4']
        })
        
        result = self.detector._data_type_analysis(df)
        
        assert result is None
    
    def test_heuristic_analysis_good(self):
        """Test heuristic analysis with good data"""
        df = pd.DataFrame({
            'sensor_id': ['CT', 'CT', 'TUS', 'TUS'],
            'recorded_time': pd.date_range('2024-01-01', periods=4, freq='H'),
            'param_type': ['P001', 'P002', 'P001', 'P002'],
            'reading': [10.5, 20.3, 15.7, 25.1]
        })
        
        result = self.detector._heuristic_analysis(df)
        
        assert result is not None
        assert result.confidence >= 0.4
        assert result.strategy == "heuristic_analysis"
    
    def test_heuristic_analysis_poor(self):
        """Test heuristic analysis with poor data"""
        df = pd.DataFrame({
            'col1': ['a', 'b'],
            'col2': ['x', 'y']
        })
        
        result = self.detector._heuristic_analysis(df)
        
        assert result is None


class TestDatasetDetectionIntegration:
    """Test complete dataset detection pipeline"""
    
    def setup_method(self):
        """Setup test data"""
        self.detector = DatasetDetector()
    
    def test_detect_sensor_data_perfect(self):
        """Test detection of perfect sensor data"""
        df = pd.DataFrame({
            'Station_ID': ['CT', 'CT', 'TUS', 'TUS'],
            'Date_Time': pd.date_range('2024-01-01', periods=4, freq='H'),
            'PCode': ['P001', 'P002', 'P001', 'P002'],
            'Result': [10.5, 20.3, 15.7, 25.1]
        })
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            df.to_csv(f.name, index=False)
            
            result = self.detector.detect_dataset_type(f.name)
            
            assert isinstance(result, DetectionResult)
            assert result.dataset_type == "sensor_data"
            assert result.confidence >= 0.9
            assert result.strategy == "strict_match"
            
            os.unlink(f.name)
    
    def test_detect_sensor_data_pattern(self):
        """Test detection of sensor data with pattern matching"""
        df = pd.DataFrame({
            'station_id': ['CT', 'CT', 'TUS', 'TUS'],
            'timestamp': pd.date_range('2024-01-01', periods=4, freq='H'),
            'parameter_code': ['P001', 'P002', 'P001', 'P002'],
            'measurement_value': [10.5, 20.3, 15.7, 25.1]
        })
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            df.to_csv(f.name, index=False)
            
            result = self.detector.detect_dataset_type(f.name)
            
            assert isinstance(result, DetectionResult)
            assert result.dataset_type == "sensor_data"
            assert result.confidence > 0.5
            assert result.strategy in ["pattern_match", "data_type_analysis", "heuristic_analysis"]
            
            os.unlink(f.name)
    
    def test_detect_unknown_data(self):
        """Test detection of unknown data format"""
        df = pd.DataFrame({
            'col1': ['a', 'b', 'c', 'd'],
            'col2': ['x', 'y', 'z', 'w'],
            'col3': ['1', '2', '3', '4'],
            'col4': ['alpha', 'beta', 'gamma', 'delta']
        })
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            df.to_csv(f.name, index=False)
            
            result = self.detector.detect_dataset_type(f.name)
            
            assert isinstance(result, DetectionResult)
            assert result.dataset_type == "unknown"
            assert result.confidence == 0.0
            assert result.strategy == "fallback"
            
            os.unlink(f.name)
    
    def test_detect_financial_data(self):
        """Test detection of financial data format"""
        df = pd.DataFrame({
            'transaction_id': ['TXN001', 'TXN002', 'TXN003', 'TXN004'],
            'date': pd.date_range('2024-01-01', periods=4, freq='D'),
            'amount': [100.50, 200.75, 150.25, 300.00],
            'account': ['ACC001', 'ACC002', 'ACC001', 'ACC003']
        })
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            df.to_csv(f.name, index=False)
            
            result = self.detector.detect_dataset_type(f.name)
            
            assert isinstance(result, DetectionResult)
            # Should detect as sensor_data due to numeric values and date
            assert result.dataset_type == "sensor_data"
            assert result.confidence > 0.0
            
            os.unlink(f.name)
    
    def test_detect_excel_file(self):
        """Test detection of Excel file"""
        df = pd.DataFrame({
            'Station_ID': ['CT', 'CT', 'TUS', 'TUS'],
            'Date_Time': pd.date_range('2024-01-01', periods=4, freq='H'),
            'PCode': ['P001', 'P002', 'P001', 'P002'],
            'Result': [10.5, 20.3, 15.7, 25.1]
        })
        
        with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as f:
            df.to_excel(f.name, index=False)
            
            result = self.detector.detect_dataset_type(f.name)
            
            assert isinstance(result, DetectionResult)
            assert result.dataset_type == "sensor_data"
            assert result.confidence >= 0.9
            
            os.unlink(f.name)


class TestDetectionConfidence:
    """Test confidence scoring and classification"""
    
    def setup_method(self):
        """Setup test data"""
        self.detector = DatasetDetector()
    
    def test_confidence_levels(self):
        """Test confidence level classification"""
        assert self.detector.get_confidence_level(0.95) == "high"
        assert self.detector.get_confidence_level(0.85) == "medium"
        assert self.detector.get_confidence_level(0.75) == "medium"
        assert self.detector.get_confidence_level(0.65) == "low"
        assert self.detector.get_confidence_level(0.45) == "low"
        assert self.detector.get_confidence_level(0.25) == "very_low"
    
    def test_suggest_dataset_type(self):
        """Test dataset type suggestion based on confidence"""
        high_conf = DetectionResult(
            dataset_type="sensor_data",
            confidence=0.9,
            strategy="strict_match",
            details={},
            required_columns=[],
            detected_columns={}
        )
        
        medium_conf = DetectionResult(
            dataset_type="sensor_data",
            confidence=0.6,
            strategy="pattern_match",
            details={},
            required_columns=[],
            detected_columns={}
        )
        
        low_conf = DetectionResult(
            dataset_type="sensor_data",
            confidence=0.3,
            strategy="heuristic_analysis",
            details={},
            required_columns=[],
            detected_columns={}
        )
        
        assert self.detector.suggest_dataset_type(high_conf) == "sensor_data"
        assert self.detector.suggest_dataset_type(medium_conf) == "likely_sensor_data"
        assert self.detector.suggest_dataset_type(low_conf) == "unknown"
    
    def test_required_actions(self):
        """Test required actions based on detection result"""
        high_conf = DetectionResult(
            dataset_type="sensor_data",
            confidence=0.9,
            strategy="strict_match",
            details={},
            required_columns=[],
            detected_columns={"Station_ID": "station", "Date_Time": "timestamp"}
        )
        
        low_conf = DetectionResult(
            dataset_type="unknown",
            confidence=0.2,
            strategy="fallback",
            details={},
            required_columns=[],
            detected_columns={}
        )
        
        high_actions = self.detector.get_required_actions(high_conf)
        low_actions = self.detector.get_required_actions(low_conf)
        
        assert len(high_actions) == 0  # No actions needed for high confidence
        assert len(low_actions) >= 2  # Multiple actions needed for low confidence
        assert "Manual review required" in low_actions
        assert "User input required for dataset type" in low_actions


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
