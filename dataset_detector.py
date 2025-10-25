#!/usr/bin/env python3
"""
dataset_detector.py - Multi-strategy dataset type detection system
"""

import json
import logging
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple, Any
from pathlib import Path
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class DetectionResult:
    """Result of dataset type detection"""
    dataset_type: str
    confidence: float
    strategy: str
    details: Dict[str, Any]
    required_columns: List[str]
    detected_columns: Dict[str, str]


class DatasetDetector:
    """Multi-strategy dataset type detection system"""
    
    def __init__(self, rules_file: str = "dataset_detection_rules.json"):
        self.rules_file = rules_file
        self.rules = self._load_rules()
    
    def _load_rules(self) -> Dict[str, Any]:
        """Load detection rules from JSON file"""
        try:
            with open(self.rules_file, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Failed to load detection rules: {e}")
            # Return default rules
            return {
                "required_columns": {
                    "strict": ["Station_ID", "Date_Time", "PCode", "Result"],
                    "flexible": ["station", "date", "time", "pcode", "result", "value"]
                },
                "column_patterns": {
                    "date_columns": ["date", "time", "timestamp", "datetime"],
                    "station_columns": ["station", "id", "branch", "location"],
                    "result_columns": ["value", "amount", "result", "reading"],
                    "pcode_columns": ["pcode", "parameter", "param", "type"]
                },
                "confidence_thresholds": {
                    "high": 0.9,
                    "medium": 0.7,
                    "low": 0.5
                }
            }
    
    def detect_dataset_type(self, file_path: str) -> DetectionResult:
        """Detect dataset type using multiple strategies"""
        try:
            # Read the file
            df = self._read_file(file_path)
            
            # Apply detection strategies
            results = []
            
            # Strategy 1: Strict matching
            strict_result = self._strict_match_detection(df)
            if strict_result:
                results.append(strict_result)
            
            # Strategy 2: Pattern matching
            pattern_result = self._pattern_match_detection(df)
            if pattern_result:
                results.append(pattern_result)
            
            # Strategy 3: Data type analysis
            dtype_result = self._data_type_analysis(df)
            if dtype_result:
                results.append(dtype_result)
            
            # Strategy 4: Heuristic analysis
            heuristic_result = self._heuristic_analysis(df)
            if heuristic_result:
                results.append(heuristic_result)
            
            # Select best result
            if not results:
                return self._create_fallback_result(df)
            
            # Sort by confidence and return best
            best_result = max(results, key=lambda x: x.confidence)
            
            logger.info(f"Dataset type detected: {best_result.dataset_type} "
                       f"(confidence: {best_result.confidence:.2f}, strategy: {best_result.strategy})")
            
            return best_result
            
        except Exception as e:
            logger.error(f"Dataset detection failed for {file_path}: {e}")
            return self._create_error_result(str(e))
    
    def _read_file(self, file_path: str) -> pd.DataFrame:
        """Read file into DataFrame"""
        ext = Path(file_path).suffix.lower()
        
        if ext == '.csv':
            return pd.read_csv(file_path, low_memory=False)
        elif ext in ['.xlsx', '.xls']:
            return pd.read_excel(file_path, engine='openpyxl')
        else:
            raise ValueError(f"Unsupported file format: {ext}")
    
    def _strict_match_detection(self, df: pd.DataFrame) -> Optional[DetectionResult]:
        """Strict column name matching"""
        try:
            required_cols = self.rules["required_columns"]["strict"]
            df_columns = [col.strip().lower() for col in df.columns]
            
            matches = 0
            detected_columns = {}
            
            for req_col in required_cols:
                req_col_lower = req_col.lower()
                for df_col in df.columns:
                    if df_col.strip().lower() == req_col_lower:
                        matches += 1
                        detected_columns[req_col] = df_col
                        break
            
            if matches == len(required_cols):
                confidence = 1.0
            elif matches >= len(required_cols) * 0.75:  # 75% match
                confidence = 0.8
            else:
                confidence = 0.0
            
            if confidence > 0:
                return DetectionResult(
                    dataset_type="sensor_data",
                    confidence=confidence,
                    strategy="strict_match",
                    details={
                        "matched_columns": matches,
                        "total_required": len(required_cols),
                        "match_ratio": matches / len(required_cols)
                    },
                    required_columns=required_cols,
                    detected_columns=detected_columns
                )
            
            return None
            
        except Exception as e:
            logger.error(f"Strict match detection failed: {e}")
            return None
    
    def _pattern_match_detection(self, df: pd.DataFrame) -> Optional[DetectionResult]:
        """Pattern-based column matching"""
        try:
            patterns = self.rules["column_patterns"]
            df_columns = [col.strip().lower() for col in df.columns]
            
            detected_columns = {}
            confidence_scores = []
            
            for pattern_type, keywords in patterns.items():
                best_match = None
                best_score = 0
                
                for keyword in keywords:
                    for df_col in df.columns:
                        df_col_lower = df_col.strip().lower()
                        # Check if keyword is in column name
                        if keyword in df_col_lower:
                            # Calculate similarity score
                            score = len(keyword) / len(df_col_lower)
                            if score > best_score:
                                best_score = score
                                best_match = df_col
                
                if best_match:
                    detected_columns[pattern_type] = best_match
                    confidence_scores.append(best_score)
            
            if len(detected_columns) >= 3:  # Need at least 3 pattern matches
                avg_confidence = sum(confidence_scores) / len(confidence_scores)
                
                return DetectionResult(
                    dataset_type="sensor_data",
                    confidence=avg_confidence * 0.8,  # Scale down for pattern matching
                    strategy="pattern_match",
                    details={
                        "detected_patterns": len(detected_columns),
                        "confidence_scores": confidence_scores,
                        "avg_score": avg_confidence
                    },
                    required_columns=list(patterns.keys()),
                    detected_columns=detected_columns
                )
            
            return None
            
        except Exception as e:
            logger.error(f"Pattern match detection failed: {e}")
            return None
    
    def _data_type_analysis(self, df: pd.DataFrame) -> Optional[DetectionResult]:
        """Data type and content analysis"""
        try:
            # Analyze data types
            numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
            datetime_cols = []
            text_cols = df.select_dtypes(include=['object']).columns.tolist()
            
            # Try to identify datetime columns
            for col in text_cols:
                try:
                    pd.to_datetime(df[col].dropna().head(10), errors='raise')
                    datetime_cols.append(col)
                except:
                    pass
            
            # Calculate confidence based on data structure
            confidence = 0.0
            detected_columns = {}
            
            # Check for typical sensor data structure
            if len(numeric_cols) >= 1:
                confidence += 0.3
                detected_columns['result_columns'] = numeric_cols[0]
            
            if len(datetime_cols) >= 1:
                confidence += 0.3
                detected_columns['date_columns'] = datetime_cols[0]
            
            # Check for categorical columns (potential station/parameter codes)
            categorical_cols = []
            for col in text_cols:
                if col not in datetime_cols:
                    unique_ratio = df[col].nunique() / len(df)
                    if 0.01 < unique_ratio < 0.5:  # Reasonable for categorical
                        categorical_cols.append(col)
            
            if len(categorical_cols) >= 1:
                confidence += 0.2
                detected_columns['station_columns'] = categorical_cols[0]
            
            if len(categorical_cols) >= 2:
                confidence += 0.2
                detected_columns['pcode_columns'] = categorical_cols[1]
            
            if confidence >= 0.5:
                return DetectionResult(
                    dataset_type="sensor_data",
                    confidence=confidence,
                    strategy="data_type_analysis",
                    details={
                        "numeric_columns": len(numeric_cols),
                        "datetime_columns": len(datetime_cols),
                        "categorical_columns": len(categorical_cols),
                        "total_columns": len(df.columns)
                    },
                    required_columns=["numeric", "datetime", "categorical"],
                    detected_columns=detected_columns
                )
            
            return None
            
        except Exception as e:
            logger.error(f"Data type analysis failed: {e}")
            return None
    
    def _heuristic_analysis(self, df: pd.DataFrame) -> Optional[DetectionResult]:
        """Heuristic-based detection"""
        try:
            confidence = 0.0
            detected_columns = {}
            details = {}
            
            # Heuristic 1: Check for common sensor data patterns
            if len(df.columns) >= 4:
                confidence += 0.2
                details["sufficient_columns"] = True
            
            # Heuristic 2: Check for time-series like data
            if len(df) > 10:
                confidence += 0.2
                details["sufficient_rows"] = True
            
            # Heuristic 3: Look for numeric data
            numeric_cols = df.select_dtypes(include=[np.number]).columns
            if len(numeric_cols) > 0:
                confidence += 0.3
                detected_columns['result_columns'] = numeric_cols[0]
                details["has_numeric_data"] = True
            
            # Heuristic 4: Check for potential ID columns
            for col in df.columns:
                if any(keyword in col.lower() for keyword in ['id', 'station', 'sensor']):
                    confidence += 0.2
                    detected_columns['station_columns'] = col
                    details["has_id_column"] = True
                    break
            
            # Heuristic 5: Check for potential date columns
            for col in df.columns:
                if any(keyword in col.lower() for keyword in ['date', 'time', 'timestamp']):
                    confidence += 0.1
                    detected_columns['date_columns'] = col
                    details["has_date_column"] = True
                    break
            
            if confidence >= 0.4:
                return DetectionResult(
                    dataset_type="sensor_data",
                    confidence=confidence,
                    strategy="heuristic_analysis",
                    details=details,
                    required_columns=["numeric", "id", "date"],
                    detected_columns=detected_columns
                )
            
            return None
            
        except Exception as e:
            logger.error(f"Heuristic analysis failed: {e}")
            return None
    
    def _create_fallback_result(self, df: pd.DataFrame) -> DetectionResult:
        """Create fallback result when no strategy succeeds"""
        return DetectionResult(
            dataset_type="unknown",
            confidence=0.0,
            strategy="fallback",
            details={
                "columns": list(df.columns),
                "rows": len(df),
                "dtypes": df.dtypes.to_dict()
            },
            required_columns=[],
            detected_columns={}
        )
    
    def _create_error_result(self, error_msg: str) -> DetectionResult:
        """Create error result"""
        return DetectionResult(
            dataset_type="error",
            confidence=0.0,
            strategy="error",
            details={"error": error_msg},
            required_columns=[],
            detected_columns={}
        )
    
    def get_confidence_level(self, confidence: float) -> str:
        """Get confidence level string"""
        thresholds = self.rules.get("confidence_thresholds", {
            "high": 0.9,
            "medium": 0.7,
            "low": 0.5
        })
        
        if confidence >= thresholds["high"]:
            return "high"
        elif confidence >= thresholds["medium"]:
            return "medium"
        elif confidence >= thresholds["low"]:
            return "low"
        else:
            return "very_low"
    
    def suggest_dataset_type(self, detection_result: DetectionResult) -> str:
        """Suggest dataset type based on detection result"""
        if detection_result.confidence >= 0.7:
            return detection_result.dataset_type
        elif detection_result.confidence >= 0.4:
            return "likely_sensor_data"
        else:
            return "unknown"
    
    def get_required_actions(self, detection_result: DetectionResult) -> List[str]:
        """Get list of required actions based on detection result"""
        actions = []
        
        if detection_result.confidence < 0.7:
            actions.append("Manual review required")
        
        if detection_result.dataset_type == "unknown":
            actions.append("User input required for dataset type")
        
        if not detection_result.detected_columns:
            actions.append("Column mapping required")
        
        return actions


# Global detector instance
dataset_detector = DatasetDetector()
