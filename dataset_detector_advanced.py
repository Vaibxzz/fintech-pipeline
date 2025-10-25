#!/usr/bin/env python3
"""
dataset_detector_advanced.py - Phase 3: Multi-strategy dataset detection with confidence scoring
"""

import pandas as pd
import logging
import json
import os
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime

logger = logging.getLogger(__name__)


class DatasetDetector:
    """Advanced dataset detection with multiple strategies and confidence scoring"""
    
    def __init__(self):
        self.enabled = os.environ.get("ENABLE_DATASET_DETECTION", "true").lower() == "true"
        self.rules_file = Path("dataset_detection_rules.json")
        self.detection_rules = self._load_detection_rules()
        logger.info(f"Dataset detection {'enabled' if self.enabled else 'disabled'}")
    
    def is_enabled(self) -> bool:
        """Check if dataset detection is enabled"""
        return self.enabled
    
    def _load_detection_rules(self) -> Dict:
        """Load dataset detection rules from JSON file"""
        if not self.rules_file.exists():
            # Create default rules if file doesn't exist
            default_rules = {
                "strategies": {
                    "column_analysis": {
                        "weight": 0.4,
                        "description": "Analyze column names and data types"
                    },
                    "data_patterns": {
                        "weight": 0.3,
                        "description": "Look for specific data patterns"
                    },
                    "file_metadata": {
                        "weight": 0.2,
                        "description": "Analyze file name and structure"
                    },
                    "content_analysis": {
                        "weight": 0.1,
                        "description": "Analyze actual data content"
                    }
                },
                "dataset_types": {
                    "raw_data": {
                        "required_columns": ["Station_ID", "Date_Time", "PCode", "Result"],
                        "optional_columns": ["Quality_Flag", "Unit", "Method"],
                        "data_patterns": {
                            "date_format": "YYYY-MM-DD HH:MM:SS",
                            "numeric_result": True,
                            "station_id_format": "alphanumeric"
                        },
                        "confidence_threshold": 0.7
                    },
                    "ct_analysis": {
                        "required_columns": ["Station", "Dates"],
                        "optional_columns": ["Data 1", "Data 2", "Data 3"],
                        "data_patterns": {
                            "station_value": "CT",
                            "date_format": "YYYY-MM-DD",
                            "multiple_data_columns": True
                        },
                        "confidence_threshold": 0.8
                    },
                    "tus_analysis": {
                        "required_columns": ["Station", "Dates"],
                        "optional_columns": ["Data 1", "Data 2", "Data 3"],
                        "data_patterns": {
                            "station_value": "TUS",
                            "date_format": "YYYY-MM-DD",
                            "multiple_data_columns": True
                        },
                        "confidence_threshold": 0.8
                    }
                }
            }
            
            # Save default rules
            with open(self.rules_file, 'w') as f:
                json.dump(default_rules, f, indent=2)
            
            logger.info(f"Created default detection rules at {self.rules_file}")
            return default_rules
        
        try:
            with open(self.rules_file, 'r') as f:
                rules = json.load(f)
            logger.info(f"Loaded detection rules from {self.rules_file}")
            return rules
        except Exception as e:
            logger.error(f"Failed to load detection rules: {e}")
            return {}
    
    def detect_dataset_type(self, file_path: str) -> Dict[str, Any]:
        """Detect dataset type using multiple strategies"""
        if not self.enabled:
            return {
                "detected_type": "unknown",
                "confidence": 0.0,
                "strategy_scores": {},
                "reasoning": "Dataset detection disabled",
                "recommendations": ["Enable dataset detection", "Manual selection required"]
            }
        
        try:
            # Load the file
            df = self._load_file(file_path)
            if df is None or df.empty:
                return self._create_result("unknown", 0.0, {}, "Empty or invalid file")
            
            # Run detection strategies
            strategy_scores = {}
            
            # Strategy 1: Column Analysis
            column_score = self._analyze_columns(df)
            strategy_scores["column_analysis"] = column_score
            
            # Strategy 2: Data Patterns
            pattern_score = self._analyze_data_patterns(df)
            strategy_scores["data_patterns"] = pattern_score
            
            # Strategy 3: File Metadata
            metadata_score = self._analyze_file_metadata(file_path)
            strategy_scores["file_metadata"] = metadata_score
            
            # Strategy 4: Content Analysis
            content_score = self._analyze_content(df)
            strategy_scores["content_analysis"] = content_score
            
            # Calculate weighted confidence
            total_confidence = 0.0
            total_weight = 0.0
            
            for strategy, score in strategy_scores.items():
                weight = self.detection_rules.get("strategies", {}).get(strategy, {}).get("weight", 0.25)
                total_confidence += score["confidence"] * weight
                total_weight += weight
            
            final_confidence = total_confidence / total_weight if total_weight > 0 else 0.0
            
            # Determine best match
            best_type = self._determine_best_match(strategy_scores, final_confidence)
            
            # Generate reasoning and recommendations
            reasoning = self._generate_reasoning(strategy_scores, best_type, final_confidence)
            recommendations = self._generate_recommendations(best_type, final_confidence)
            
            return {
                "detected_type": best_type,
                "confidence": final_confidence,
                "strategy_scores": strategy_scores,
                "reasoning": reasoning,
                "recommendations": recommendations,
                "file_info": {
                    "path": file_path,
                    "rows": len(df),
                    "columns": len(df.columns),
                    "column_names": list(df.columns)
                }
            }
            
        except Exception as e:
            logger.error(f"Dataset detection failed for {file_path}: {e}")
            return self._create_result("error", 0.0, {}, f"Detection failed: {str(e)}")
    
    def _load_file(self, file_path: str) -> Optional[pd.DataFrame]:
        """Load file into DataFrame"""
        try:
            file_path = Path(file_path)
            
            if file_path.suffix.lower() == '.csv':
                return pd.read_csv(file_path)
            elif file_path.suffix.lower() in ['.xlsx', '.xls']:
                return pd.read_excel(file_path)
            else:
                logger.warning(f"Unsupported file type: {file_path.suffix}")
                return None
                
        except Exception as e:
            logger.error(f"Failed to load file {file_path}: {e}")
            return None
    
    def _analyze_columns(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Analyze column names and types"""
        scores = {}
        total_score = 0.0
        
        for dataset_type, rules in self.detection_rules.get("dataset_types", {}).items():
            score = 0.0
            required_cols = rules.get("required_columns", [])
            optional_cols = rules.get("optional_columns", [])
            
            # Check required columns
            required_found = 0
            for col in required_cols:
                if col in df.columns:
                    required_found += 1
            
            if required_cols:
                required_score = required_found / len(required_cols)
            else:
                required_score = 0.0
            
            # Check optional columns
            optional_found = 0
            for col in optional_cols:
                if col in df.columns:
                    optional_found += 1
            
            if optional_cols:
                optional_score = optional_found / len(optional_cols) * 0.5  # Optional columns worth less
            else:
                optional_score = 0.0
            
            score = required_score + optional_score
            scores[dataset_type] = score
            total_score = max(total_score, score)
        
        return {
            "confidence": total_score,
            "scores": scores,
            "reasoning": f"Column analysis: {len(df.columns)} columns analyzed"
        }
    
    def _analyze_data_patterns(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Analyze data patterns and values"""
        scores = {}
        total_score = 0.0
        
        for dataset_type, rules in self.detection_rules.get("dataset_types", {}).items():
            score = 0.0
            patterns = rules.get("data_patterns", {})
            
            # Check station value pattern
            if "station_value" in patterns:
                station_col = None
                for col in ["Station", "Station_ID"]:
                    if col in df.columns:
                        station_col = col
                        break
                
                if station_col:
                    unique_stations = df[station_col].unique()
                    expected_station = patterns["station_value"]
                    if expected_station in unique_stations:
                        score += 0.4
            
            # Check date format pattern
            if "date_format" in patterns:
                date_col = None
                for col in ["Dates", "Date_Time", "Date"]:
                    if col in df.columns:
                        date_col = col
                        break
                
                if date_col:
                    try:
                        # Try to parse dates
                        pd.to_datetime(df[date_col].head(10))
                        score += 0.3
                    except:
                        pass
            
            # Check numeric result pattern
            if patterns.get("numeric_result"):
                result_col = None
                for col in ["Result", "Value", "Data 1"]:
                    if col in df.columns:
                        result_col = col
                        break
                
                if result_col:
                    try:
                        pd.to_numeric(df[result_col].head(10))
                        score += 0.3
                    except:
                        pass
            
            scores[dataset_type] = score
            total_score = max(total_score, score)
        
        return {
            "confidence": total_score,
            "scores": scores,
            "reasoning": f"Data patterns: {len(df)} rows analyzed"
        }
    
    def _analyze_file_metadata(self, file_path: str) -> Dict[str, Any]:
        """Analyze file name and metadata"""
        file_path = Path(file_path)
        filename = file_path.name.lower()
        
        scores = {}
        total_score = 0.0
        
        # Check filename patterns
        if "raw" in filename:
            scores["raw_data"] = 0.8
            total_score = max(total_score, 0.8)
        elif "ct" in filename:
            scores["ct_analysis"] = 0.8
            total_score = max(total_score, 0.8)
        elif "tus" in filename:
            scores["tus_analysis"] = 0.8
            total_score = max(total_score, 0.8)
        else:
            # Default low scores
            for dataset_type in self.detection_rules.get("dataset_types", {}):
                scores[dataset_type] = 0.1
            total_score = 0.1
        
        return {
            "confidence": total_score,
            "scores": scores,
            "reasoning": f"File metadata: {filename} analyzed"
        }
    
    def _analyze_content(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Analyze actual data content"""
        scores = {}
        total_score = 0.0
        
        # Basic content analysis
        for dataset_type in self.detection_rules.get("dataset_types", {}):
            score = 0.0
            
            # Check for reasonable data size
            if len(df) > 10:
                score += 0.2
            
            # Check for reasonable number of columns
            if 3 <= len(df.columns) <= 30:
                score += 0.2
            
            # Check for non-null data
            non_null_ratio = df.count().sum() / (len(df) * len(df.columns))
            if non_null_ratio > 0.5:
                score += 0.3
            
            # Check for numeric data
            numeric_cols = df.select_dtypes(include=['number']).columns
            if len(numeric_cols) > 0:
                score += 0.3
            
            scores[dataset_type] = score
            total_score = max(total_score, score)
        
        return {
            "confidence": total_score,
            "scores": scores,
            "reasoning": f"Content analysis: {len(df)} rows, {len(df.columns)} columns"
        }
    
    def _determine_best_match(self, strategy_scores: Dict, final_confidence: float) -> str:
        """Determine the best matching dataset type"""
        if final_confidence < 0.3:
            return "unknown"
        
        # Find the dataset type with highest average score across strategies
        type_scores = {}
        
        for dataset_type in self.detection_rules.get("dataset_types", {}):
            total_score = 0.0
            count = 0
            
            for strategy, result in strategy_scores.items():
                if dataset_type in result.get("scores", {}):
                    total_score += result["scores"][dataset_type]
                    count += 1
            
            if count > 0:
                type_scores[dataset_type] = total_score / count
        
        if type_scores:
            best_type = max(type_scores, key=type_scores.get)
            threshold = self.detection_rules.get("dataset_types", {}).get(best_type, {}).get("confidence_threshold", 0.5)
            
            if type_scores[best_type] >= threshold:
                return best_type
        
        return "unknown"
    
    def _generate_reasoning(self, strategy_scores: Dict, detected_type: str, confidence: float) -> str:
        """Generate human-readable reasoning"""
        reasoning_parts = []
        
        if detected_type == "unknown":
            reasoning_parts.append("Dataset type could not be determined with sufficient confidence.")
        else:
            reasoning_parts.append(f"Detected as '{detected_type}' with {confidence:.1%} confidence.")
        
        # Add strategy-specific reasoning
        for strategy, result in strategy_scores.items():
            if result.get("confidence", 0) > 0.5:
                reasoning_parts.append(f"{strategy.replace('_', ' ').title()}: {result.get('reasoning', '')}")
        
        return " ".join(reasoning_parts)
    
    def _generate_recommendations(self, detected_type: str, confidence: float) -> List[str]:
        """Generate recommendations based on detection results"""
        recommendations = []
        
        if confidence >= 0.8:
            recommendations.append(f"High confidence detection: Use '{detected_type}'")
        elif confidence >= 0.5:
            recommendations.append(f"Medium confidence: Consider '{detected_type}' or manual verification")
        else:
            recommendations.append("Low confidence: Manual dataset type selection recommended")
        
        if detected_type == "unknown":
            recommendations.extend([
                "Check file format and column names",
                "Verify data structure matches expected format",
                "Consider preprocessing the file"
            ])
        
        return recommendations
    
    def _create_result(self, detected_type: str, confidence: float, strategy_scores: Dict, reasoning: str) -> Dict[str, Any]:
        """Create a standardized result dictionary"""
        return {
            "detected_type": detected_type,
            "confidence": confidence,
            "strategy_scores": strategy_scores,
            "reasoning": reasoning,
            "recommendations": ["Manual verification recommended"],
            "file_info": {}
        }


# Global instance
dataset_detector = DatasetDetector()
