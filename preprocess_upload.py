import pandas as pd
import numpy as np
import hashlib
import logging
from pathlib import Path
from datetime import datetime, timedelta
import sys
from database_models import UploadFileRepository
from storage_manager import storage_manager
from dataset_detector import dataset_detector

logger = logging.getLogger(__name__)

def read_any(path):
    ext = Path(path).suffix.lower()
    if ext in (".xls", ".xlsx"):
        return pd.read_excel(path, engine="openpyxl")
    return pd.read_csv(path, low_memory=False)

def find_column(df, keywords):
    for c in df.columns:
        name = str(c).strip().lower().replace(" ", "_")
        for key in keywords:
            if key in name:
                return c
    return None

def compute_file_hash(file_path: str) -> str:
    """Compute SHA-256 hash of file"""
    hash_sha256 = hashlib.sha256()
    try:
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_sha256.update(chunk)
        return hash_sha256.hexdigest()
    except Exception as e:
        logger.error(f"Failed to compute hash for {file_path}: {e}")
        raise


def check_duplicate_file(file_hash: str) -> tuple[bool, str]:
    """Check if file hash already exists and return existing job info"""
    try:
        upload_file = UploadFileRepository.get_upload_file(file_hash)
        if upload_file:
            # Get recent jobs for this file
            from database_models import JobRepository
            recent_jobs = UploadFileRepository.get_recent_jobs_for_file(file_hash, limit=1)
            if recent_jobs:
                job = recent_jobs[0]
                return True, f"File already processed (Job ID: {job.job_id}, Status: {job.status})"
            return True, "File already processed"
        return False, ""
    except Exception as e:
        logger.error(f"Failed to check duplicate file: {e}")
        return False, ""


def normalize_any_file(path, file_hash: str = None):
    """Normalize file with idempotency checks"""
    try:
        # Compute file hash if not provided
        if not file_hash:
            file_hash = compute_file_hash(path)
        
        # Check for duplicates
        is_duplicate, duplicate_info = check_duplicate_file(file_hash)
        if is_duplicate:
            logger.info(f"Duplicate file detected: {duplicate_info}")
            # Return existing normalized path if available
            upload_file = UploadFileRepository.get_upload_file(file_hash)
            if upload_file and upload_file.normalized_path:
                return upload_file.normalized_path, file_hash, True
        
        # Detect dataset type
        detection_result = dataset_detector.detect_dataset_type(path)
        logger.info(f"Dataset type detected: {detection_result.dataset_type} "
                   f"(confidence: {detection_result.confidence:.2f})")
        
        df = read_any(path)
        df = df.copy()
        df.columns = [str(c).strip() for c in df.columns]

        # Use detected columns if available, otherwise fall back to original logic
        if detection_result.confidence >= 0.7 and detection_result.detected_columns:
            # Use detected columns
            date_col = detection_result.detected_columns.get('date_columns')
            station_col = detection_result.detected_columns.get('station_columns')
            result_col = detection_result.detected_columns.get('result_columns')
        else:
            # Fall back to original detection logic
            date_col = find_column(df, ["date", "time", "timestamp", "datetime", "recorded"])
            station_col = find_column(df, ["station", "id", "branch", "location", "sensor"])
            result_col = find_column(df, ["value", "amount", "result", "reading", "score", "price", "metric"])

        # --- Create base DataFrame ---
        if date_col:
            try:
                df["Date_Time"] = pd.to_datetime(df[date_col], errors="coerce")
            except Exception:
                df["Date_Time"] = pd.to_datetime("today")
        else:
            df["Date_Time"] = [datetime.today() - timedelta(minutes=i) for i in range(len(df))]

        # --- Assign Station_ID ---
        if station_col:
            df["Station_ID"] = df[station_col].astype(str).fillna("CT")
        else:
            df["Station_ID"] = "CT"

        # --- Detect numeric columns for dynamic PCode mapping ---
        numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()

        # If no numeric columns, use any detected "Result" or create dummy
        if not numeric_cols and result_col:
            numeric_cols = [result_col]
        elif not numeric_cols:
            # If truly nothing numeric, create one dummy numeric column
            df["Random_Result"] = np.random.uniform(0, 100, size=len(df))
            numeric_cols = ["Random_Result"]

        # --- Melt into PCode / Result structure ---
        df_melted = df.melt(
            id_vars=["Station_ID", "Date_Time"],
            value_vars=numeric_cols,
            var_name="PCode",
            value_name="Result"
        )

        # Clean up
        df_melted["PCode"] = df_melted["PCode"].astype(str).fillna("X1")
        df_melted["Result"] = pd.to_numeric(df_melted["Result"], errors="coerce").fillna(0)

        # --- Save normalized CSV ---
        out_path = str(path) + ".normalized.csv"
        df_melted.to_csv(out_path, index=False)
        
        # Upload to Supabase Storage
        try:
            storage_path = f"uploads/{file_hash}.csv"
            with open(out_path, 'rb') as f:
                file_data = f.read()
            
            storage_manager.upload_file("uploads", storage_path, file_data, "text/csv")
            logger.info(f"Uploaded normalized file to storage: {storage_path}")
        except Exception as e:
            logger.error(f"Failed to upload to storage: {e}")
            # Continue with local file
        
        # Record in database
        try:
            UploadFileRepository.create_or_update_upload_file(
                file_hash, 
                Path(path).name, 
                out_path
            )
        except Exception as e:
            logger.error(f"Failed to record upload file: {e}")

        return out_path, file_hash, False
        
    except Exception as e:
        logger.error(f"File normalization failed for {path}: {e}")
        raise


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python preprocess_upload.py <path> [file_hash]")
        sys.exit(2)

    path = sys.argv[1]
    file_hash = sys.argv[2] if len(sys.argv) > 2 else None
    
    try:
        out_path, computed_hash, is_duplicate = normalize_any_file(path, file_hash)
        print(out_path)
        
        if is_duplicate:
            logger.info(f"Used existing normalized file for {path}")
        else:
            logger.info(f"Created new normalized file for {path}")
            
    except Exception as e:
        logger.error(f"Preprocessing failed for {path}: {e}")
        print(path)  # Return original path on error