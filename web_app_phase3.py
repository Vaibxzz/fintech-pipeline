#!/usr/bin/env python3
"""
web_app_phase3.py - Phase 3: Advanced features with file hashing, dataset detection, and enhanced job management
"""

import os
import logging
import subprocess
import uuid
import threading
import time
import hashlib
from queue import Queue
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Optional
from flask import (
    Flask,
    request,
    render_template_string,
    send_from_directory,
    redirect,
    url_for,
    flash,
    abort,
    jsonify,
)
from supabase_storage_client import supabase_storage
from supabase_rest_client import supabase_rest
from file_hasher import file_hasher
from dataset_detector_advanced import dataset_detector
from job_manager_advanced import advanced_job_manager

# ----------------------
# App & logging
# ----------------------
app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "change-me-123")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger("fintech_web_app_phase3")
logger.info("Phase 3 web app starting with advanced features")

# ----------------------
# HTML template
# ----------------------
INDEX_HTML = """
<!DOCTYPE html>
<html>
<head>
    <title>Fintech Data Pipeline - Phase 3</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 40px; }
        .container { max-width: 1400px; margin: 0 auto; }
        .upload-section { border: 2px dashed #ccc; padding: 20px; margin: 20px 0; text-align: center; }
        .job-list { margin: 20px 0; }
        .job-item { border: 1px solid #ddd; padding: 15px; margin: 10px 0; border-radius: 5px; }
        .job-status { font-weight: bold; }
        .status-queued { color: #ffa500; }
        .status-running { color: #0066cc; }
        .status-retrying { color: #ff6600; }
        .status-done { color: #00aa00; }
        .status-failed { color: #cc0000; }
        .status-cancelled { color: #666; }
        .download-btn { background: #0066cc; color: white; padding: 5px 10px; text-decoration: none; border-radius: 3px; }
        .download-btn:hover { background: #0052a3; }
        .stats { display: flex; gap: 20px; margin: 20px 0; }
        .stat-box { background: #f8f9fa; padding: 15px; border-radius: 5px; text-align: center; }
        .storage-status { background: #e7f3ff; border: 1px solid #b3d9ff; padding: 10px; margin: 10px 0; border-radius: 5px; }
        .database-status { background: #f0f8ff; border: 1px solid #b3d9ff; padding: 10px; margin: 10px 0; border-radius: 5px; }
        .duplicate-warning { background: #fff3cd; border: 1px solid #ffeaa7; padding: 10px; margin: 10px 0; border-radius: 5px; }
        .detection-results { background: #d1ecf1; border: 1px solid #bee5eb; padding: 10px; margin: 10px 0; border-radius: 5px; }
        .confidence-high { color: #28a745; font-weight: bold; }
        .confidence-medium { color: #ffc107; font-weight: bold; }
        .confidence-low { color: #dc3545; font-weight: bold; }
        .feature-indicator { display: inline-block; margin: 2px; padding: 2px 6px; border-radius: 3px; font-size: 11px; }
        .feature-enabled { background: #d4edda; color: #155724; }
        .feature-disabled { background: #f8d7da; color: #721c24; }
        .retry-info { background: #fff3cd; border: 1px solid #ffeaa7; padding: 5px; margin: 5px 0; border-radius: 3px; font-size: 12px; }
    </style>
</head>
<body>
    <div class="container">
        <h1>Fintech Data Pipeline - Phase 3</h1>
        
        <!-- System Status -->
        <div class="storage-status">
            <h4>Storage Status</h4>
            <p><strong>Supabase Storage:</strong> 
                <span style="color: {{ 'green' if storage.enabled else 'orange' }}">
                    {{ 'Enabled' if storage.enabled else 'Disabled' }}
                </span>
                {% if storage.enabled %}
                <span style="color: {{ 'green' if storage.connected else 'red' }}">
                    ({{ 'Connected' if storage.connected else 'Disconnected' }})
                </span>
                {% endif %}
            </p>
            <p><strong>Local Storage:</strong> <span style="color: green;">Always Available</span></p>
        </div>
        
        <div class="database-status">
            <h4>Database Status</h4>
            <p><strong>PostgreSQL Tracking:</strong> 
                <span style="color: {{ 'green' if database.enabled else 'orange' }}">
                    {{ 'Enabled' if database.enabled else 'Disabled' }}
                </span>
                {% if database.enabled %}
                <span style="color: {{ 'green' if database.connected else 'red' }}">
                    ({{ 'Connected' if database.connected else 'Disconnected' }})
                </span>
                {% endif %}
            </p>
            <p><strong>File-based Fallback:</strong> <span style="color: green;">Always Available</span></p>
        </div>
        
        <!-- Feature Status -->
        <div style="background: #f8f9fa; border: 1px solid #dee2e6; padding: 15px; margin: 20px 0; border-radius: 5px;">
            <h4>Phase 3 Features</h4>
            <div>
                <span class="feature-indicator {{ 'feature-enabled' if features.duplicate_detection else 'feature-disabled' }}">
                    🔍 Duplicate Detection
                </span>
                <span class="feature-indicator {{ 'feature-enabled' if features.dataset_detection else 'feature-disabled' }}">
                    🎯 Dataset Detection
                </span>
                <span class="feature-indicator {{ 'feature-enabled' if features.advanced_jobs else 'feature-disabled' }}">
                    ⚡ Advanced Jobs
                </span>
                <span class="feature-indicator {{ 'feature-enabled' if features.retry_logic else 'feature-disabled' }}">
                    🔄 Retry Logic
                </span>
            </div>
        </div>
        
        <!-- File Upload -->
        <div class="upload-section">
            <h3>Upload Data File</h3>
            <form method="post" action="/upload" enctype="multipart/form-data">
                <input type="file" name="file" accept=".csv,.xlsx,.xls" required>
                <br><br>
                <input type="submit" value="Upload and Process" style="padding: 10px 20px; font-size: 16px;">
            </form>
            <p>Supported formats: CSV, Excel (.xlsx, .xls)</p>
        </div>

        <!-- Flash Messages -->
        {% with messages = get_flashed_messages() %}
            {% if messages %}
                {% for message in messages %}
                    <div style="background: #d4edda; border: 1px solid #c3e6cb; padding: 10px; margin: 10px 0; border-radius: 5px;">
                        {{ message }}
                    </div>
                {% endfor %}
            {% endif %}
        {% endwith %}

        <!-- Duplicate Warning -->
        {% if duplicate_info %}
        <div class="duplicate-warning">
            <h4>⚠️ Duplicate File Detected</h4>
            <pre style="white-space: pre-wrap; font-size: 12px;">{{ duplicate_info }}</pre>
            <form method="post" action="/reprocess" style="display: inline;">
                <input type="hidden" name="file_hash" value="{{ file_hash }}">
                <input type="submit" value="Reprocess Anyway" style="background: #dc3545; color: white; padding: 5px 10px; border: none; border-radius: 3px;">
            </form>
        </div>
        {% endif %}

        <!-- Dataset Detection Results -->
        {% if detection_results %}
        <div class="detection-results">
            <h4>🎯 Dataset Detection Results</h4>
            <p><strong>Detected Type:</strong> 
                <span class="confidence-{{ detection_results.confidence_level }}">
                    {{ detection_results.detected_type.upper() }}
                </span>
                ({{ detection_results.confidence }}% confidence)
            </p>
            <p><strong>Reasoning:</strong> {{ detection_results.reasoning }}</p>
            {% if detection_results.recommendations %}
            <p><strong>Recommendations:</strong></p>
            <ul>
                {% for rec in detection_results.recommendations %}
                <li>{{ rec }}</li>
                {% endfor %}
            </ul>
            {% endif %}
        </div>
        {% endif %}

        <!-- Recent Jobs -->
        <div class="job-list">
            <h3>Recent Jobs</h3>
            {% for job in recent_jobs %}
            <div class="job-item">
                <div>
                    <strong>Job ID:</strong> {{ job.job_id }}<br>
                    <strong>File:</strong> {{ job.original_filename }}<br>
                    <strong>Status:</strong> 
                    <span class="job-status status-{{ job.status }}">{{ job.status.upper() }}</span><br>
                    <strong>Uploaded:</strong> {{ job.uploaded_at }}<br>
                    {% if job.started_at %}
                    <strong>Started:</strong> {{ job.started_at }}<br>
                    {% endif %}
                    {% if job.finished_at %}
                    <strong>Finished:</strong> {{ job.finished_at }}<br>
                    {% endif %}
                    {% if job.error_msg %}
                    <strong>Error:</strong> {{ job.error_msg }}<br>
                    {% endif %}
                    {% if job.dataset_type %}
                    <strong>Dataset Type:</strong> {{ job.dataset_type }}<br>
                    {% endif %}
                    {% if job.retry_count and job.retry_count > 0 %}
                    <div class="retry-info">
                        <strong>Retry Count:</strong> {{ job.retry_count }} / 3
                    </div>
                    {% endif %}
                </div>
                
                {% if job.outputs %}
                <div style="margin-top: 10px;">
                    <h4>Outputs:</h4>
                    {% for output in job.outputs %}
                    <div style="margin: 5px 0;">
                        <strong>{{ output.file_type }}:</strong> 
                        <a href="/download/{{ output.output_id }}" class="download-btn">Download</a>
                        {% if output.file_type == 'dashboard' %}
                        <a href="/view/{{ output.output_id }}" class="download-btn" style="background: #28a745;">View</a>
                        {% endif %}
                        {% if output.cloud_available %}
                        <span style="color: green; font-size: 12px;">☁️ Cloud</span>
                        {% endif %}
                        {% if output.database_tracked %}
                        <span style="color: blue; font-size: 12px;">🗄️ DB</span>
                        {% endif %}
                    </div>
                    {% endfor %}
                </div>
                {% endif %}
                
                {% if job.status in ['running', 'retrying'] %}
                <div style="margin-top: 10px;">
                    <form method="post" action="/cancel_job" style="display: inline;">
                        <input type="hidden" name="job_id" value="{{ job.job_id }}">
                        <input type="submit" value="Cancel Job" style="background: #dc3545; color: white; padding: 3px 8px; border: none; border-radius: 3px; font-size: 12px;">
                    </form>
                </div>
                {% endif %}
            </div>
            {% endfor %}
        </div>

        <!-- Health Check -->
        <div style="margin-top: 40px; padding: 20px; background: #f8f9fa; border-radius: 5px;">
            <h4>System Health</h4>
            <p><strong>Local Storage:</strong> <span style="color: green;">Connected</span></p>
            <p><strong>Supabase Storage:</strong> 
                <span style="color: {{ 'green' if storage.connected else 'red' }}">
                    {{ 'Connected' if storage.connected else 'Disconnected' }}
                </span>
            </p>
            <p><strong>Database:</strong> 
                <span style="color: {{ 'green' if database.connected else 'red' }}">
                    {{ 'Connected' if database.connected else 'Disconnected' }}
                </span>
            </p>
            <p><strong>Job Queue:</strong> {{ queue_status.queued_jobs }} queued, {{ queue_status.running_jobs }} running</p>
            <p><strong>Last Check:</strong> {{ health.timestamp }}</p>
        </div>
    </div>

    <script>
        // Auto-refresh every 30 seconds
        setTimeout(function() {
            location.reload();
        }, 30000);
    </script>
</body>
</html>
"""

# ----------------------
# Routes & helpers
# ----------------------
@app.route("/health", methods=["GET"])
def health():
    """Health check endpoint"""
    storage_health = supabase_storage.health_check()
    database_health = supabase_rest.health_check()
    queue_status = advanced_job_manager.get_queue_status()
    
    health_status = {
        "local_storage": True,
        "supabase_storage": storage_health,
        "database": database_health,
        "queue": queue_status,
        "timestamp": datetime.utcnow().isoformat()
    }
    return jsonify(health_status), 200


@app.route("/", methods=["GET"])
def index():
    """Main page"""
    try:
        # Get recent jobs
        recent_jobs = _get_recent_jobs()
        
        # Get system health
        storage_health = supabase_storage.health_check()
        database_health = supabase_rest.health_check()
        queue_status = advanced_job_manager.get_queue_status()
        health_status = {
            "timestamp": datetime.utcnow().isoformat()
        }
        
        # Get feature status
        features = {
            "duplicate_detection": file_hasher.is_enabled(),
            "dataset_detection": dataset_detector.is_enabled(),
            "advanced_jobs": advanced_job_manager.is_enabled(),
            "retry_logic": advanced_job_manager.is_enabled()
        }
        
        return render_template_string(
            INDEX_HTML, 
            recent_jobs=recent_jobs,
            health=health_status,
            storage=storage_health,
            database=database_health,
            queue_status=queue_status,
            features=features,
            duplicate_info=None,
            file_hash=None,
            detection_results=None
        )
        
    except Exception as e:
        logger.error(f"Index page error: {e}")
        return f"Error loading page: {e}", 500


def _get_recent_jobs() -> List[Dict]:
    """Get recent jobs with enhanced information"""
    recent_jobs = []
    
    # Try advanced job manager first
    if advanced_job_manager.is_enabled():
        try:
            jobs = advanced_job_manager.get_recent_jobs(15)
            for job in jobs:
                # Get outputs from database
                outputs = []
                if supabase_rest.is_enabled():
                    try:
                        db_outputs = supabase_rest.get_outputs_by_job(job["job_id"])
                        for output in db_outputs:
                            outputs.append({
                                "output_id": output["output_id"],
                                "file_type": output["file_type"],
                                "cloud_available": _check_cloud_file(output["storage_path"]),
                                "database_tracked": True
                            })
                    except Exception as e:
                        logger.error(f"Failed to get outputs for job {job['job_id']}: {e}")
                
                recent_jobs.append({
                    'job_id': job['job_id'],
                    'status': job['status'],
                    'original_filename': job['original_filename'],
                    'uploaded_at': job['uploaded_at'][:19] if job['uploaded_at'] else "Unknown",
                    'started_at': job['started_at'][:19] if job['started_at'] else None,
                    'finished_at': job['finished_at'][:19] if job['finished_at'] else None,
                    'error_msg': job['error_msg'],
                    'dataset_type': job['dataset_type'],
                    'retry_count': job.get('retry_count', 0),
                    'outputs': outputs
                })
            
            return recent_jobs
            
        except Exception as e:
            logger.error(f"Failed to get jobs from advanced manager: {e}")
    
    # Fall back to database or filesystem
    return _get_recent_jobs_fallback()


def _get_recent_jobs_fallback() -> List[Dict]:
    """Fallback method to get recent jobs"""
    recent_jobs = []
    
    # Try database first
    if supabase_rest.is_enabled():
        try:
            db_jobs = supabase_rest.get_jobs_by_status("done", limit=10)
            db_jobs.extend(supabase_rest.get_jobs_by_status("running", limit=5))
            db_jobs.extend(supabase_rest.get_jobs_by_status("failed", limit=5))
            
            for job in db_jobs:
                outputs = []
                try:
                    db_outputs = supabase_rest.get_outputs_by_job(job["job_id"])
                    for output in db_outputs:
                        outputs.append({
                            "output_id": output["output_id"],
                            "file_type": output["file_type"],
                            "cloud_available": _check_cloud_file(output["storage_path"]),
                            "database_tracked": True
                        })
                except Exception as e:
                    logger.error(f"Failed to get outputs for job {job['job_id']}: {e}")
                
                recent_jobs.append({
                    'job_id': job["job_id"],
                    'status': job["status"],
                    'original_filename': job["original_filename"],
                    'uploaded_at': job["uploaded_at"][:19] if job["uploaded_at"] else "Unknown",
                    'started_at': job["started_at"][:19] if job["started_at"] else None,
                    'finished_at': job["finished_at"][:19] if job["finished_at"] else None,
                    'error_msg': job["error_msg"],
                    'dataset_type': job["dataset_type"],
                    'retry_count': 0,
                    'outputs': outputs
                })
            
            recent_jobs.sort(key=lambda x: x['uploaded_at'], reverse=True)
            return recent_jobs[:15]
            
        except Exception as e:
            logger.error(f"Failed to get jobs from database: {e}")
    
    # Fall back to filesystem
    return _get_recent_jobs_from_filesystem()


def _get_recent_jobs_from_filesystem() -> List[Dict]:
    """Get recent jobs from filesystem (fallback)"""
    recent_jobs = []
    
    # Check for existing output directories
    output_dir = Path("outputs")
    if output_dir.exists():
        for job_dir in output_dir.iterdir():
            if job_dir.is_dir():
                job_id = job_dir.name
                # Check if processing is complete
                dashboard_file = job_dir / "dashboard.html"
                ct_file = job_dir / "CT_Analysis_Output.csv"
                tus_file = job_dir / "TUS_Analysis_Output.csv"
                
                if dashboard_file.exists() and ct_file.exists() and tus_file.exists():
                    status = "done"
                    outputs = [
                        {
                            "output_id": f"{job_id}_ct", 
                            "file_type": "CT",
                            "cloud_available": _check_cloud_file(f"outputs/{job_id}/CT_Analysis_Output.csv"),
                            "database_tracked": False
                        },
                        {
                            "output_id": f"{job_id}_tus", 
                            "file_type": "TUS",
                            "cloud_available": _check_cloud_file(f"outputs/{job_id}/TUS_Analysis_Output.csv"),
                            "database_tracked": False
                        },
                        {
                            "output_id": f"{job_id}_dashboard", 
                            "file_type": "dashboard",
                            "cloud_available": _check_cloud_file(f"outputs/{job_id}/dashboard.html"),
                            "database_tracked": False
                        }
                    ]
                else:
                    status = "running"
                    outputs = []
                
                recent_jobs.append({
                    'job_id': job_id,
                    'status': status,
                    'original_filename': 'processed_file.csv',
                    'uploaded_at': datetime.fromtimestamp(job_dir.stat().st_mtime).strftime('%Y-%m-%d %H:%M:%S'),
                    'started_at': None,
                    'finished_at': None,
                    'error_msg': None,
                    'dataset_type': None,
                    'retry_count': 0,
                    'outputs': outputs
                })
    
    # Sort by upload time (most recent first)
    recent_jobs.sort(key=lambda x: x['uploaded_at'], reverse=True)
    return recent_jobs[:10]


def _check_cloud_file(file_path: str) -> bool:
    """Check if file exists in cloud storage"""
    if not supabase_storage.is_enabled():
        return False
    
    try:
        # Extract bucket and path
        if file_path.startswith("outputs/"):
            bucket = "outputs"
            path = file_path[8:]  # Remove "outputs/" prefix
        else:
            bucket = "uploads"
            path = file_path[8:]  # Remove "uploads/" prefix
        
        files = supabase_storage.list_files(bucket, os.path.dirname(path))
        filename = os.path.basename(path)
        
        for file_info in files:
            if file_info.get('name') == filename:
                return True
        return False
    except Exception:
        return False


def allowed_file(filename):
    """Check if file extension is allowed"""
    allowed_extensions = {".csv", ".xlsx", ".xls"}
    return Path(filename).suffix.lower() in allowed_extensions


@app.route("/upload", methods=["POST"])
def upload():
    """Handle file upload with Phase 3 features"""
    try:
        if "file" not in request.files:
            flash("No file part")
            return redirect(url_for("index"))

        file = request.files["file"]
        if file.filename == "":
            flash("No selected file")
            return redirect(url_for("index"))

        if not allowed_file(file.filename):
            flash("Unsupported file type. Allowed: CSV, Excel (.xlsx, .xls)")
            return redirect(url_for("index"))

        # Save uploaded file temporarily
        fname = Path(file.filename).name
        uid = uuid.uuid4().hex[:8]
        saved_name = f"{uid}_{fname}"
        saved_path = os.path.join("uploads", saved_name)
        
        # Create uploads directory if it doesn't exist
        os.makedirs("uploads", exist_ok=True)
        file.save(saved_path)
        
        logger.info(f"File saved to {saved_path}")
        
        # Phase 3: File hashing and duplicate detection
        file_hash = None
        duplicate_info = None
        detection_results = None
        
        if file_hasher.is_enabled():
            try:
                file_hash = file_hasher.compute_file_hash(saved_path)
                is_duplicate, upload_record = file_hasher.check_duplicate_file(file_hash)
                
                if is_duplicate:
                    duplicate_info = file_hasher.generate_duplicate_report(file_hash)
                    # Show duplicate warning page
                    return _show_duplicate_warning_page(duplicate_info, file_hash)
                
            except Exception as e:
                logger.error(f"File hashing failed: {e}")
        
        # Phase 3: Dataset detection
        detected_dataset_type = None
        if dataset_detector.is_enabled():
            try:
                detection_result = dataset_detector.detect_dataset_type(saved_path)
                detected_dataset_type = detection_result.get("detected_type")
                
                # Show detection results if confidence is low
                if detection_result.get("confidence", 0) < 0.7:
                    detection_results = {
                        "detected_type": detected_dataset_type,
                        "confidence": int(detection_result.get("confidence", 0) * 100),
                        "confidence_level": "high" if detection_result.get("confidence", 0) >= 0.8 else "medium" if detection_result.get("confidence", 0) >= 0.5 else "low",
                        "reasoning": detection_result.get("reasoning", ""),
                        "recommendations": detection_result.get("recommendations", [])
                    }
                    
                    # Show detection results page
                    return _show_detection_results_page(detection_results, file_hash)
                
            except Exception as e:
                logger.error(f"Dataset detection failed: {e}")
        
        # Upload to Supabase Storage if enabled
        cloud_uploaded = False
        if supabase_storage.is_enabled():
            try:
                with open(saved_path, 'rb') as f:
                    file_data = f.read()
                
                storage_path = f"uploads/{saved_name}"
                cloud_uploaded = supabase_storage.upload_file("uploads", storage_path, file_data)
                if cloud_uploaded:
                    logger.info(f"File uploaded to cloud storage: {storage_path}")
            except Exception as e:
                logger.error(f"Cloud upload failed: {e}")
        
        # Phase 3: Create job with advanced job manager
        if advanced_job_manager.is_enabled():
            job_id = advanced_job_manager.create_job(
                file_path=saved_path,
                file_hash=file_hash or "unknown",
                original_filename=fname,
                dataset_type=detected_dataset_type,
                callback=_process_file_callback
            )
        else:
            # Fallback to simple job creation
            job_id = uuid.uuid4().hex[:8]
            if supabase_rest.is_enabled():
                try:
                    job = supabase_rest.create_job(file_hash or "unknown", fname, detected_dataset_type)
                    if job:
                        job_id = job["job_id"]
                except Exception as e:
                    logger.error(f"Database job creation failed: {e}")
        
        # Record file upload
        if file_hasher.is_enabled() and file_hash:
            file_hasher.record_file_upload(file_hash, fname)
        
        # Start processing
        if not advanced_job_manager.is_enabled():
            # Fallback to simple processing
            thread = threading.Thread(target=_process_file_simple, args=(job_id, saved_path), daemon=True)
            thread.start()
        
        cloud_status = " (cloud storage enabled)" if cloud_uploaded else ""
        db_status = " (database tracking enabled)" if supabase_rest.is_enabled() else ""
        advanced_status = " (advanced features enabled)" if advanced_job_manager.is_enabled() else ""
        
        flash(f"File uploaded successfully. Job {job_id} is processing in the background.{cloud_status}{db_status}{advanced_status}")
        return redirect(url_for("index"))
        
    except Exception as e:
        logger.error(f"Upload failed: {e}")
        flash(f"Upload failed: {e}")
        return redirect(url_for("index"))


def _process_file_callback(job_context):
    """Callback function for advanced job processing"""
    try:
        # Create output directory
        output_dir = f"outputs/{job_context.job_id}"
        os.makedirs(output_dir, exist_ok=True)
        
        # Run preprocessing
        logger.info(f"Running preprocessing for {job_context.file_path}")
        preproc = subprocess.run(
            ["python3", "preprocess_upload.py", job_context.file_path], 
            cwd=".", 
            capture_output=True, 
            text=True, 
            timeout=60
        )
        
        if preproc.returncode == 0 and preproc.stdout.strip():
            preprocessed = preproc.stdout.strip()
            if Path(preprocessed).exists():
                job_context.file_path = preprocessed
                logger.info(f"Using preprocessed file: {preprocessed}")
        
        # Run data processing
        logger.info(f"Processing data for job {job_context.job_id}")
        cmd = [
            "python3", "process_data_fintech.py", 
            "--raw", job_context.file_path,
            "--out_dir", output_dir,
            "--job_id", job_context.job_id
        ]
        
        proc = subprocess.run(
            cmd, 
            cwd=".", 
            capture_output=True, 
            text=True, 
            timeout=3600
        )
        
        if proc.returncode != 0:
            raise Exception(f"Data processing failed: {proc.stderr[:500]}")
        
        # Generate dashboard
        logger.info(f"Generating dashboard for job {job_context.job_id}")
        cmd2 = [
            "python3", "generate_dashboard.py",
            "--job_id", job_context.job_id
        ]
        
        proc2 = subprocess.run(
            cmd2,
            cwd=".",
            capture_output=True,
            text=True,
            timeout=300
        )
        
        if proc2.returncode != 0:
            raise Exception(f"Dashboard generation failed: {proc2.stderr[:500]}")
        
        # Upload outputs to cloud storage and track in database
        for filename in os.listdir(output_dir):
            file_path = os.path.join(output_dir, filename)
            if os.path.isfile(file_path):
                try:
                    # Upload to cloud storage
                    if supabase_storage.is_enabled():
                        storage_output_path = f"outputs/{job_context.job_id}/{filename}"
                        with open(file_path, 'rb') as f:
                            output_data = f.read()
                        
                        content_type = "text/csv" if filename.endswith('.csv') else "text/html"
                        cloud_uploaded = supabase_storage.upload_file("outputs", storage_output_path, output_data, content_type)
                        if cloud_uploaded:
                            logger.info(f"Output uploaded to cloud: {storage_output_path}")
                    
                    # Track in database
                    if supabase_rest.is_enabled():
                        file_type = _determine_file_type(filename)
                        file_size = os.path.getsize(file_path)
                        storage_path = f"outputs/{job_context.job_id}/{filename}"
                        
                        supabase_rest.create_output(job_context.job_id, file_type, storage_path, file_size)
                        logger.info(f"Output tracked in database: {filename}")
                        
                except Exception as e:
                    logger.error(f"Failed to process output {filename}: {e}")
        
        logger.info(f"Job {job_context.job_id} completed successfully")
        return True
        
    except Exception as e:
        logger.error(f"Job {job_context.job_id} failed: {e}")
        raise


def _process_file_simple(job_id: str, saved_path: str):
    """Simple file processing (fallback)"""
    try:
        # Update job status to running
        if supabase_rest.is_enabled():
            supabase_rest.update_job_status(job_id, "running")
        
        # Create output directory
        output_dir = f"outputs/{job_id}"
        os.makedirs(output_dir, exist_ok=True)
        
        # Run preprocessing
        logger.info(f"Running preprocessing for {saved_path}")
        preproc = subprocess.run(
            ["python3", "preprocess_upload.py", saved_path], 
            cwd=".", 
            capture_output=True, 
            text=True, 
            timeout=60
        )
        
        if preproc.returncode == 0 and preproc.stdout.strip():
            preprocessed = preproc.stdout.strip()
            if Path(preprocessed).exists():
                saved_path = preprocessed
                logger.info(f"Using preprocessed file: {saved_path}")
        
        # Run data processing
        logger.info(f"Processing data for job {job_id}")
        cmd = [
            "python3", "process_data_fintech.py", 
            "--raw", saved_path,
            "--out_dir", output_dir,
            "--job_id", job_id
        ]
        
        proc = subprocess.run(
            cmd, 
            cwd=".", 
            capture_output=True, 
            text=True, 
            timeout=3600
        )
        
        if proc.returncode != 0:
            error_msg = f"Data processing failed: {proc.stderr[:500]}"
            logger.error(error_msg)
            if supabase_rest.is_enabled():
                supabase_rest.update_job_status(job_id, "failed", error_msg)
            return
        
        # Generate dashboard
        logger.info(f"Generating dashboard for job {job_id}")
        cmd2 = [
            "python3", "generate_dashboard.py",
            "--job_id", job_id
        ]
        
        proc2 = subprocess.run(
            cmd2,
            cwd=".",
            capture_output=True,
            text=True,
            timeout=300
        )
        
        if proc2.returncode != 0:
            error_msg = f"Dashboard generation failed: {proc2.stderr[:500]}"
            logger.error(error_msg)
            if supabase_rest.is_enabled():
                supabase_rest.update_job_status(job_id, "failed", error_msg)
            return
        
        # Upload outputs to cloud storage and track in database
        for filename in os.listdir(output_dir):
            file_path = os.path.join(output_dir, filename)
            if os.path.isfile(file_path):
                try:
                    # Upload to cloud storage
                    if supabase_storage.is_enabled():
                        storage_output_path = f"outputs/{job_id}/{filename}"
                        with open(file_path, 'rb') as f:
                            output_data = f.read()
                        
                        content_type = "text/csv" if filename.endswith('.csv') else "text/html"
                        cloud_uploaded = supabase_storage.upload_file("outputs", storage_output_path, output_data, content_type)
                        if cloud_uploaded:
                            logger.info(f"Output uploaded to cloud: {storage_output_path}")
                    
                    # Track in database
                    if supabase_rest.is_enabled():
                        file_type = _determine_file_type(filename)
                        file_size = os.path.getsize(file_path)
                        storage_path = f"outputs/{job_id}/{filename}"
                        
                        supabase_rest.create_output(job_id, file_type, storage_path, file_size)
                        logger.info(f"Output tracked in database: {filename}")
                        
                except Exception as e:
                    logger.error(f"Failed to process output {filename}: {e}")
        
        # Mark job as done
        if supabase_rest.is_enabled():
            supabase_rest.update_job_status(job_id, "done")
        
        logger.info(f"Job {job_id} completed successfully")
        
    except Exception as e:
        error_msg = f"Processing failed: {str(e)}"
        logger.error(error_msg)
        if supabase_rest.is_enabled():
            supabase_rest.update_job_status(job_id, "error", error_msg)


def _show_duplicate_warning_page(duplicate_info: str, file_hash: str):
    """Show duplicate warning page"""
    recent_jobs = _get_recent_jobs()
    storage_health = supabase_storage.health_check()
    database_health = supabase_rest.health_check()
    queue_status = advanced_job_manager.get_queue_status()
    health_status = {"timestamp": datetime.utcnow().isoformat()}
    features = {
        "duplicate_detection": file_hasher.is_enabled(),
        "dataset_detection": dataset_detector.is_enabled(),
        "advanced_jobs": advanced_job_manager.is_enabled(),
        "retry_logic": advanced_job_manager.is_enabled()
    }
    
    return render_template_string(
        INDEX_HTML,
        recent_jobs=recent_jobs,
        health=health_status,
        storage=storage_health,
        database=database_health,
        queue_status=queue_status,
        features=features,
        duplicate_info=duplicate_info,
        file_hash=file_hash,
        detection_results=None
    )


def _show_detection_results_page(detection_results: Dict, file_hash: str):
    """Show dataset detection results page"""
    recent_jobs = _get_recent_jobs()
    storage_health = supabase_storage.health_check()
    database_health = supabase_rest.health_check()
    queue_status = advanced_job_manager.get_queue_status()
    health_status = {"timestamp": datetime.utcnow().isoformat()}
    features = {
        "duplicate_detection": file_hasher.is_enabled(),
        "dataset_detection": dataset_detector.is_enabled(),
        "advanced_jobs": advanced_job_manager.is_enabled(),
        "retry_logic": advanced_job_manager.is_enabled()
    }
    
    return render_template_string(
        INDEX_HTML,
        recent_jobs=recent_jobs,
        health=health_status,
        storage=storage_health,
        database=database_health,
        queue_status=queue_status,
        features=features,
        duplicate_info=None,
        file_hash=file_hash,
        detection_results=detection_results
    )


@app.route("/reprocess", methods=["POST"])
def reprocess():
    """Reprocess a duplicate file"""
    try:
        file_hash = request.form.get("file_hash")
        if not file_hash:
            flash("Invalid request")
            return redirect(url_for("index"))
        
        # Create new job for reprocessing
        if advanced_job_manager.is_enabled():
            # Get original file info
            upload_file = file_hasher.get_upload_file(file_hash) if file_hasher.is_enabled() else None
            if upload_file:
                job_id = advanced_job_manager.create_job(
                    file_path="unknown",  # Would need to reconstruct path
                    file_hash=file_hash,
                    original_filename=upload_file["original_name"],
                    callback=_process_file_callback
                )
            else:
                job_id = uuid.uuid4().hex[:8]
        else:
            job_id = uuid.uuid4().hex[:8]
            if supabase_rest.is_enabled():
                try:
                    upload_file = supabase_rest.get_upload_file(file_hash)
                    if upload_file:
                        job = supabase_rest.create_job(file_hash, upload_file["original_name"])
                        if job:
                            job_id = job["job_id"]
                except Exception as e:
                    logger.error(f"Database reprocess setup failed: {e}")
        
        flash(f"File queued for reprocessing. Job {job_id} created.")
        return redirect(url_for("index"))
        
    except Exception as e:
        logger.error(f"Reprocess failed: {e}")
        flash(f"Reprocess failed: {e}")
        return redirect(url_for("index"))


@app.route("/cancel_job", methods=["POST"])
def cancel_job():
    """Cancel a running job"""
    try:
        job_id = request.form.get("job_id")
        if not job_id:
            flash("Invalid job ID")
            return redirect(url_for("index"))
        
        if advanced_job_manager.is_enabled():
            success = advanced_job_manager.cancel_job(job_id)
            if success:
                flash(f"Job {job_id} cancelled successfully")
            else:
                flash(f"Failed to cancel job {job_id}")
        else:
            flash("Job cancellation not available in simple mode")
        
        return redirect(url_for("index"))
        
    except Exception as e:
        logger.error(f"Job cancellation failed: {e}")
        flash(f"Job cancellation failed: {e}")
        return redirect(url_for("index"))


def _determine_file_type(filename: str) -> str:
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


@app.route("/download/<output_id>", methods=["GET"])
def download_output(output_id):
    """Download output file with cloud/local fallback"""
    try:
        # Try to get output info from database first
        if supabase_rest.is_enabled():
            try:
                output = supabase_rest.get_output(output_id)
                if output:
                    # Try cloud storage first
                    if supabase_storage.is_enabled():
                        signed_url = supabase_storage.get_signed_url("outputs", output["storage_path"])
                        if signed_url:
                            logger.info(f"Downloading from cloud: {output['storage_path']}")
                            return redirect(signed_url)
                    
                    # Fall back to local file
                    file_path = Path("outputs") / output["storage_path"]
                    if file_path.exists():
                        logger.info(f"Downloading from local: {file_path}")
                        return send_from_directory(file_path.parent, file_path.name, as_attachment=True)
            except Exception as e:
                logger.error(f"Database download lookup failed: {e}")
        
        # Fall back to filesystem-based download
        job_id = output_id.split('_')[0]
        file_type = output_id.split('_')[1] if '_' in output_id else 'dashboard'
        
        file_mapping = {
            'ct': 'CT_Analysis_Output.csv',
            'tus': 'TUS_Analysis_Output.csv',
            'dashboard': 'dashboard.html',
            'audit': 'audit_lineage.csv'
        }
        
        filename = file_mapping.get(file_type, 'dashboard.html')
        file_path = Path("outputs") / job_id / filename
        
        if file_path.exists():
            logger.info(f"Downloading from local fallback: {file_path}")
            return send_from_directory(file_path.parent, file_path.name, as_attachment=True)
        else:
            abort(404)
        
    except Exception as e:
        logger.error(f"Download failed for {output_id}: {e}")
        abort(500)


@app.route("/view/<output_id>", methods=["GET"])
def view_dashboard(output_id):
    """View dashboard in browser with cloud/local fallback"""
    try:
        # Try to get output info from database first
        if supabase_rest.is_enabled():
            try:
                output = supabase_rest.get_output(output_id)
                if output and output["file_type"] == "dashboard":
                    # Try cloud storage first
                    if supabase_storage.is_enabled():
                        signed_url = supabase_storage.get_signed_url("outputs", output["storage_path"])
                        if signed_url:
                            logger.info(f"Viewing from cloud: {output['storage_path']}")
                            return redirect(signed_url)
                    
                    # Fall back to local file
                    file_path = Path("outputs") / output["storage_path"]
                    if file_path.exists():
                        logger.info(f"Viewing from local: {file_path}")
                        return send_from_directory(file_path.parent, file_path.name)
            except Exception as e:
                logger.error(f"Database view lookup failed: {e}")
        
        # Fall back to filesystem-based view
        job_id = output_id.split('_')[0]
        file_path = Path("outputs") / job_id / "dashboard.html"
        
        if file_path.exists():
            logger.info(f"Viewing from local fallback: {file_path}")
            return send_from_directory(file_path.parent, file_path.name)
        else:
            abort(404)
        
    except Exception as e:
        logger.error(f"View failed for {output_id}: {e}")
        abort(500)


# ----------------------
# Local run block
# ----------------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
