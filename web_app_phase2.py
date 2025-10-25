#!/usr/bin/env python3
"""
web_app_phase2.py - Phase 2: Database integration with hybrid storage and fallback
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

# ----------------------
# App & logging
# ----------------------
app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "change-me-123")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger("fintech_web_app")
logger.info("Phase 2 web app starting with database integration")

# ----------------------
# HTML template
# ----------------------
INDEX_HTML = """
<!DOCTYPE html>
<html>
<head>
    <title>Fintech Data Pipeline - Phase 2</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 40px; }
        .container { max-width: 1200px; margin: 0 auto; }
        .upload-section { border: 2px dashed #ccc; padding: 20px; margin: 20px 0; text-align: center; }
        .job-list { margin: 20px 0; }
        .job-item { border: 1px solid #ddd; padding: 15px; margin: 10px 0; border-radius: 5px; }
        .job-status { font-weight: bold; }
        .status-queued { color: #ffa500; }
        .status-running { color: #0066cc; }
        .status-done { color: #00aa00; }
        .status-failed { color: #cc0000; }
        .download-btn { background: #0066cc; color: white; padding: 5px 10px; text-decoration: none; border-radius: 3px; }
        .download-btn:hover { background: #0052a3; }
        .stats { display: flex; gap: 20px; margin: 20px 0; }
        .stat-box { background: #f8f9fa; padding: 15px; border-radius: 5px; text-align: center; }
        .storage-status { background: #e7f3ff; border: 1px solid #b3d9ff; padding: 10px; margin: 10px 0; border-radius: 5px; }
        .database-status { background: #f0f8ff; border: 1px solid #b3d9ff; padding: 10px; margin: 10px 0; border-radius: 5px; }
        .duplicate-warning { background: #fff3cd; border: 1px solid #ffeaa7; padding: 10px; margin: 10px 0; border-radius: 5px; }
    </style>
</head>
<body>
    <div class="container">
        <h1>Fintech Data Pipeline - Phase 2</h1>
        
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
            <p>{{ duplicate_info }}</p>
            <form method="post" action="/reprocess" style="display: inline;">
                <input type="hidden" name="file_hash" value="{{ file_hash }}">
                <input type="submit" value="Reprocess Anyway" style="background: #dc3545; color: white; padding: 5px 10px; border: none; border-radius: 3px;">
            </form>
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
    
    health_status = {
        "local_storage": True,
        "supabase_storage": storage_health,
        "database": database_health,
        "timestamp": datetime.utcnow().isoformat()
    }
    return jsonify(health_status), 200


@app.route("/", methods=["GET"])
def index():
    """Main page"""
    try:
        # Get recent jobs from database or filesystem
        recent_jobs = _get_recent_jobs()
        
        # Get system health
        storage_health = supabase_storage.health_check()
        database_health = supabase_rest.health_check()
        health_status = {
            "timestamp": datetime.utcnow().isoformat()
        }
        
        return render_template_string(
            INDEX_HTML, 
            recent_jobs=recent_jobs,
            health=health_status,
            storage=storage_health,
            database=database_health,
            duplicate_info=None,
            file_hash=None
        )
        
    except Exception as e:
        logger.error(f"Index page error: {e}")
        return f"Error loading page: {e}", 500


def _get_recent_jobs() -> List[Dict]:
    """Get recent jobs from database or filesystem fallback"""
    recent_jobs = []
    
    # Try database first
    if supabase_rest.is_enabled():
        try:
            # Get jobs from database
            db_jobs = supabase_rest.get_jobs_by_status("done", limit=10)
            db_jobs.extend(supabase_rest.get_jobs_by_status("running", limit=5))
            db_jobs.extend(supabase_rest.get_jobs_by_status("failed", limit=5))
            
            for job in db_jobs:
                # Get outputs from database
                outputs = supabase_rest.get_outputs_by_job(job["job_id"])
                output_list = []
                
                for output in outputs:
                    output_list.append({
                        "output_id": output["output_id"],
                        "file_type": output["file_type"],
                        "cloud_available": _check_cloud_file(output["storage_path"]),
                        "database_tracked": True
                    })
                
                recent_jobs.append({
                    'job_id': job["job_id"],
                    'status': job["status"],
                    'original_filename': job["original_filename"],
                    'uploaded_at': job["uploaded_at"][:19] if job["uploaded_at"] else "Unknown",
                    'started_at': job["started_at"][:19] if job["started_at"] else None,
                    'finished_at': job["finished_at"][:19] if job["finished_at"] else None,
                    'error_msg': job["error_msg"],
                    'dataset_type': job["dataset_type"],
                    'outputs': output_list
                })
            
            # Sort by upload time (most recent first)
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


def compute_file_hash(file_path: str) -> str:
    """Compute SHA-256 hash of file"""
    hash_sha256 = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_sha256.update(chunk)
    return hash_sha256.hexdigest()


@app.route("/upload", methods=["POST"])
def upload():
    """Handle file upload"""
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
        
        # Compute file hash
        file_hash = compute_file_hash(saved_path)
        
        # Check for duplicates in database
        if supabase_rest.is_enabled():
            try:
                upload_file = supabase_rest.get_upload_file(file_hash)
                if upload_file:
                    recent_jobs = supabase_rest.get_recent_jobs_for_file(file_hash, limit=1)
                    if recent_jobs:
                        job = recent_jobs[0]
                        duplicate_info = f"File already processed (Job ID: {job['job_id']}, Status: {job['status']})"
                    else:
                        duplicate_info = "File already processed"
                    
                    # Show duplicate warning page
                    storage_health = supabase_storage.health_check()
                    database_health = supabase_rest.health_check()
                    return render_template_string(
                        INDEX_HTML,
                        recent_jobs=_get_recent_jobs(),
                        health={"timestamp": datetime.utcnow().isoformat()},
                        storage=storage_health,
                        database=database_health,
                        duplicate_info=duplicate_info,
                        file_hash=file_hash
                    )
            except Exception as e:
                logger.error(f"Duplicate check failed: {e}")
        
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
        
        # Create job in database or use filesystem
        job_id = uuid.uuid4().hex[:8]
        
        if supabase_rest.is_enabled():
            try:
                # Create job in database
                job = supabase_rest.create_job(file_hash, fname)
                if job:
                    job_id = job["job_id"]
                    logger.info(f"Created job in database: {job_id}")
                
                # Record upload file
                supabase_rest.create_or_update_upload_file(file_hash, fname)
            except Exception as e:
                logger.error(f"Database job creation failed: {e}")
        
        # Run processing in background
        def process_file():
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
        
        # Start background processing
        thread = threading.Thread(target=process_file, daemon=True)
        thread.start()
        
        cloud_status = " (cloud storage enabled)" if cloud_uploaded else ""
        db_status = " (database tracking enabled)" if supabase_rest.is_enabled() else ""
        flash(f"File uploaded successfully. Job {job_id} is processing in the background.{cloud_status}{db_status}")
        return redirect(url_for("index"))
        
    except Exception as e:
        logger.error(f"Upload failed: {e}")
        flash(f"Upload failed: {e}")
        return redirect(url_for("index"))


@app.route("/reprocess", methods=["POST"])
def reprocess():
    """Reprocess a duplicate file"""
    try:
        file_hash = request.form.get("file_hash")
        if not file_hash:
            flash("Invalid request")
            return redirect(url_for("index"))
        
        # Create new job for reprocessing
        job_id = uuid.uuid4().hex[:8]
        
        if supabase_rest.is_enabled():
            try:
                # Get original file info
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
