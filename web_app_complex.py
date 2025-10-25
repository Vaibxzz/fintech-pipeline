#!/usr/bin/env python3
"""
web_app.py (cloud-native version)

Flask web UI with Supabase database and storage integration.
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
from config import config
from job_manager import job_manager
from storage_manager import storage_manager
from dataset_detector import dataset_detector
from database_models import JobRepository, OutputRepository, UploadFileRepository
from supabase_client import supabase_client

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
logger.info("Cloud-native web app starting")

# ----------------------
# HTML template
# ----------------------
INDEX_HTML = """
<!DOCTYPE html>
<html>
<head>
    <title>Fintech Data Pipeline</title>
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
        .outputs { margin: 10px 0; }
        .output-item { margin: 5px 0; }
        .download-btn { background: #0066cc; color: white; padding: 5px 10px; text-decoration: none; border-radius: 3px; }
        .download-btn:hover { background: #0052a3; }
        .duplicate-warning { background: #fff3cd; border: 1px solid #ffeaa7; padding: 10px; margin: 10px 0; border-radius: 5px; }
        .stats { display: flex; gap: 20px; margin: 20px 0; }
        .stat-box { background: #f8f9fa; padding: 15px; border-radius: 5px; text-align: center; }
    </style>
</head>
<body>
    <div class="container">
        <h1>Fintech Data Pipeline</h1>
        
        <!-- Statistics -->
        <div class="stats">
            <div class="stat-box">
                <h3>{{ stats.queued }}</h3>
                <p>Queued</p>
            </div>
            <div class="stat-box">
                <h3>{{ stats.running }}</h3>
                <p>Running</p>
            </div>
            <div class="stat-box">
                <h3>{{ stats.done }}</h3>
                <p>Completed</p>
            </div>
            <div class="stat-box">
                <h3>{{ stats.failed }}</h3>
                <p>Failed</p>
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
                </div>
                
                {% if job.outputs %}
                <div class="outputs">
                    <h4>Outputs:</h4>
                    {% for output in job.outputs %}
                    <div class="output-item">
                        <strong>{{ output.file_type }}:</strong> 
                        <a href="/download/{{ output.output_id }}" class="download-btn">Download</a>
                        {% if output.file_type == 'dashboard' %}
                        <a href="/view/{{ output.output_id }}" class="download-btn" style="background: #28a745;">View</a>
                        {% endif %}
                        ({{ output.file_size }} bytes)
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
            <p><strong>Database:</strong> 
                <span style="color: {{ 'green' if health.database else 'red' }}">
                    {{ 'Connected' if health.database else 'Disconnected' }}
                </span>
            </p>
            <p><strong>Storage:</strong> 
                <span style="color: {{ 'green' if health.storage else 'red' }}">
                    {{ 'Connected' if health.storage else 'Disconnected' }}
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
    health_status = supabase_client.health_check()
    return jsonify(health_status), 200


@app.route("/", methods=["GET"])
def index():
    """Main page"""
    try:
        # Get job statistics
        stats = job_manager.get_job_statistics()
        
        # Get recent jobs with outputs
        recent_jobs = []
        for status in ['running', 'done', 'failed', 'queued']:
            jobs = JobRepository.get_jobs_by_status(status, limit=5)
            for job in jobs:
                outputs = OutputRepository.get_outputs_by_job(job.job_id)
                job_dict = {
                    'job_id': job.job_id,
                    'status': job.status,
                    'original_filename': job.original_filename,
                    'uploaded_at': job.uploaded_at.strftime('%Y-%m-%d %H:%M:%S') if job.uploaded_at else '',
                    'started_at': job.started_at.strftime('%Y-%m-%d %H:%M:%S') if job.started_at else None,
                    'finished_at': job.finished_at.strftime('%Y-%m-%d %H:%M:%S') if job.finished_at else None,
                    'error_msg': job.error_msg,
                    'outputs': [
                        {
                            'output_id': output.output_id,
                            'file_type': output.file_type,
                            'file_size': output.file_size
                        } for output in outputs
                    ]
                }
                recent_jobs.append(job_dict)
        
        # Sort by upload time (most recent first)
        recent_jobs.sort(key=lambda x: x['uploaded_at'], reverse=True)
        recent_jobs = recent_jobs[:20]  # Limit to 20 most recent
        
        # Get system health
        health_status = supabase_client.health_check()
        
        return render_template_string(
            INDEX_HTML, 
            stats=stats, 
            recent_jobs=recent_jobs,
            health=health_status,
            duplicate_info=None,
            file_hash=None
        )
        
    except Exception as e:
        logger.error(f"Index page error: {e}")
        return f"Error loading page: {e}", 500


def allowed_file(filename):
    """Check if file extension is allowed"""
    return Path(filename).suffix.lower() in config.allowed_extensions


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
            flash(f"Unsupported file type. Allowed: {', '.join(sorted(config.allowed_extensions))}")
            return redirect(url_for("index"))

        # Save uploaded file
        fname = Path(file.filename).name
        uid = uuid.uuid4().hex[:8]
        saved_name = f"{uid}_{fname}"
        saved_path = os.path.join(config.upload_folder, saved_name)
        file.save(saved_path)
        
        logger.info(f"File saved to {saved_path}")
        
        # Compute file hash
        file_hash = compute_file_hash(saved_path)
        
        # Check for duplicates
        upload_file = UploadFileRepository.get_upload_file(file_hash)
        if upload_file:
            recent_jobs = UploadFileRepository.get_recent_jobs_for_file(file_hash, limit=1)
            if recent_jobs:
                job = recent_jobs[0]
                duplicate_info = f"File already processed (Job ID: {job.job_id}, Status: {job.status})"
            else:
                duplicate_info = "File already processed"
            
            # Show duplicate warning page
            return render_template_string(
                INDEX_HTML,
                stats=job_manager.get_job_statistics(),
                recent_jobs=[],
                health=supabase_client.health_check(),
                duplicate_info=duplicate_info,
                file_hash=file_hash
            )
        
        # Detect dataset type
        detection_result = dataset_detector.detect_dataset_type(saved_path)
        dataset_type = detection_result.dataset_type if detection_result.confidence >= 0.7 else None
        
        # Create job
        job = job_manager.create_job(file_hash, fname, dataset_type)
        
        # Upload file to storage
        storage_path = f"uploads/{file_hash}.{Path(fname).suffix[1:]}"
        with open(saved_path, 'rb') as f:
            file_data = f.read()
        
        storage_manager.upload_file("uploads", storage_path, file_data)
        
        # Run preprocessing
        try:
            logger.info(f"Running preprocessing for {saved_path}")
            preproc = subprocess.run(
                ["python3", "preprocess_upload.py", saved_path, file_hash], 
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
        except Exception as e:
            logger.exception(f"Preprocessing failed; continuing with original file: {e}")
        
        flash(f"File uploaded successfully. Job {job.job_id} queued for processing.")
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
        
        # Find the original file
        upload_file = UploadFileRepository.get_upload_file(file_hash)
        if not upload_file:
            flash("Original file not found")
            return redirect(url_for("index"))
        
        # Create new job for reprocessing
        job = job_manager.create_job(file_hash, upload_file.original_name)
        
        flash(f"File queued for reprocessing. Job {job.job_id} created.")
        return redirect(url_for("index"))
        
    except Exception as e:
        logger.error(f"Reprocess failed: {e}")
        flash(f"Reprocess failed: {e}")
        return redirect(url_for("index"))


@app.route("/job/<job_id>", methods=["GET"])
def job_status(job_id):
    """Get job status as JSON"""
    try:
        job_info = job_manager.get_job_with_outputs(job_id)
        if not job_info:
            return jsonify({"status": "not_found"}), 404
        
        # Convert to JSON-serializable format
        job_dict = {
            "job_id": job_info["job"].job_id,
            "status": job_info["job"].status,
            "uploaded_at": job_info["job"].uploaded_at.isoformat() if job_info["job"].uploaded_at else None,
            "started_at": job_info["job"].started_at.isoformat() if job_info["job"].started_at else None,
            "finished_at": job_info["job"].finished_at.isoformat() if job_info["job"].finished_at else None,
            "original_filename": job_info["job"].original_filename,
            "dataset_type": job_info["job"].dataset_type,
            "error_msg": job_info["job"].error_msg,
            "outputs": [
                {
                    "output_id": output.output_id,
                    "file_type": output.file_type,
                    "storage_path": output.storage_path,
                    "file_size": output.file_size,
                    "created_at": output.created_at.isoformat() if output.created_at else None
                } for output in job_info["outputs"]
            ]
        }
        
        return jsonify(job_dict)
        
    except Exception as e:
        logger.error(f"Job status error for {job_id}: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/download/<output_id>", methods=["GET"])
def download_output(output_id):
    """Download output file"""
    try:
        output = OutputRepository.get_output(output_id)
        if not output:
            abort(404)
        
        # Generate signed URL for download
        bucket, file_path = output.storage_path.split('/', 1)
        signed_url = storage_manager.get_signed_url(bucket, file_path, expires_in=3600)
        
        logger.info(f"Generated download URL for {output_id}")
        return redirect(signed_url)
        
    except Exception as e:
        logger.error(f"Download failed for {output_id}: {e}")
        abort(500)


@app.route("/view/<output_id>", methods=["GET"])
def view_dashboard(output_id):
    """View dashboard in browser"""
    try:
        output = OutputRepository.get_output(output_id)
        if not output:
            abort(404)
        
        if output.file_type != 'dashboard':
            return redirect(url_for("download_output", output_id=output_id))
        
        # Generate signed URL for viewing
        bucket, file_path = output.storage_path.split('/', 1)
        signed_url = storage_manager.get_signed_url(bucket, file_path, expires_in=3600)
        
        logger.info(f"Generated view URL for dashboard {output_id}")
        return redirect(signed_url)
        
    except Exception as e:
        logger.error(f"View failed for {output_id}: {e}")
        abort(500)


# ----------------------
# Startup
# ----------------------
def start_background_worker():
    """Start background worker"""
    try:
        job_manager.start_worker()
        logger.info("Background worker started")
    except Exception as e:
        logger.error(f"Failed to start background worker: {e}")


# Start worker on app startup
start_background_worker()

# ----------------------
# Local run block
# ----------------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
