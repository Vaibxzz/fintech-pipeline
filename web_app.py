#!/usr/bin/env python3
"""
web_app_phase1.py - Phase 1: Hybrid storage with Supabase + local fallback
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
# from supabase_storage_client import supabase_storage  # Disabled for basic version

# Basic storage fallback for basic version
class BasicStorage:
    def health_check(self):
        return {"enabled": False, "connected": False, "timestamp": None}
    
    def is_enabled(self):
        return False
    
    def list_files(self, bucket, path):
        return []
    
    def upload_file(self, bucket, path, data, content_type=None):
        return False
    
    def get_signed_url(self, bucket, path):
        return None

supabase_storage = BasicStorage()

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
logger.info("Phase 1 web app starting with hybrid storage")

# ----------------------
# HTML template
# ----------------------
INDEX_HTML = """
<!DOCTYPE html>
<html>
<head>
    <title>Fintech Data Pipeline - Phase 1</title>
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
    </style>
</head>
<body>
    <div class="container">
        <h1>Fintech Data Pipeline - Phase 1</h1>
        
        <!-- Storage Status -->
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
                    {% if job.error_msg %}
                    <strong>Error:</strong> {{ job.error_msg }}<br>
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
    
    health_status = {
        "local_storage": True,
        "supabase_storage": storage_health,
        "timestamp": datetime.utcnow().isoformat()
    }
    return jsonify(health_status), 200


@app.route("/", methods=["GET"])
def index():
    """Main page"""
    try:
        # Get recent jobs from local filesystem
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
                                "cloud_available": _check_cloud_file(f"outputs/{job_id}/CT_Analysis_Output.csv")
                            },
                            {
                                "output_id": f"{job_id}_tus", 
                                "file_type": "TUS",
                                "cloud_available": _check_cloud_file(f"outputs/{job_id}/TUS_Analysis_Output.csv")
                            },
                            {
                                "output_id": f"{job_id}_dashboard", 
                                "file_type": "dashboard",
                                "cloud_available": _check_cloud_file(f"outputs/{job_id}/dashboard.html")
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
                        'error_msg': None,
                        'outputs': outputs
                    })
        
        # Sort by upload time (most recent first)
        recent_jobs.sort(key=lambda x: x['uploaded_at'], reverse=True)
        recent_jobs = recent_jobs[:10]  # Limit to 10 most recent
        
        # Get system health
        storage_health = supabase_storage.health_check()
        health_status = {
            "timestamp": datetime.utcnow().isoformat()
        }
        
        return render_template_string(
            INDEX_HTML, 
            recent_jobs=recent_jobs,
            health=health_status,
            storage=storage_health
        )
        
    except Exception as e:
        logger.error(f"Index page error: {e}")
        return f"Error loading page: {e}", 500


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
        
        # Create job ID
        job_id = uuid.uuid4().hex[:8]
        
        # Run processing in background
        def process_file():
            try:
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
                    logger.error(f"Data processing failed: {proc.stderr}")
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
                    logger.error(f"Dashboard generation failed: {proc2.stderr}")
                    return
                
                # Upload outputs to cloud storage if enabled
                if supabase_storage.is_enabled():
                    for filename in os.listdir(output_dir):
                        file_path = os.path.join(output_dir, filename)
                        if os.path.isfile(file_path):
                            try:
                                storage_output_path = f"outputs/{job_id}/{filename}"
                                with open(file_path, 'rb') as f:
                                    output_data = f.read()
                                
                                content_type = "text/csv" if filename.endswith('.csv') else "text/html"
                                cloud_uploaded = supabase_storage.upload_file("outputs", storage_output_path, output_data, content_type)
                                if cloud_uploaded:
                                    logger.info(f"Output uploaded to cloud: {storage_output_path}")
                            except Exception as e:
                                logger.error(f"Failed to upload output {filename}: {e}")
                
                logger.info(f"Job {job_id} completed successfully")
                
            except Exception as e:
                logger.error(f"Processing failed for job {job_id}: {e}")
        
        # Start background processing
        thread = threading.Thread(target=process_file, daemon=True)
        thread.start()
        
        cloud_status = " (cloud storage enabled)" if cloud_uploaded else ""
        flash(f"File uploaded successfully. Job {job_id} is processing in the background.{cloud_status}")
        return redirect(url_for("index"))
        
    except Exception as e:
        logger.error(f"Upload failed: {e}")
        flash(f"Upload failed: {e}")
        return redirect(url_for("index"))


@app.route("/download/<output_id>", methods=["GET"])
def download_output(output_id):
    """Download output file with cloud/local fallback"""
    try:
        # Extract job_id and file type from output_id
        job_id = output_id.split('_')[0]
        file_type = output_id.split('_')[1] if '_' in output_id else 'dashboard'
        
        # Map file types to actual filenames
        file_mapping = {
            'ct': 'CT_Analysis_Output.csv',
            'tus': 'TUS_Analysis_Output.csv',
            'dashboard': 'dashboard.html',
            'audit': 'audit_lineage.csv'
        }
        
        filename = file_mapping.get(file_type, 'dashboard.html')
        file_path = Path("outputs") / job_id / filename
        
        # Try cloud storage first if enabled
        if supabase_storage.is_enabled():
            try:
                storage_path = f"outputs/{job_id}/{filename}"
                signed_url = supabase_storage.get_signed_url("outputs", storage_path)
                if signed_url:
                    logger.info(f"Downloading from cloud: {storage_path}")
                    return redirect(signed_url)
            except Exception as e:
                logger.error(f"Cloud download failed: {e}")
        
        # Fall back to local file
        if file_path.exists():
            logger.info(f"Downloading from local: {file_path}")
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
        # Extract job_id from output_id
        job_id = output_id.split('_')[0]
        file_path = Path("outputs") / job_id / "dashboard.html"
        
        # Try cloud storage first if enabled
        if supabase_storage.is_enabled():
            try:
                storage_path = f"outputs/{job_id}/dashboard.html"
                signed_url = supabase_storage.get_signed_url("outputs", storage_path)
                if signed_url:
                    logger.info(f"Viewing from cloud: {storage_path}")
                    return redirect(signed_url)
            except Exception as e:
                logger.error(f"Cloud view failed: {e}")
        
        # Fall back to local file
        if file_path.exists():
            logger.info(f"Viewing from local: {file_path}")
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
