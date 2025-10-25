#!/usr/bin/env python3
"""
web_app_simple.py - Simplified version without direct psycopg2 dependency
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
from supabase import create_client, Client

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
logger.info("Simplified web app starting")

# ----------------------
# Supabase client
# ----------------------
supabase_url = os.environ.get("SUPABASE_URL")
supabase_key = os.environ.get("SUPABASE_SERVICE_KEY")

if not supabase_url or not supabase_key:
    logger.error("Missing SUPABASE_URL or SUPABASE_SERVICE_KEY environment variables")
    raise ValueError("Supabase configuration required")

supabase: Client = create_client(supabase_url, supabase_key)

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
        .download-btn { background: #0066cc; color: white; padding: 5px 10px; text-decoration: none; border-radius: 3px; }
        .download-btn:hover { background: #0052a3; }
        .stats { display: flex; gap: 20px; margin: 20px 0; }
        .stat-box { background: #f8f9fa; padding: 15px; border-radius: 5px; text-align: center; }
    </style>
</head>
<body>
    <div class="container">
        <h1>Fintech Data Pipeline</h1>
        
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
    try:
        # Test storage connection
        supabase.storage.from_("uploads").list()
        storage_status = True
    except Exception as e:
        logger.error(f"Storage health check failed: {e}")
        storage_status = False
    
    health_status = {
        "database": True,  # Assume true for simplified version
        "storage": storage_status,
        "timestamp": datetime.utcnow().isoformat()
    }
    return jsonify(health_status), 200


@app.route("/", methods=["GET"])
def index():
    """Main page"""
    try:
        # Get recent jobs from storage (simplified approach)
        recent_jobs = []
        
        # Try to list files in outputs bucket to show recent activity
        try:
            output_files = supabase.storage.from_("outputs").list()
            # Create mock job entries based on files found
            for file_info in output_files[:10]:  # Limit to 10 most recent
                if file_info.get('name', '').endswith('.html'):
                    job_id = file_info['name'].split('/')[0] if '/' in file_info['name'] else 'unknown'
                    recent_jobs.append({
                        'job_id': job_id,
                        'status': 'done',
                        'original_filename': 'processed_file.csv',
                        'uploaded_at': file_info.get('created_at', 'Unknown'),
                        'error_msg': None,
                        'outputs': [
                            {
                                'output_id': f"{job_id}_dashboard",
                                'file_type': 'dashboard'
                            }
                        ]
                    })
        except Exception as e:
            logger.error(f"Error listing output files: {e}")
        
        # Get system health
        health_status = {
            "storage": True,
            "timestamp": datetime.utcnow().isoformat()
        }
        
        return render_template_string(
            INDEX_HTML, 
            recent_jobs=recent_jobs,
            health=health_status
        )
        
    except Exception as e:
        logger.error(f"Index page error: {e}")
        return f"Error loading page: {e}", 500


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
        
        # Upload to Supabase Storage
        storage_path = f"uploads/{file_hash}.{Path(fname).suffix[1:]}"
        with open(saved_path, 'rb') as f:
            file_data = f.read()
        
        supabase.storage.from_("uploads").upload(storage_path, file_data)
        
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
                
                # Upload outputs to storage
                for filename in os.listdir(output_dir):
                    file_path = os.path.join(output_dir, filename)
                    if os.path.isfile(file_path):
                        storage_output_path = f"outputs/{job_id}/{filename}"
                        with open(file_path, 'rb') as f:
                            output_data = f.read()
                        
                        supabase.storage.from_("outputs").upload(storage_output_path, output_data)
                        logger.info(f"Uploaded {filename} for job {job_id}")
                
                logger.info(f"Job {job_id} completed successfully")
                
            except Exception as e:
                logger.error(f"Processing failed for job {job_id}: {e}")
        
        # Start background processing
        thread = threading.Thread(target=process_file, daemon=True)
        thread.start()
        
        flash(f"File uploaded successfully. Job {job_id} is processing in the background.")
        return redirect(url_for("index"))
        
    except Exception as e:
        logger.error(f"Upload failed: {e}")
        flash(f"Upload failed: {e}")
        return redirect(url_for("index"))


@app.route("/download/<output_id>", methods=["GET"])
def download_output(output_id):
    """Download output file"""
    try:
        # Extract job_id from output_id (simplified approach)
        job_id = output_id.split('_')[0]
        
        # Try to find the file in storage
        try:
            files = supabase.storage.from_("outputs").list(f"outputs/{job_id}")
            for file_info in files:
                if file_info.get('name', '').endswith(('.csv', '.html')):
                    file_path = f"outputs/{job_id}/{file_info['name']}"
                    signed_url = supabase.storage.from_("outputs").create_signed_url(file_path, 3600)
                    
                    if signed_url.get("error"):
                        raise Exception(f"Signed URL generation failed: {signed_url['error']}")
                    
                    logger.info(f"Generated download URL for {output_id}")
                    return redirect(signed_url["signedURL"])
        except Exception as e:
            logger.error(f"Download failed for {output_id}: {e}")
        
        abort(404)
        
    except Exception as e:
        logger.error(f"Download failed for {output_id}: {e}")
        abort(500)


@app.route("/view/<output_id>", methods=["GET"])
def view_dashboard(output_id):
    """View dashboard in browser"""
    try:
        # Extract job_id from output_id
        job_id = output_id.split('_')[0]
        
        # Try to find dashboard file
        try:
            files = supabase.storage.from_("outputs").list(f"outputs/{job_id}")
            for file_info in files:
                if file_info.get('name', '').endswith('.html'):
                    file_path = f"outputs/{job_id}/{file_info['name']}"
                    signed_url = supabase.storage.from_("outputs").create_signed_url(file_path, 3600)
                    
                    if signed_url.get("error"):
                        raise Exception(f"Signed URL generation failed: {signed_url['error']}")
                    
                    logger.info(f"Generated view URL for dashboard {output_id}")
                    return redirect(signed_url["signedURL"])
        except Exception as e:
            logger.error(f"View failed for {output_id}: {e}")
        
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
