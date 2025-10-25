# Phase 3 Deployment Guide

## 🚀 Deploy Phase 3: Advanced Features

This guide will help you deploy the Phase 3 fintech pipeline with all advanced features including file hashing, duplicate detection, dataset detection, and enhanced job management.

## Prerequisites

### 1. Supabase Project Setup

You need a Supabase project with:
- **Database**: PostgreSQL with the required tables
- **Storage**: Two buckets (`uploads` and `outputs`)
- **API Keys**: Service key and anon key

### 2. Required Environment Variables

```bash
# Supabase Configuration (REQUIRED)
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_SERVICE_KEY=your-service-key-here
SUPABASE_ANON_KEY=your-anon-key-here

# Feature Flags (OPTIONAL - defaults to true)
ENABLE_SUPABASE_STORAGE=true
ENABLE_DATABASE_TRACKING=true
ENABLE_DUPLICATE_DETECTION=true
ENABLE_DATASET_DETECTION=true
ENABLE_ADVANCED_JOBS=true

# App Configuration
SECRET_KEY=auto-generated-by-render
```

## Deployment Options

### Option 1: Render Deployment (Recommended)

#### Step 1: Create New Render Service

1. Go to [Render Dashboard](https://dashboard.render.com)
2. Click "New +" → "Web Service"
3. Connect your GitHub repository
4. Use these settings:

```
Name: fintech-pipeline-phase3
Environment: Python
Build Command: pip install -r requirements_phase3.txt
Start Command: gunicorn web_app_phase3:app
```

#### Step 2: Set Environment Variables

In the Render dashboard, go to Environment tab and add:

```
SUPABASE_URL = https://your-project.supabase.co
SUPABASE_SERVICE_KEY = your-service-key
SUPABASE_ANON_KEY = your-anon-key
ENABLE_SUPABASE_STORAGE = true
ENABLE_DATABASE_TRACKING = true
ENABLE_DUPLICATE_DETECTION = true
ENABLE_DATASET_DETECTION = true
ENABLE_ADVANCED_JOBS = true
```

#### Step 3: Deploy

1. Click "Create Web Service"
2. Wait for build to complete (2-3 minutes)
3. Test the deployment

### Option 2: Railway Deployment

#### Step 1: Create Railway Project

1. Go to [Railway](https://railway.app)
2. Click "New Project" → "Deploy from GitHub repo"
3. Select your repository

#### Step 2: Configure Environment

In Railway dashboard, add environment variables:

```
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_SERVICE_KEY=your-service-key
SUPABASE_ANON_KEY=your-anon-key
ENABLE_SUPABASE_STORAGE=true
ENABLE_DATABASE_TRACKING=true
ENABLE_DUPLICATE_DETECTION=true
ENABLE_DATASET_DETECTION=true
ENABLE_ADVANCED_JOBS=true
```

#### Step 3: Deploy

Railway will automatically deploy when you push to main branch.

## Supabase Setup

### 1. Create Database Tables

Run this SQL in your Supabase SQL Editor:

```sql
-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Jobs table
CREATE TABLE jobs (
    job_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    status VARCHAR(20) NOT NULL DEFAULT 'queued' CHECK (status IN ('queued', 'running', 'done', 'failed', 'error', 'retrying', 'cancelled')),
    uploaded_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    started_at TIMESTAMP WITH TIME ZONE,
    finished_at TIMESTAMP WITH TIME ZONE,
    file_hash VARCHAR(64) NOT NULL,
    original_filename VARCHAR(255) NOT NULL,
    dataset_type VARCHAR(50),
    error_msg TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Outputs table
CREATE TABLE outputs (
    output_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    job_id UUID NOT NULL REFERENCES jobs(job_id) ON DELETE CASCADE,
    file_type VARCHAR(20) NOT NULL CHECK (file_type IN ('CT', 'TUS', 'dashboard', 'audit', 'raw')),
    storage_path VARCHAR(500) NOT NULL,
    file_size BIGINT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Upload files table
CREATE TABLE upload_files (
    file_hash VARCHAR(64) PRIMARY KEY,
    original_name VARCHAR(255) NOT NULL,
    normalized_path VARCHAR(500),
    first_seen TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    last_used TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    usage_count INTEGER DEFAULT 1
);

-- Indexes for performance
CREATE INDEX idx_jobs_status ON jobs(status);
CREATE INDEX idx_jobs_file_hash ON jobs(file_hash);
CREATE INDEX idx_outputs_job_id ON outputs(job_id);
CREATE INDEX idx_upload_files_last_used ON upload_files(last_used);

-- Row Level Security (RLS) policies
ALTER TABLE jobs ENABLE ROW LEVEL SECURITY;
ALTER TABLE outputs ENABLE ROW LEVEL SECURITY;
ALTER TABLE upload_files ENABLE ROW LEVEL SECURITY;

-- Allow all operations for service role
CREATE POLICY "Allow all for service role" ON jobs FOR ALL USING (true);
CREATE POLICY "Allow all for service role" ON outputs FOR ALL USING (true);
CREATE POLICY "Allow all for service role" ON upload_files FOR ALL USING (true);
```

### 2. Create Storage Buckets

1. Go to Storage in Supabase dashboard
2. Create bucket: `uploads`
3. Create bucket: `outputs`
4. Set both buckets to public (for signed URLs)

### 3. Get API Keys

1. Go to Settings → API
2. Copy:
   - **Project URL** (for SUPABASE_URL)
   - **Service Role Key** (for SUPABASE_SERVICE_KEY)
   - **Anon Key** (for SUPABASE_ANON_KEY)

## Testing the Deployment

### 1. Health Check

Visit: `https://your-app.onrender.com/health`

Expected response:
```json
{
  "local_storage": true,
  "supabase_storage": {
    "enabled": true,
    "connected": true
  },
  "database": {
    "enabled": true,
    "connected": true
  },
  "queue": {
    "enabled": true,
    "queued_jobs": 0,
    "running_jobs": 0
  }
}
```

### 2. Upload Test

1. Go to your app URL
2. Upload a CSV or Excel file
3. Check that:
   - File is processed successfully
   - Dashboard is generated
   - Job status shows in the UI
   - Files are available for download

### 3. Feature Testing

Test each Phase 3 feature:

- **Duplicate Detection**: Upload the same file twice
- **Dataset Detection**: Upload different file types
- **Job Management**: Check retry logic with failed jobs
- **Advanced UI**: Verify all status indicators work

## Troubleshooting

### Common Issues

1. **Database Connection Failed**
   - Check SUPABASE_URL and SUPABASE_SERVICE_KEY
   - Verify database tables exist
   - Check Supabase project status

2. **Storage Upload Failed**
   - Verify SUPABASE_ANON_KEY
   - Check storage buckets exist and are public
   - Verify file permissions

3. **Job Processing Stuck**
   - Check background worker logs
   - Verify file paths and permissions
   - Check processing script dependencies

### Debug Mode

Add to environment variables:
```
LOG_LEVEL=DEBUG
```

### Fallback Behavior

If any feature fails, the app will:
- **Storage**: Fall back to local file serving
- **Database**: Fall back to filesystem tracking
- **Advanced Features**: Disable gracefully with user notification

## Monitoring

### Key Metrics to Watch

1. **Job Success Rate**: Should be >95%
2. **Processing Time**: Typically 30-60 seconds
3. **Error Rate**: Should be <5%
4. **Storage Usage**: Monitor Supabase storage limits

### Logs

Check Render/Railway logs for:
- Job processing status
- Error messages
- Performance metrics
- Feature enable/disable status

## Rollback Plan

If issues occur, you can:

1. **Disable Features**: Set environment variables to `false`
2. **Rollback to Phase 2**: Change start command to `gunicorn web_app_phase2:app`
3. **Rollback to Phase 1**: Change start command to `gunicorn web_app_phase1:app`
4. **Rollback to Basic**: Change start command to `gunicorn web_app:app`

## Success Criteria

✅ **Deployment successful** - App starts without errors  
✅ **Health check passes** - All services connected  
✅ **File upload works** - Can upload and process files  
✅ **Dashboard generated** - Interactive charts display  
✅ **Features enabled** - All Phase 3 features working  
✅ **Fallback works** - App works even if cloud services fail  

## Next Steps

After successful deployment:

1. **Monitor performance** for 24-48 hours
2. **Test with real data** and different file types
3. **Optimize settings** based on usage patterns
4. **Scale resources** if needed (upgrade Render plan)
5. **Set up monitoring** and alerting

---

**Phase 3 Status**: 🚀 **Ready for Production Deployment**

The system now provides enterprise-grade features with robust fallback mechanisms. Ready to handle real-world usage!
