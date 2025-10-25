# Phase 2: Database Integration

## Overview

Phase 2 adds PostgreSQL database tracking to the fintech pipeline while maintaining full backward compatibility with local file operations. This provides persistent job tracking, duplicate detection, and output metadata management.

## Key Features

### ✅ Database Integration
- **PostgreSQL tracking** via Supabase REST API (no psycopg2 required)
- **Job lifecycle management** with status tracking
- **Output metadata** with file associations
- **Upload file tracking** with usage statistics

### ✅ Hybrid Architecture
- **Database-first** approach with local fallback
- **Cloud storage** integration with local backup
- **Graceful degradation** when services are unavailable
- **Feature flags** for easy control

### ✅ Enhanced UI
- **Real-time status** showing database and storage connectivity
- **Duplicate detection** with reprocess options
- **Job history** from database or filesystem
- **Cloud/local indicators** for each output

## Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Web App       │    │   Supabase       │    │   Local Files   │
│   (Phase 2)     │    │   Database       │    │   (Fallback)    │
├─────────────────┤    ├──────────────────┤    ├─────────────────┤
│ • Job tracking  │◄──►│ • jobs table     │    │ • outputs/      │
│ • File upload   │    │ • outputs table  │    │ • uploads/      │
│ • Download mgmt │    │ • upload_files   │    │ • Job tracking  │
│ • Health checks │    │   table          │    │   (filesystem)  │
└─────────────────┘    └──────────────────┘    └─────────────────┘
         │                       │
         ▼                       ▼
┌─────────────────┐    ┌──────────────────┐
│   Supabase      │    │   REST API       │
│   Storage       │    │   (No psycopg2)  │
├─────────────────┤    ├──────────────────┤
│ • File uploads  │    │ • HTTP requests  │
│ • Outputs       │    │ • JSON responses │
│ • Signed URLs   │    │ • Error handling │
└─────────────────┘    └──────────────────┘
```

## Files Added/Modified

### New Files
- `supabase_rest_client.py` - REST API client for database operations
- `web_app_phase2.py` - Phase 2 web application with database integration
- `requirements_phase2.txt` - Dependencies for Phase 2
- `test_phase2.py` - Comprehensive test suite
- `render_phase2.yaml` - Deployment configuration
- `README_PHASE2.md` - This documentation

### Modified Files
- Uses existing `supabase_storage_client.py` from Phase 1
- Uses existing `preprocess_upload.py` with file hashing
- Uses existing `process_data_fintech.py` with job_id support
- Uses existing `generate_dashboard.py` with job-specific dashboards

## Database Schema

### Jobs Table
```sql
CREATE TABLE jobs (
    job_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    status VARCHAR(20) NOT NULL DEFAULT 'queued',
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
```

### Outputs Table
```sql
CREATE TABLE outputs (
    output_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    job_id UUID NOT NULL REFERENCES jobs(job_id) ON DELETE CASCADE,
    file_type VARCHAR(20) NOT NULL,
    storage_path VARCHAR(500) NOT NULL,
    file_size BIGINT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
```

### Upload Files Table
```sql
CREATE TABLE upload_files (
    file_hash VARCHAR(64) PRIMARY KEY,
    original_name VARCHAR(255) NOT NULL,
    normalized_path VARCHAR(500),
    first_seen TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    last_used TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    usage_count INTEGER DEFAULT 1
);
```

## Environment Variables

### Required for Full Functionality
```bash
# Supabase Configuration
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_SERVICE_KEY=your-service-key
SUPABASE_ANON_KEY=your-anon-key

# Feature Flags
ENABLE_SUPABASE_STORAGE=true
ENABLE_DATABASE_TRACKING=true

# App Configuration
SECRET_KEY=your-secret-key
```

### Optional
```bash
# Disable features for testing
ENABLE_SUPABASE_STORAGE=false  # Use local storage only
ENABLE_DATABASE_TRACKING=false # Use filesystem tracking only
```

## Local Development

### 1. Install Dependencies
```bash
pip install -r requirements_phase2.txt
```

### 2. Set Environment Variables
```bash
export SUPABASE_URL="https://your-project.supabase.co"
export SUPABASE_SERVICE_KEY="your-service-key"
export SUPABASE_ANON_KEY="your-anon-key"
export ENABLE_SUPABASE_STORAGE="true"
export ENABLE_DATABASE_TRACKING="true"
export SECRET_KEY="dev-secret-key"
```

### 3. Run Tests
```bash
python test_phase2.py
```

### 4. Start Development Server
```bash
python web_app_phase2.py
```

## Deployment

### Render Deployment

1. **Create New Service**
   - Use `render_phase2.yaml` configuration
   - Set environment variables in Render dashboard

2. **Environment Variables**
   ```
   SUPABASE_URL=https://your-project.supabase.co
   SUPABASE_SERVICE_KEY=your-service-key
   SUPABASE_ANON_KEY=your-anon-key
   ENABLE_SUPABASE_STORAGE=true
   ENABLE_DATABASE_TRACKING=true
   SECRET_KEY=auto-generated
   ```

3. **Deploy**
   ```bash
   git add .
   git commit -m "Deploy Phase 2"
   git push origin main
   ```

### Railway Deployment

1. **Create New Project**
   - Connect GitHub repository
   - Use `requirements_phase2.txt`

2. **Set Environment Variables**
   - Same as Render configuration

3. **Deploy**
   - Automatic deployment on git push

## Testing

### Automated Tests
```bash
python test_phase2.py
```

### Manual Testing

1. **Upload Test File**
   - Upload a CSV/Excel file
   - Verify job creation in database
   - Check processing status updates

2. **Duplicate Detection**
   - Upload same file twice
   - Verify duplicate warning
   - Test reprocess functionality

3. **Download/View**
   - Download CT/TUS outputs
   - View dashboard in browser
   - Verify cloud/local indicators

4. **Health Checks**
   - Visit `/health` endpoint
   - Check database connectivity
   - Verify storage status

## Monitoring

### Health Endpoint
```bash
curl https://your-app.onrender.com/health
```

### Database Monitoring
- Check Supabase dashboard for table data
- Monitor job status transitions
- Review error messages and logs

### Storage Monitoring
- Verify file uploads to Supabase Storage
- Check signed URL generation
- Monitor storage usage

## Troubleshooting

### Common Issues

1. **Database Connection Failed**
   - Check `SUPABASE_URL` and `SUPABASE_SERVICE_KEY`
   - Verify database schema is created
   - Check Supabase project status

2. **Storage Upload Failed**
   - Verify `SUPABASE_ANON_KEY`
   - Check storage buckets exist
   - Verify file permissions

3. **Job Processing Stuck**
   - Check background thread logs
   - Verify file paths and permissions
   - Check processing script dependencies

### Fallback Behavior

- **Database unavailable**: Falls back to filesystem tracking
- **Storage unavailable**: Falls back to local file serving
- **Processing fails**: Job marked as failed, error logged
- **Download fails**: Shows 404 error page

## Migration from Phase 1

### Automatic Migration
- Phase 2 is backward compatible
- Existing files continue to work
- New uploads use database tracking

### Manual Migration
```bash
# Backup existing data
cp -r outputs/ outputs_backup/
cp -r uploads/ uploads_backup/

# Deploy Phase 2
# Existing files will be discovered via filesystem fallback
```

## Next Steps (Phase 3)

Phase 2 provides the foundation for Phase 3 advanced features:

- **Enhanced duplicate detection** with confidence scoring
- **Dataset type detection** with multiple strategies
- **Background job processing** with retry logic
- **Real-time progress updates** via WebSocket
- **Advanced error handling** and recovery

## Support

For issues or questions:

1. Check the health endpoint: `/health`
2. Review application logs
3. Test individual components with `test_phase2.py`
4. Verify environment variables and Supabase configuration

## Performance

### Expected Performance
- **Job creation**: < 100ms
- **File upload**: Depends on file size and network
- **Processing**: Same as Phase 1 (unchanged)
- **Download**: < 200ms (cloud) or < 50ms (local)

### Scaling Considerations
- Database can handle thousands of jobs
- Storage scales with Supabase limits
- Background processing is single-threaded
- Consider job queue for high volume

---

**Phase 2 Status**: ✅ Complete and ready for deployment
**Next Phase**: Phase 3 - Advanced Features
