-- Initial database schema for fintech pipeline
-- Supabase PostgreSQL

-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Jobs table - tracks processing jobs
CREATE TABLE jobs (
    job_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    status VARCHAR(20) NOT NULL DEFAULT 'queued' CHECK (status IN ('queued', 'running', 'done', 'failed', 'error')),
    uploaded_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    started_at TIMESTAMP WITH TIME ZONE,
    finished_at TIMESTAMP WITH TIME ZONE,
    file_hash VARCHAR(64) NOT NULL, -- SHA-256 hash
    original_filename VARCHAR(255) NOT NULL,
    dataset_type VARCHAR(50), -- detected or user-specified
    error_msg TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Outputs table - tracks generated files per job
CREATE TABLE outputs (
    output_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    job_id UUID NOT NULL REFERENCES jobs(job_id) ON DELETE CASCADE,
    file_type VARCHAR(20) NOT NULL CHECK (file_type IN ('CT', 'TUS', 'dashboard', 'audit', 'raw')),
    storage_path VARCHAR(500) NOT NULL, -- path in Supabase Storage
    file_size BIGINT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Upload files table - tracks file hashes for deduplication
CREATE TABLE upload_files (
    file_hash VARCHAR(64) PRIMARY KEY, -- SHA-256 hash
    original_name VARCHAR(255) NOT NULL,
    normalized_path VARCHAR(500), -- path to normalized CSV
    first_seen TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    last_used TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    usage_count INTEGER DEFAULT 1
);

-- Indexes for performance
CREATE INDEX idx_jobs_status ON jobs(status);
CREATE INDEX idx_jobs_file_hash ON jobs(file_hash);
CREATE INDEX idx_jobs_uploaded_at ON jobs(uploaded_at);
CREATE INDEX idx_outputs_job_id ON outputs(job_id);
CREATE INDEX idx_outputs_file_type ON outputs(file_type);
CREATE INDEX idx_upload_files_last_used ON upload_files(last_used);

-- Function to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Trigger to auto-update updated_at
CREATE TRIGGER update_jobs_updated_at 
    BEFORE UPDATE ON jobs 
    FOR EACH ROW 
    EXECUTE FUNCTION update_updated_at_column();

-- Row Level Security (RLS) - enable for future multi-tenancy
ALTER TABLE jobs ENABLE ROW LEVEL SECURITY;
ALTER TABLE outputs ENABLE ROW LEVEL SECURITY;
ALTER TABLE upload_files ENABLE ROW LEVEL SECURITY;

-- For now, allow all operations (can be restricted later)
CREATE POLICY "Allow all operations on jobs" ON jobs FOR ALL USING (true);
CREATE POLICY "Allow all operations on outputs" ON outputs FOR ALL USING (true);
CREATE POLICY "Allow all operations on upload_files" ON upload_files FOR ALL USING (true);
