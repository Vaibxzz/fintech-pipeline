# Cloud Deployment Guide

This guide explains how to deploy the Fintech Data Pipeline to cloud platforms using Supabase for database and storage.

## Prerequisites

1. **Supabase Account**: Sign up at [supabase.com](https://supabase.com)
2. **Cloud Platform Account**: Choose one of:
   - [Render](https://render.com) (recommended for free tier)
   - [Railway](https://railway.app)
   - [Heroku](https://heroku.com)

## Supabase Setup

### 1. Create Supabase Project

1. Go to [supabase.com](https://supabase.com) and create a new project
2. Wait for the project to be provisioned (usually 2-3 minutes)
3. Note down your project URL and API keys

### 2. Set Up Database Schema

1. Go to the SQL Editor in your Supabase dashboard
2. Copy and paste the contents of `migrations/001_initial_schema.sql`
3. Run the SQL to create the required tables

### 3. Configure Storage Buckets

1. Go to Storage in your Supabase dashboard
2. Create two buckets:
   - `uploads` (private)
   - `outputs` (private)
3. Set up Row Level Security (RLS) policies if needed

### 4. Get Connection Details

From your Supabase project settings, collect:
- Project URL
- Anon key
- Service role key
- Database URL

## Deployment Options

### Option 1: Render (Recommended)

1. **Connect Repository**:
   - Go to [render.com](https://render.com)
   - Connect your GitHub repository
   - Select the repository containing this code

2. **Create Web Service**:
   - Choose "Web Service"
   - Use the following settings:
     - Build Command: `pip install -r requirements.txt`
     - Start Command: `python web_app.py`
     - Environment: Python 3

3. **Set Environment Variables**:
   ```
   SUPABASE_URL=https://your-project.supabase.co
   SUPABASE_KEY=your-anon-key
   SUPABASE_SERVICE_KEY=your-service-role-key
   DATABASE_URL=postgresql://postgres:[password]@db.your-project.supabase.co:5432/postgres
   SECRET_KEY=your-secret-key-here
   PORT=10000
   ```

4. **Deploy**:
   - Click "Create Web Service"
   - Wait for deployment to complete
   - Your app will be available at the provided URL

### Option 2: Railway

1. **Connect Repository**:
   - Go to [railway.app](https://railway.app)
   - Connect your GitHub repository

2. **Deploy**:
   - Railway will automatically detect the Python app
   - Add environment variables in the Railway dashboard
   - Deploy will start automatically

3. **Environment Variables**:
   - Add the same environment variables as Render
   - Railway will provide a public URL

### Option 3: Docker Deployment

1. **Build Docker Image**:
   ```bash
   docker build -f Dockerfile.cloud -t fintech-pipeline .
   ```

2. **Run Container**:
   ```bash
   docker run -p 5000:5000 \
     -e SUPABASE_URL=https://your-project.supabase.co \
     -e SUPABASE_KEY=your-anon-key \
     -e SUPABASE_SERVICE_KEY=your-service-role-key \
     -e DATABASE_URL=postgresql://postgres:[password]@db.your-project.supabase.co:5432/postgres \
     fintech-pipeline
   ```

## Environment Variables

| Variable | Description | Required | Default |
|----------|-------------|----------|---------|
| `SUPABASE_URL` | Your Supabase project URL | Yes | - |
| `SUPABASE_KEY` | Supabase anon key | Yes | - |
| `SUPABASE_SERVICE_KEY` | Supabase service role key | Yes | - |
| `DATABASE_URL` | PostgreSQL connection string | Yes | - |
| `SECRET_KEY` | Flask secret key | Yes | - |
| `PORT` | Port to run the app on | No | 5000 |
| `UPLOAD_FOLDER` | Local upload folder | No | uploads |
| `OUTPUT_FOLDER` | Local output folder | No | outputs |
| `MAX_FILE_SIZE` | Max file size in MB | No | 50 |
| `LOG_LEVEL` | Logging level | No | INFO |

## Post-Deployment

### 1. Test the Application

1. Visit your deployed URL
2. Upload a test CSV file
3. Check that the job is created and processed
4. Verify outputs are generated and downloadable

### 2. Monitor Health

- Health check endpoint: `https://your-app-url/health`
- Check logs for any errors
- Monitor Supabase usage in the dashboard

### 3. Database Management

- Use Supabase dashboard to monitor database usage
- Check storage usage in the Storage section
- Monitor API usage in the API section

## Troubleshooting

### Common Issues

1. **Database Connection Errors**:
   - Verify DATABASE_URL is correct
   - Check if database is accessible from your deployment platform
   - Ensure RLS policies allow access

2. **Storage Upload Errors**:
   - Verify storage buckets exist
   - Check bucket permissions
   - Ensure service role key has storage access

3. **Job Processing Errors**:
   - Check application logs
   - Verify all dependencies are installed
   - Check if background worker is running

### Logs and Debugging

1. **Render**: Check logs in the Render dashboard
2. **Railway**: Use `railway logs` command or dashboard
3. **Docker**: Use `docker logs <container-id>`

### Performance Optimization

1. **Database**:
   - Monitor query performance in Supabase
   - Add indexes for frequently queried columns
   - Use connection pooling

2. **Storage**:
   - Monitor storage usage
   - Implement file cleanup policies
   - Use CDN for static assets

## Security Considerations

1. **Environment Variables**:
   - Never commit secrets to version control
   - Use platform-specific secret management
   - Rotate keys regularly

2. **Database Security**:
   - Use RLS policies for multi-tenancy
   - Limit database access to application only
   - Monitor for suspicious activity

3. **File Upload Security**:
   - Validate file types and sizes
   - Scan uploaded files for malware
   - Implement rate limiting

## Scaling Considerations

1. **Database Scaling**:
   - Upgrade Supabase plan for more resources
   - Implement read replicas for heavy read workloads
   - Use connection pooling

2. **Application Scaling**:
   - Use multiple worker processes
   - Implement horizontal scaling
   - Use load balancers

3. **Storage Scaling**:
   - Monitor storage usage
   - Implement file lifecycle policies
   - Use CDN for global distribution

## Cost Optimization

1. **Supabase Free Tier Limits**:
   - 500MB database storage
   - 1GB file storage
   - 50,000 monthly active users
   - 2GB bandwidth

2. **Optimization Strategies**:
   - Implement file cleanup policies
   - Use efficient database queries
   - Monitor and optimize API usage
   - Compress files before storage

## Support

For issues and questions:
1. Check the application logs
2. Review Supabase documentation
3. Check platform-specific documentation
4. Create an issue in the repository
