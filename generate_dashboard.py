import pandas as pd
import plotly.express as px
import plotly.io as pio
import argparse
import logging
from pathlib import Path
from database_models import JobRepository, OutputRepository
from storage_manager import storage_manager

logger = logging.getLogger(__name__)

def generate_dashboard_for_job(job_id: str):
    """Generate dashboard for a specific job"""
    try:
        # Get job information
        job = JobRepository.get_job(job_id)
        if not job:
            raise ValueError(f"Job {job_id} not found")
        
        # Get job outputs
        outputs = OutputRepository.get_outputs_by_job(job_id)
        
        # Find CT and TUS analysis files
        ct_output = None
        tus_output = None
        
        for output in outputs:
            if output.file_type == 'CT':
                ct_output = output
            elif output.file_type == 'TUS':
                tus_output = output
        
        if not ct_output or not tus_output:
            raise ValueError(f"Missing CT or TUS analysis files for job {job_id}")
        
        # Download files from storage
        ct_data = storage_manager.download_file("outputs", ct_output.storage_path)
        tus_data = storage_manager.download_file("outputs", tus_output.storage_path)
        
        # Read CSV data
        import io
        ct = pd.read_csv(io.StringIO(ct_data.decode('utf-8')))
        tus = pd.read_csv(io.StringIO(tus_data.decode('utf-8')))
        
        # Prepare data for visualization
        ct_melt = prepare_data(ct, "CT")
        tus_melt = prepare_data(tus, "TUS")
        df_all = pd.concat([ct_melt, tus_melt])
        
        # Create visualization
        fig = px.line(df_all, x="Dates", y="Value", color="PCode", facet_col="Station",
                      title=f"CT and TUS Station Time Series - Job {job_id}", markers=True)
        
        # Generate HTML
        html_content = pio.to_html(fig, full_html=True, include_plotlyjs="cdn")
        
        # Upload dashboard to storage
        dashboard_path = f"outputs/{job_id}/dashboard.html"
        storage_manager.upload_file("outputs", dashboard_path, html_content.encode('utf-8'), "text/html")
        
        # Record dashboard output in database
        OutputRepository.create_output(job_id, "dashboard", dashboard_path, len(html_content))
        
        logger.info(f"Dashboard generated for job {job_id}")
        print(f"✅ Dashboard generated for job {job_id}")
        
        return dashboard_path
        
    except Exception as e:
        logger.error(f"Dashboard generation failed for job {job_id}: {e}")
        raise

def prepare_data(df, station):
    """Prepare data for visualization"""
    cols = [c for c in df.columns if c not in ('Station','Dates','generated_at','pipeline_version','job_id')]
    melt = df.melt(id_vars=['Dates'], value_vars=cols, var_name='PCode', value_name='Value')
    melt['Station'] = station
    melt['Dates'] = pd.to_datetime(melt['Dates'])
    return melt

def generate_static_dashboard():
    """Generate static dashboard for backward compatibility"""
    try:
        out_dir = Path("outputs")
        ct_path = out_dir / "CT_Analysis_Output.csv"
        tus_path = out_dir / "TUS_Analysis_Output.csv"
        
        if not ct_path.exists() or not tus_path.exists():
            raise FileNotFoundError("CT or TUS analysis files not found")
        
        ct = pd.read_csv(ct_path)
        tus = pd.read_csv(tus_path)
        
        ct_melt = prepare_data(ct, "CT")
        tus_melt = prepare_data(tus, "TUS")
        df_all = pd.concat([ct_melt, tus_melt])
        
        fig = px.line(df_all, x="Dates", y="Value", color="PCode", facet_col="Station",
                      title="CT and TUS Station Time Series", markers=True)
        
        pio.write_html(fig, file="outputs/dashboard.html", full_html=True, include_plotlyjs="cdn")
        print("✅ Static dashboard generated at outputs/dashboard.html")
        
    except Exception as e:
        logger.error(f"Static dashboard generation failed: {e}")
        raise

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--job_id', help='Job ID for job-specific dashboard')
    args = parser.parse_args()
    
    if args.job_id:
        generate_dashboard_for_job(args.job_id)
    else:
        generate_static_dashboard()
