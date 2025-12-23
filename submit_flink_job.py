import os
import sys
import json
import requests
import argparse
import logging
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

from config.kafka_config import FlinkConfig

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def check_flink_cluster(jobmanager_url):
    """Check if Flink cluster is accessible"""
    try:
        response = requests.get(f"http://{jobmanager_url}/overview", timeout=5)
        if response.status_code == 200:
            data = response.json()
            logger.info("Flink cluster is accessible")
            logger.info(f"   Flink version: {data.get('flink-version', 'unknown')}")
            logger.info(f"   TaskManagers: {data.get('taskmanagers', 0)}")
            logger.info(f"   Slots available: {data.get('slots-available', 0)}")
            return True
        else:
            logger.error(f"Flink cluster returned status: {response.status_code}")
            return False
    except Exception as e:
        logger.error(f"Cannot connect to Flink cluster at {jobmanager_url}: {e}")
        logger.error("")
        logger.error("Please ensure:")
        logger.error("  1. Flink is running on K8s")
        logger.error("  2. Port-forward is active:")
        logger.error("     kubectl port-forward -n stock-analytics svc/flink-jobmanager 8081:8081")
        logger.error("")
        return False


def list_jobs(jobmanager_url):
    """List all running jobs"""
    try:
        response = requests.get(f"http://{jobmanager_url}/jobs", timeout=5)
        if response.status_code == 200:
            data = response.json()
            jobs = data.get('jobs', [])
            
            if not jobs:
                logger.info("No jobs running")
                return []
            
            logger.info(f"Found {len(jobs)} job(s):")
            for job in jobs:
                job_id = job.get('id')
                status = job.get('status')
                
                # Get job details
                detail_response = requests.get(f"http://{jobmanager_url}/jobs/{job_id}", timeout=5)
                if detail_response.status_code == 200:
                    job_detail = detail_response.json()
                    job_name = job_detail.get('name', 'Unknown')
                    logger.info(f"   - {job_name} [{job_id}] - {status}")
                else:
                    logger.info(f"   - [{job_id}] - {status}")
            
            return jobs
        else:
            logger.error(f"Failed to list jobs: {response.status_code}")
            return []
    except Exception as e:
        logger.error(f"Error listing jobs: {e}")
        return []


def upload_jar(jobmanager_url, jar_path):
    """Upload JAR file to Flink cluster"""
    try:
        logger.info(f"📤 Uploading JAR: {jar_path}")
        
        with open(jar_path, 'rb') as f:
            files = {'jarfile': (os.path.basename(jar_path), f, 'application/java-archive')}
            response = requests.post(
                f"http://{jobmanager_url}/jars/upload",
                files=files,
                timeout=60
            )
        
        if response.status_code == 200:
            data = response.json()
            jar_id = data.get('filename', '').split('/')[-1]
            logger.info(f"JAR uploaded successfully: {jar_id}")
            return jar_id
        else:
            logger.error(f"Failed to upload JAR: {response.status_code}")
            logger.error(f"   Response: {response.text}")
            return None
    except Exception as e:
        logger.error(f"Error uploading JAR: {e}")
        return None


def submit_pyflink_job(jobmanager_url, python_file):
    """
    Submit PyFlink job to cluster
    
    Note: PyFlink jobs need to be submitted differently than regular Flink JAR jobs.
    The job will execute locally but connect to the remote cluster.
    """
    logger.info("="*60)
    logger.info("Submitting PyFlink Job to Remote Cluster")
    logger.info("="*60)
    
    # Set environment variable to use remote cluster
    os.environ['FLINK_JOBMANAGER_URL'] = f"http://{jobmanager_url}"
    
    logger.info(f"Python file: {python_file}")
    logger.info(f"JobManager: {jobmanager_url}")
    logger.info("")
    logger.info("Starting PyFlink job...")
    logger.info("   (Job will connect to remote Flink cluster)")
    logger.info("")
    
    # Execute the Python file
    import subprocess
    
    try:
        # Run the Python file which will submit to remote cluster
        process = subprocess.Popen(
            [sys.executable, python_file],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            universal_newlines=True
        )
        
        # Stream output in real-time
        if process.stdout:
            for line in iter(process.stdout.readline, ''):
                if line:
                    print(line, end='')
        
        # Wait for completion
        return_code = process.wait()
        
        if return_code == 0:
            logger.info("")
            logger.info("Job submitted successfully")
            logger.info(f"   View job at: http://{jobmanager_url}")
            return True
        else:
            logger.error(f"Job submission failed with code {return_code}")
            return False
            
    except Exception as e:
        logger.error(f"Error submitting job: {e}")
        import traceback
        traceback.print_exc()
        return False


def cancel_job(jobmanager_url, job_id):
    """Cancel a running job"""
    try:
        logger.info(f"Cancelling job: {job_id}")
        response = requests.patch(f"http://{jobmanager_url}/jobs/{job_id}", timeout=5)
        
        if response.status_code == 202:
            logger.info("Job cancellation requested")
            return True
        else:
            logger.error(f"Failed to cancel job: {response.status_code}")
            return False
    except Exception as e:
        logger.error(f"Error cancelling job: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description='Submit PyFlink job to remote Flink cluster')
    parser.add_argument(
        'action',
        choices=['submit', 'list', 'cancel', 'check'],
        help='Action to perform'
    )
    parser.add_argument(
        '--job-file',
        help='Path to PyFlink job file (for submit action)'
    )
    parser.add_argument(
        '--job-id',
        help='Job ID (for cancel action)'
    )
    parser.add_argument(
        '--jobmanager',
        default=FlinkConfig.JOBMANAGER_URL,
        help=f'Flink JobManager URL (default: {FlinkConfig.JOBMANAGER_URL})'
    )
    
    args = parser.parse_args()
    
    if args.action == 'check':
        check_flink_cluster(args.jobmanager)
    
    elif args.action == 'list':
        if check_flink_cluster(args.jobmanager):
            list_jobs(args.jobmanager)
    
    elif args.action == 'submit':
        if not args.job_file:
            logger.error("--job-file is required for submit action")
            sys.exit(1)
        
        if not os.path.exists(args.job_file):
            logger.error(f"Job file not found: {args.job_file}")
            sys.exit(1)
        
        if check_flink_cluster(args.jobmanager):
            if submit_pyflink_job(args.jobmanager, args.job_file):
                logger.info("")
                logger.info("Job is now running on Flink cluster")
                logger.info(f"   Dashboard: http://{args.jobmanager}")
            else:
                sys.exit(1)
    
    elif args.action == 'cancel':
        if not args.job_id:
            logger.error("--job-id is required for cancel action")
            sys.exit(1)
        
        if check_flink_cluster(args.jobmanager):
            cancel_job(args.jobmanager, args.job_id)


if __name__ == "__main__":
    main()
