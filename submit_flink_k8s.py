import os
import sys
import subprocess
import argparse
import logging
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def run_kubectl_command(cmd, check=True):
    """Run kubectl command"""
    try:
        result = subprocess.run(
            cmd,
            shell=True,
            capture_output=True,
            text=True,
            check=check
        )
        return result.stdout.strip(), result.stderr.strip(), result.returncode
    except subprocess.CalledProcessError as e:
        logger.error(f"Command failed: {e}")
        return None, e.stderr, e.returncode


def find_flink_jobmanager(namespace='stock-analytics'):
    """Find Flink JobManager pod"""
    logger.info(f"Looking for Flink JobManager in namespace: {namespace}")
    
    cmd = f"kubectl get pods -n {namespace} -l component=jobmanager -o name"
    stdout, stderr, code = run_kubectl_command(cmd)
    
    if code != 0 or not stdout:
        logger.error(f"Cannot find JobManager pod")
        if stderr:
            logger.error(f"   Error: {stderr}")
        return None
    
    # Get first pod name (remove 'pod/' prefix)
    pod_name = stdout.split('\n')[0].replace('pod/', '') if stdout else None
    if pod_name:
        logger.info(f"Found JobManager pod: {pod_name}")
    return pod_name


def copy_files_to_pod(pod_name, namespace, local_path, remote_path='/opt/flink/pyjob'):
    """Copy Python files and dependencies to Flink pod"""
    logger.info(f"Copying files to pod: {pod_name}")
    
    local_file = Path(local_path)
    if not local_file.exists():
        logger.error(f"File not found: {local_path}")
        return False
    
    project_root = local_file.parent.parent.parent
    
    # Create remote directory
    cmd = f"kubectl exec -n {namespace} {pod_name} -- mkdir -p {remote_path}"
    run_kubectl_command(cmd, check=False)
    
    # Copy main job file
    remote_file = f"{remote_path}/{local_file.name}"
    
    # Use kubectl cp for main file
    cp_cmd = f"kubectl cp {str(local_file)} {namespace}/{pod_name}:{remote_file}"
    logger.info(f"   Copying {local_file.name}...")
    stdout, stderr, code = run_kubectl_command(cp_cmd, check=False)
    
    if code != 0:
        logger.error(f"Failed to copy {local_file.name}")
        if stderr:
            logger.error(f"      {stderr}")
        return None
    
    logger.info(f"{local_file.name}")
    
    # Copy config directory
    config_dir = project_root / 'config'
    if config_dir.exists():
        logger.info(f"   Copying config directory...")
        
        # Create config directory in pod
        cmd = f"kubectl exec -n {namespace} {pod_name} -- mkdir -p {remote_path}/config"
        run_kubectl_command(cmd, check=False)
        
        # Copy all Python files
        for py_file in config_dir.rglob('*.py'):
            rel_path = py_file.relative_to(config_dir)
            remote_config_file = f"{remote_path}/config/{rel_path}"
            
            # Create subdirectories if needed
            remote_dir = str(Path(remote_config_file).parent)
            cmd = f"kubectl exec -n {namespace} {pod_name} -- mkdir -p {remote_dir}"
            run_kubectl_command(cmd, check=False)
            
            # Copy file
            cp_cmd = f"kubectl cp {str(py_file)} {namespace}/{pod_name}:{remote_config_file}"
            stdout, stderr, code = run_kubectl_command(cp_cmd, check=False)
            
            if code == 0:
                logger.info(f"   ✓ config/{rel_path}")
            else:
                logger.warning(f"   ✗ config/{rel_path}")
    
    logger.info(f"Files copied to: {remote_file}")
    return remote_file


def download_kafka_connector_jar(pod_name, namespace):
    """Download Kafka connector JAR inside the pod if not exists"""
    logger.info("Checking Kafka connector JAR...")
    
    jar_path = "/opt/flink/lib/flink-sql-connector-kafka-3.1.0-1.18.jar"
    
    # Check if JAR exists
    cmd = f"kubectl exec -n {namespace} {pod_name} -- test -f {jar_path}"
    stdout, stderr, code = run_kubectl_command(cmd, check=False)
    
    if code == 0:
        logger.info(f"Kafka connector JAR already exists")
        return True
    
    # Download JAR
    logger.info("   Downloading Kafka connector JAR...")
    download_cmd = (
        f"kubectl exec -n {namespace} {pod_name} -- "
        f"wget -q -O {jar_path} "
        f"https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.1.0-1.18/flink-sql-connector-kafka-3.1.0-1.18.jar"
    )
    
    stdout, stderr, code = run_kubectl_command(download_cmd)
    
    if code != 0:
        logger.warning(f"Failed to download JAR, job may fail without it")
        return False
    
    logger.info(f"Kafka connector JAR downloaded")
    return True


def submit_flink_job(pod_name, namespace, remote_file, parallelism=4):
    """Submit Flink job from inside the pod"""
    logger.info("="*60)
    logger.info("Submitting PyFlink job to cluster")
    logger.info("="*60)
    
    # Get the directory containing the job file (for PYTHONPATH)
    # Use POSIX path for Linux container
    from pathlib import PurePosixPath
    remote_dir = str(PurePosixPath(remote_file).parent)
    
    # Submit job using flink run command with -pyfs to set PYTHONPATH
    cmd = (
        f"kubectl exec -n {namespace} {pod_name} -- "
        f"flink run -py {remote_file} -pyfs {remote_dir} -pyexec python3 -d"
    )
    
    logger.info(f"Submitting job: {remote_file}")
    logger.info(f"   Python path: {remote_dir}")
    logger.info(f"   Parallelism: {parallelism}")
    logger.info("")
    
    stdout, stderr, code = run_kubectl_command(cmd)
    
    if code == 0:
        logger.info("Job submitted successfully!")
        if stdout:
            logger.info(f"\n{stdout}")
            
            # Extract job ID from output
            for line in stdout.split('\n'):
                if 'Job has been submitted with JobID' in line:
                    job_id = line.split('JobID')[-1].strip()
                    logger.info("")
                    logger.info(f"Job ID: {job_id}")
                    logger.info(f"View job at: http://localhost:8081/#/job/{job_id}/overview")
        
        return True
    else:
        logger.error("Job submission failed!")
        if stderr:
            logger.error(f"\nError output:\n{stderr}")
        if stdout:
            logger.error(f"\nStdout:\n{stdout}")
        return False


def list_jobs(pod_name, namespace):
    """List running Flink jobs"""
    logger.info("Listing Flink jobs...")
    
    cmd = f"kubectl exec -n {namespace} {pod_name} -- flink list"
    stdout, stderr, code = run_kubectl_command(cmd)
    
    if code == 0:
        print(stdout)
    else:
        logger.error(f"Failed to list jobs: {stderr}")


def cancel_job(pod_name, namespace, job_id):
    """Cancel a running Flink job"""
    logger.info(f"Cancelling job: {job_id}")
    
    cmd = f"kubectl exec -n {namespace} {pod_name} -- flink cancel {job_id}"
    stdout, stderr, code = run_kubectl_command(cmd)
    
    if code == 0:
        logger.info(f"Job cancelled: {job_id}")
        print(stdout)
    else:
        logger.error(f"Failed to cancel job: {stderr}")


def main():
    parser = argparse.ArgumentParser(
        description='Submit PyFlink job to Flink cluster on Kubernetes'
    )
    parser.add_argument(
        'action',
        choices=['submit', 'list', 'cancel'],
        help='Action to perform'
    )
    parser.add_argument(
        '--job-file',
        help='Path to PyFlink job file (for submit action)',
        default='src/streaming/flink_jobs/news_flink_job.py'
    )
    parser.add_argument(
        '--job-id',
        help='Job ID (for cancel action)'
    )
    parser.add_argument(
        '--namespace',
        default='stock-analytics',
        help='Kubernetes namespace (default: stock-analytics)'
    )
    parser.add_argument(
        '--parallelism',
        type=int,
        default=4,
        help='Job parallelism (default: 4)'
    )
    
    args = parser.parse_args()
    
    # Find JobManager pod
    pod_name = find_flink_jobmanager(args.namespace)
    if not pod_name:
        sys.exit(1)
    
    if args.action == 'submit':
        # Download connector JAR if needed
        download_kafka_connector_jar(pod_name, args.namespace)
        
        # Copy files to pod
        remote_file = copy_files_to_pod(
            pod_name, 
            args.namespace, 
            args.job_file
        )
        
        if not remote_file:
            sys.exit(1)
        
        # Submit job
        if submit_flink_job(pod_name, args.namespace, remote_file, args.parallelism):
            logger.info("")
            logger.info("Job is now running on Flink cluster!")
            logger.info("")
            logger.info("To view job status:")
            logger.info(f"  kubectl port-forward -n {args.namespace} svc/flink-jobmanager 8081:8081")
            logger.info("  Then open: http://localhost:8081")
        else:
            sys.exit(1)
    
    elif args.action == 'list':
        list_jobs(pod_name, args.namespace)
    
    elif args.action == 'cancel':
        if not args.job_id:
            logger.error("--job-id is required for cancel action")
            sys.exit(1)
        cancel_job(pod_name, args.namespace, args.job_id)


if __name__ == "__main__":
    main()
