import subprocess
import time
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class FlinkJobSubmitter:
    """Submit and manage Flink jobs in K8s"""
    
    def __init__(self, namespace="stock-analytics"):
        self.namespace = namespace
        self.jobmanager_pod = None
        
    def get_flink_jobmanager_pod(self):
        """Get Flink JobManager pod name"""
        logger.info("Finding Flink JobManager pod...")
        result = subprocess.run(
            [
                'kubectl', 'get', 'pods', 
                '-n', self.namespace,
                '-l', 'app=flink,component=jobmanager',
                '-o', 'jsonpath={.items[0].metadata.name}'
            ],
            capture_output=True,
            text=True
        )
        
        if result.returncode == 0 and result.stdout:
            self.jobmanager_pod = result.stdout.strip()
            logger.info(f"Found JobManager: {self.jobmanager_pod}")
            return self.jobmanager_pod
        else:
            logger.error("Failed to find Flink JobManager pod")
            return None
    
    def check_flink_status(self):
        """Check if Flink is running"""
        logger.info("Checking Flink cluster status...")
        
        if not self.jobmanager_pod:
            self.get_flink_jobmanager_pod()
        
        if not self.jobmanager_pod:
            return False
        
        # Check pod status
        result = subprocess.run(
            ['kubectl', 'get', 'pod', self.jobmanager_pod, '-n', self.namespace],
            capture_output=True,
            text=True
        )
        
        if 'Running' in result.stdout:
            logger.info("✓ Flink JobManager is running")
            return True
        else:
            logger.error("✗ Flink JobManager is not ready")
            return False
    
    def port_forward_flink_ui(self):
        """Port forward Flink UI for monitoring"""
        logger.info("Setting up port forward to Flink UI...")
        logger.info("Access Flink UI at: http://localhost:8081")
        logger.info("Press Ctrl+C to stop port forwarding")
        
        subprocess.run([
            'kubectl', 'port-forward',
            '-n', self.namespace,
            'svc/flink-jobmanager',
            '8081:8081'
        ])
    
    def copy_job_files_to_pod(self):
        """Copy Flink job files to JobManager pod"""
        logger.info("Copying Flink job files to JobManager pod...")
        
        if not self.jobmanager_pod:
            self.get_flink_jobmanager_pod()
        
        if not self.jobmanager_pod:
            logger.error("Cannot copy files: JobManager pod not found")
            return False
        
        # Copy flink-jobs directory
        result = subprocess.run([
            'kubectl', 'cp',
            '../flink-jobs',
            f'{self.namespace}/{self.jobmanager_pod}:/opt/flink/flink-jobs'
        ], capture_output=True, text=True)
        
        if result.returncode == 0:
            logger.info("✓ Job files copied successfully")
            return True
        else:
            logger.error(f"✗ Failed to copy files: {result.stderr}")
            return False
    
    def install_dependencies_in_pod(self):
        """Check Python availability in Flink pod"""
        logger.info("Checking Python availability in Flink pod...")
        
        if not self.jobmanager_pod:
            self.get_flink_jobmanager_pod()
        
        if not self.jobmanager_pod:
            logger.error("Cannot check dependencies: JobManager pod not found")
            return False
        
        # Check for Python
        result = subprocess.run([
            'kubectl', 'exec',
            self.jobmanager_pod,
            '-n', self.namespace,
            '--',
            'bash', '-c', 'which python3 || which python || echo "none"'
        ], capture_output=True, text=True)
        
        if result.returncode == 0 and 'none' not in result.stdout:
            python_path = result.stdout.strip()
            logger.info(f"✓ Python found: {python_path}")
            
            # Try to install packages
            install_result = subprocess.run([
                'kubectl', 'exec',
                self.jobmanager_pod,
                '-n', self.namespace,
                '--',
                'bash', '-c', f'{python_path} -m pip install kafka-python pymongo'
            ], capture_output=True, text=True)
            
            if install_result.returncode == 0:
                logger.info("✓ Python packages installed successfully")
                return True
            else:
                logger.warning("⚠ Failed to install packages")
                if install_result.stderr:
                    logger.warning(f"  {install_result.stderr.strip()}")
        else:
            logger.warning("✗ Python not found in Flink pod")
            logger.info("\n" + "="*60)
            logger.info("RECOMMENDED APPROACH:")
            logger.info("="*60)
            logger.info("Standard Flink images don't include Python.")
            logger.info("\nFor processing Kafka data, use one of these options:")
            logger.info("\n1. Run Kafka consumer locally (RECOMMENDED FOR TESTING):")
            logger.info("   python kafka_consumer_processor.py")
            logger.info("\n2. Use Java-based Flink jobs")
            logger.info("\n3. Build custom Flink image with Python (for production)")
            logger.info("="*60)
            return False
    
    def submit_pyflink_job(self, job_file, job_name):
        """Submit a PyFlink job to the cluster"""
        logger.info(f"Submitting Flink job: {job_name}...")
        
        # For now, we'll run Python jobs as standalone consumers
        # In production, you'd use PyFlink API
        logger.info(f"Job {job_name} would be submitted here")
        logger.info("Note: For full Flink integration, consider using PyFlink or Java Flink jobs")
        
    def list_running_jobs(self):
        """List all running Flink jobs"""
        logger.info("Listing running Flink jobs...")
        
        if not self.jobmanager_pod:
            self.get_flink_jobmanager_pod()
        
        if not self.jobmanager_pod:
            logger.error("Cannot list jobs: JobManager pod not found")
            return False
        
        result = subprocess.run([
            'kubectl', 'exec',
            self.jobmanager_pod,
            '-n', self.namespace,
            '--',
            'flink', 'list'
        ], capture_output=True, text=True)
        
        if result.returncode == 0:
            logger.info("Running jobs:")
            print(result.stdout)
        else:
            logger.error("Failed to list jobs")


def main():
    """Main function to setup and submit Flink jobs"""
    logger.info("=" * 60)
    logger.info("Flink Cluster Management Tool")
    logger.info("=" * 60)
    
    submitter = FlinkJobSubmitter()
    
    # Check Flink status
    if not submitter.check_flink_status():
        logger.error("Flink is not running. Please start Flink first.")
        return
    
    print("\n" + "="*60)
    print("NOTE: Standard Flink images don't include Python.")
    print("For Kafka data processing, use: python kafka_consumer_processor.py")
    print("="*60)
    
    print("\nFlink Cluster Options:")
    print("1. Check Python availability in Flink pod")
    print("2. Port forward Flink UI (for monitoring)")
    print("3. List running Flink jobs")
    print("4. Exit")
    
    choice = input("\nSelect option (1-4): ").strip()
    
    if choice == '1':
        submitter.install_dependencies_in_pod()
    elif choice == '2':
        submitter.port_forward_flink_ui()
    elif choice == '3':
        submitter.list_running_jobs()
    elif choice == '4':
        logger.info("Exiting...")
    else:
        logger.error("Invalid option")



if __name__ == "__main__":
    main()
