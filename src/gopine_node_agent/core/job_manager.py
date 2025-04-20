"""
Job Manager

Manages the execution of jobs on the node, including queuing, processing,
and tracking results.
"""

import asyncio
import json
import logging
import os
import shutil
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from queue import Queue
from typing import Dict, List, Optional, Any

from gopine_node_agent.jobs.factory import JobFactory
from gopine_node_agent.jobs.base_job import BaseJob

logger = logging.getLogger(__name__)

class JobManager:
    """
    Manages job execution on the node.
    
    Responsible for:
    - Maintaining a queue of pending jobs
    - Executing jobs within resource constraints
    - Tracking job progress and results
    - Handling job failures and retries
    - Cleaning up after jobs complete
    """
    
    def __init__(
        self, 
        work_dir: str,
        concurrent_jobs: int = 2,
        cleanup_after_job: bool = True
    ):
        """
        Initialize the job manager.
        
        Args:
            work_dir (str): Directory for job working files
            concurrent_jobs (int): Maximum number of jobs to run simultaneously
            cleanup_after_job (bool): Whether to clean up job files after completion
        """
        self.work_dir = work_dir
        self.concurrent_jobs = concurrent_jobs
        self.cleanup_after_job = cleanup_after_job
        
        # Create work directory if it doesn't exist
        os.makedirs(self.work_dir, exist_ok=True)
        
        # Job tracking
        self.job_queue = Queue()
        self.active_jobs = {}  # job_id -> Job object
        self.completed_jobs = {}  # job_id -> result dict
        
        # Threading
        self.executor = ThreadPoolExecutor(max_workers=self.concurrent_jobs)
        self.job_factory = JobFactory()
        self.is_running = False
        self.worker_thread = None
        self.lock = threading.RLock()
    
    def start(self):
        """Start the job manager worker thread."""
        if self.is_running:
            return
            
        logger.info("Starting job manager (max concurrent jobs: %d)", self.concurrent_jobs)
        self.is_running = True
        self.worker_thread = threading.Thread(target=self._worker_loop, daemon=True)
        self.worker_thread.start()
    
    def stop(self):
        """Stop the job manager and clean up resources."""
        if not self.is_running:
            return
            
        logger.info("Stopping job manager...")
        self.is_running = False
        
        if self.worker_thread:
            self.worker_thread.join(timeout=5.0)
        
        # Shutdown executor
        self.executor.shutdown(wait=True)
        logger.info("Job manager stopped")
        
    async def add_job(self, job_data: Dict):
        """
        Add a job to the processing queue.
        
        Args:
            job_data (Dict): Job data from the server
        """
        job_id = job_data.get("job_id")
        if not job_id:
            logger.error("Cannot add job without job_id")
            return False
        
        logger.info("Adding job %s to queue", job_id)
        
        with self.lock:
            # Check if job is already in our system
            if job_id in self.active_jobs or job_id in self.completed_jobs:
                logger.warning("Job %s is already in the system, ignoring", job_id)
                return False
            
            # Add to queue
            self.job_queue.put(job_data)
            
            # Notify server that we accepted the job
            await self._notify_job_status(job_id, "queued")
            
            return True
        
    def has_capacity(self) -> bool:
        """
        Check if we have capacity to accept more jobs.
        
        Returns:
            bool: True if we can accept more jobs, False otherwise
        """
        with self.lock:
            return len(self.active_jobs) < self.concurrent_jobs
    
    def get_available_capacity(self) -> int:
        """
        Get the number of additional jobs we can accept.
        
        Returns:
            int: Number of job slots available
        """
        with self.lock:
            return max(0, self.concurrent_jobs - len(self.active_jobs))
    
    def active_job_count(self) -> int:
        """
        Get the number of currently active jobs.
        
        Returns:
            int: Number of active jobs
        """
        with self.lock:
            return len(self.active_jobs)
    
    def get_completed_jobs(self) -> Dict[str, Dict]:
        """
        Get all completed jobs that haven't been reported to the server.
        
        Returns:
            Dict[str, Dict]: Dictionary mapping job_id to job result
        """
        with self.lock:
            # Return a copy to avoid modification during iteration
            return dict(self.completed_jobs)
    
    def remove_completed_job(self, job_id: str):
        """
        Remove a completed job after its result has been reported to the server.
        
        Args:
            job_id (str): ID of the job to remove
        """
        with self.lock:
            if job_id in self.completed_jobs:
                del self.completed_jobs[job_id]
                logger.debug("Removed job %s from completed jobs", job_id)
    
    def _worker_loop(self):
        """Main worker loop that processes jobs from the queue."""
        logger.info("Job manager worker thread started")
        
        while self.is_running:
            try:
                # Check if we have capacity for another job
                if not self.has_capacity():
                    time.sleep(1)
                    continue
                
                # Try to get a job from the queue (non-blocking)
                try:
                    job_data = self.job_queue.get(block=False)
                except Exception:
                    # No jobs in queue
                    time.sleep(1)
                    continue
                
                # Process the job
                job_id = job_data.get("job_id")
                if not job_id:
                    logger.error("Job data missing job_id, skipping")
                    self.job_queue.task_done()
                    continue
                
                # Create a job directory
                job_dir = os.path.join(self.work_dir, job_id)
                os.makedirs(job_dir, exist_ok=True)
                
                try:
                    # Create the appropriate job instance
                    job_type = job_data.get("job_type")
                    if not job_type:
                        raise ValueError("Job data missing job_type")
                    
                    job = self.job_factory.create_job(
                        job_type=job_type,
                        job_id=job_id,
                        job_data=job_data,
                        work_dir=job_dir
                    )
                    
                    # Track this job
                    with self.lock:
                        self.active_jobs[job_id] = job
                    
                    # Start the job in the thread pool
                    self.executor.submit(self._execute_job, job)
                    
                except Exception as e:
                    logger.error("Error preparing job %s: %s", job_id, str(e), exc_info=True)
                    # Report failure
                    loop = asyncio.new_event_loop()
                    loop.run_until_complete(self._notify_job_failure(job_id, str(e)))
                    loop.close()
                    
                    # Mark as done in queue
                    self.job_queue.task_done()
                
            except Exception as e:
                logger.error("Error in job manager worker loop: %s", str(e), exc_info=True)
                time.sleep(1)
    
    def _execute_job(self, job: BaseJob):
        """
        Execute a job and handle its result.
        
        Args:
            job: The job instance to execute
        """
        job_id = job.job_id
        logger.info("Starting execution of job %s", job_id)
        
        # Set up event loop for async operations
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            # Notify job start
            loop.run_until_complete(self._notify_job_status(job_id, "processing"))
            
            # Execute the job
            result = job.execute()
            
            # Store the result
            with self.lock:
                self.completed_jobs[job_id] = result
                # Remove from active jobs
                if job_id in self.active_jobs:
                    del self.active_jobs[job_id]
            
            # Notify job completion
            loop.run_until_complete(self._notify_job_status(job_id, "completed"))
            
            logger.info("Job %s completed successfully", job_id)
            
        except Exception as e:
            logger.error("Error executing job %s: %s", job_id, str(e), exc_info=True)
            
            # Remove from active jobs
            with self.lock:
                if job_id in self.active_jobs:
                    del self.active_jobs[job_id]
            
            # Notify job failure
            loop.run_until_complete(self._notify_job_failure(job_id, str(e)))
            
        finally:
            # Clean up
            if self.cleanup_after_job:
                self._cleanup_job_files(job_id)
            
            # Mark as done in queue
            self.job_queue.task_done()
            
            # Close event loop
            loop.close()
    
    def _cleanup_job_files(self, job_id: str):
        """
        Clean up temporary files created for a job.
        
        Args:
            job_id (str): ID of the job to clean up
        """
        job_dir = os.path.join(self.work_dir, job_id)
        if os.path.exists(job_dir):
            try:
                shutil.rmtree(job_dir)
                logger.debug("Cleaned up job directory for %s", job_id)
            except Exception as e:
                logger.warning("Error cleaning up job directory for %s: %s", job_id, str(e))
    
    async def _notify_job_status(self, job_id: str, status: str):
        """
        Notify the server about a job status change.
        
        Args:
            job_id (str): ID of the job
            status (str): New job status
        """
        # This would use the ServerAPI to send a status update
        # For now, we'll just log it
        logger.info("Job %s status changed to: %s", job_id, status)
        
        # In a real implementation, we would send a message to the server
        # Something like: await self.server_api.update_job_status(job_id, status)
    
    async def _notify_job_failure(self, job_id: str, error_message: str):
        """
        Notify the server about a job failure.
        
        Args:
            job_id (str): ID of the job that failed
            error_message (str): Error message
        """
        # This would use the ServerAPI to send a failure notification
        # For now, we'll just log it
        logger.info("Job %s failed: %s", job_id, error_message)
        
        # In a real implementation, we would send a message to the server
        # Something like: await self.server_api.notify_job_failure(job_id, error_message)