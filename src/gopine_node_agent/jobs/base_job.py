"""
Base Job

Base class for all job types in the GoPine node agent.
"""

import abc
import logging
import os
import time
from datetime import datetime
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

class BaseJob(abc.ABC):
    """
    Base class for all job types.
    
    Defines the interface that all job types must implement,
    and provides common functionality.
    """
    
    def __init__(self, job_id: str, job_data: Dict[str, Any], work_dir: str):
        """
        Initialize a job.
        
        Args:
            job_id (str): Unique identifier for the job
            job_data (Dict[str, Any]): Job data from the server
            work_dir (str): Working directory for this job
        """
        self.job_id = job_id
        self.job_data = job_data
        self.work_dir = work_dir
        
        # Create work directory if it doesn't exist
        os.makedirs(self.work_dir, exist_ok=True)
        
        # Basic job information
        self.job_type = job_data.get("job_type")
        self.priority = job_data.get("priority", 5)
        self.timeout_seconds = job_data.get("timeout_seconds", 3600)
        self.parameters = job_data.get("parameters", {})
        
        # Status tracking
        self.start_time = None
        self.end_time = None
        self.progress = 0.0
        self.status = "assigned"
        self.error = None
        
        # Create input and output directories
        self.input_dir = os.path.join(self.work_dir, "input")
        self.output_dir = os.path.join(self.work_dir, "output")
        os.makedirs(self.input_dir, exist_ok=True)
        os.makedirs(self.output_dir, exist_ok=True)
    
    def execute(self) -> Dict[str, Any]:
        """
        Execute the job and return the results.
        
        Returns:
            Dict[str, Any]: Job result data
        """
        logger.info("Starting job %s of type %s", self.job_id, self.job_type)
        self.start_time = datetime.now()
        self.status = "processing"
        
        try:
            # Ensure we have all required parameters
            self._validate_parameters()
            
            # Download input file if needed
            self._prepare_input()
            
            # Execute the actual job implementation
            result = self._process_job()
            
            # Job completed successfully
            self.status = "completed"
            self.progress = 100.0
            
            # Update timing
            self.end_time = datetime.now()
            processing_time = (self.end_time - self.start_time).total_seconds()
            
            # Prepare result data
            result_data = {
                "status": "completed",
                "result": result,
                "processing_stats": {
                    "processing_time_seconds": processing_time,
                    "start_time": self.start_time.isoformat(),
                    "end_time": self.end_time.isoformat()
                }
            }
            
            logger.info("Job %s completed successfully in %.1f seconds", 
                      self.job_id, processing_time)
            
            return result_data
            
        except Exception as e:
            logger.error("Error executing job %s: %s", self.job_id, str(e), exc_info=True)
            
            # Job failed
            self.status = "failed"
            self.error = str(e)
            
            # Update timing
            self.end_time = datetime.now()
            processing_time = (self.end_time - self.start_time).total_seconds()
            
            # Prepare error data
            error_data = {
                "status": "failed",
                "error": {
                    "message": str(e),
                    "details": self._get_error_details()
                },
                "processing_stats": {
                    "processing_time_seconds": processing_time,
                    "start_time": self.start_time.isoformat(),
                    "end_time": self.end_time.isoformat()
                }
            }
            
            return error_data
    
    @abc.abstractmethod
    def _process_job(self) -> Dict[str, Any]:
        """
        Process the job and return the results.
        
        This method must be implemented by all job types.
        
        Returns:
            Dict[str, Any]: Job result data
        """
        pass
    
    def _validate_parameters(self):
        """
        Validate that all required parameters are present.
        
        Raises:
            ValueError: If any required parameter is missing
        """
        # Default implementation checks nothing
        # Subclasses should override this method to check their specific parameters
        pass
    
    def _prepare_input(self):
        """
        Prepare input files for the job.
        
        Downloads or extracts input files as needed.
        """
        input_file = self.parameters.get("input_file")
        if not input_file:
            logger.debug("No input file specified for job %s", self.job_id)
            return
        
        # Handle input file transfer based on schema from job server
        # For now, we'll assume the input file is a local path or URL
        
        # TODO: Implement file transfer from server or URL
        logger.info("Input file preparation would happen here: %s", input_file)
    
    def _get_error_details(self) -> Dict[str, Any]:
        """
        Get detailed error information.
        
        Returns:
            Dict[str, Any]: Detailed error information
        """
        # Basic error details
        return {
            "job_id": self.job_id,
            "job_type": self.job_type,
            "parameters": self.parameters,
            "error_time": datetime.now().isoformat()
        }
    
    def update_progress(self, progress: float):
        """
        Update the job progress.
        
        Args:
            progress (float): Progress percentage (0-100)
        """
        self.progress = max(0.0, min(100.0, progress))
        logger.debug("Job %s progress: %.1f%%", self.job_id, self.progress)