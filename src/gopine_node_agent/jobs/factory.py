"""
Job Factory

Factory for creating job instances based on job type.
"""

import importlib
import logging
from typing import Dict, Any, Optional, Type

from gopine_node_agent.jobs.base_job import BaseJob
from gopine_node_agent.jobs.ocr_job import OCRJob
from gopine_node_agent.jobs.pdf_parse_job import PDFParseJob

logger = logging.getLogger(__name__)

class JobFactory:
    """
    Factory for creating job instances based on job type.
    """
    
    def __init__(self):
        """Initialize the job factory."""
        # Map of job types to job classes
        self.job_classes = {
            "ocr": OCRJob,
            "pdf_parse": PDFParseJob
        }
    
    def create_job(
        self,
        job_type: str,
        job_id: str,
        job_data: Dict[str, Any],
        work_dir: str
    ) -> BaseJob:
        """
        Create a job instance based on job type.
        
        Args:
            job_type (str): Type of job to create
            job_id (str): Unique identifier for the job
            job_data (Dict[str, Any]): Job data from the server
            work_dir (str): Working directory for the job
        
        Returns:
            BaseJob: Job instance
            
        Raises:
            ValueError: If job type is unknown
        """
        # Get the job class for this job type
        job_class = self.job_classes.get(job_type)
        
        if not job_class:
            raise ValueError(f"Unknown job type: {job_type}")
        
        # Create and return the job instance
        return job_class(job_id, job_data, work_dir)
    
    def register_job_class(self, job_type: str, job_class: Type[BaseJob]):
        """
        Register a new job class.
        
        Args:
            job_type (str): Job type identifier
            job_class (Type[BaseJob]): Job class
        """
        self.job_classes[job_type] = job_class
        logger.info("Registered job class for type: %s", job_type)
    
    def get_supported_job_types(self) -> list:
        """
        Get list of supported job types.
        
        Returns:
            list: List of supported job types
        """
        return list(self.job_classes.keys())