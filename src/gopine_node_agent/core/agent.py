"""
GoPine Node Agent Core

This module contains the main NodeAgent class that coordinates job processing
and communication with the GoPine Job Server.
"""

import asyncio
import json
import logging
import os
import platform
import socket
import time
import uuid
from datetime import datetime
from typing import Dict, List, Optional

import psutil

from gopine_node_agent.api.server_api import ServerAPI
from gopine_node_agent.core.config import Config, load_config
from gopine_node_agent.core.job_manager import JobManager
from gopine_node_agent.core.resource_monitor import ResourceMonitor
from gopine_node_agent.utils.system_info import get_system_info

logger = logging.getLogger(__name__)

class NodeAgent:
    """
    Main agent class for the GoPine distributed computing system.
    
    This class is responsible for:
    - Connecting to the GoPine Job Server
    - Registering the node with the server
    - Monitoring local system resources
    - Requesting and processing jobs
    - Sending heartbeats and status updates to the server
    """
    
    def __init__(
        self,
        config_path: Optional[str] = None,
        work_dir: Optional[str] = None,
        server_url: Optional[str] = None,
        node_id: Optional[str] = None
    ):
        """
        Initialize the Node Agent.
        
        Args:
            config_path (Optional[str]): Path to configuration file
            work_dir (Optional[str]): Working directory for temporary files
            server_url (Optional[str]): URL of the GoPine Job Server
            node_id (Optional[str]): Unique ID for this node (if not specified, hostname will be used)
        """
        self.config = load_config(config_path)
        
        # Override config with command line arguments if provided
        if work_dir:
            self.config.node_agent.job_processing.work_dir = work_dir
        if server_url:
            self.config.node_agent.connection.server_url = server_url
        
        # Ensure work directory exists
        self.work_dir = self.config.node_agent.job_processing.work_dir
        os.makedirs(self.work_dir, exist_ok=True)
        
        # Set up node identity
        self.node_id = node_id or self.config.node_agent.get('node_id') or socket.gethostname()
        self.hostname = socket.gethostname()
        
        # Initialize API connection to server
        self.api = ServerAPI(
            server_url=self.config.node_agent.connection.server_url,
            websocket_url=self.config.node_agent.connection.websocket_url,
            node_id=self.node_id
        )
        
        # Initialize components
        self.resource_monitor = ResourceMonitor(
            max_cpu_percent=self.config.node_agent.resources.max_cpu_percent,
            max_memory_percent=self.config.node_agent.resources.max_memory_percent,
            min_free_disk_space_mb=self.config.node_agent.resources.min_free_disk_space_mb
        )
        
        self.job_manager = JobManager(
            work_dir=self.work_dir,
            concurrent_jobs=self.config.node_agent.resources.concurrent_jobs,
            cleanup_after_job=self.config.node_agent.job_processing.cleanup_after_job
        )
        
        # State tracking
        self.is_running = False
        self.is_registered = False
        self.last_heartbeat_time = 0
        self.system_info = get_system_info()
        
    async def register_with_server(self) -> bool:
        """
        Register this node with the GoPine job server.
        
        Returns:
            bool: True if registration successful, False otherwise
        """
        logger.info("Registering node with server...")
        
        try:
            # Prepare registration data
            registration_data = {
                "message_id": str(uuid.uuid4()),
                "message_type": "node_registration",
                "timestamp": datetime.utcnow().isoformat(),
                "sender": {
                    "id": self.node_id,
                    "type": "node_agent"
                },
                "payload": {
                    "node_id": self.node_id,
                    "hostname": self.hostname,
                    "ip_address": socket.gethostbyname(socket.gethostname()),
                    "version": self.config.node_agent.get("version", "0.1.0"),
                    "capabilities": [
                        "ocr",
                        "pdf_parse"
                    ],
                    "resource_info": {
                        "cpu_cores": psutil.cpu_count(logical=True),
                        "cpu_model": platform.processor(),
                        "total_memory_mb": psutil.virtual_memory().total // (1024 * 1024),
                        "available_memory_mb": psutil.virtual_memory().available // (1024 * 1024),
                        "available_disk_space_mb": psutil.disk_usage(self.work_dir).free // (1024 * 1024),
                        "operating_system": f"{platform.system()} {platform.release()}"
                    },
                    "time_restrictions": {
                        "available_hours": self._get_available_hours()
                    }
                }
            }
            
            # Send registration to server
            success = await self.api.register_node(registration_data)
            
            if success:
                logger.info("Node registered successfully with ID: %s", self.node_id)
                self.is_registered = True
                return True
            else:
                logger.error("Node registration failed")
                return False
                
        except Exception as e:
            logger.error("Error during node registration: %s", str(e), exc_info=True)
            return False
    
    async def send_heartbeat(self) -> bool:
        """
        Send a heartbeat to the server to indicate this node is still active.
        
        Returns:
            bool: True if heartbeat successful, False otherwise
        """
        logger.debug("Sending heartbeat to server...")
        
        try:
            # Get current load and status
            current_load = self.resource_monitor.get_current_load()
            active_jobs = self.job_manager.active_job_count()
            
            # Determine status based on load and jobs
            if active_jobs > 0:
                status = "busy"
            else:
                status = "idle"
            
            # Prepare heartbeat data
            heartbeat_data = {
                "message_id": str(uuid.uuid4()),
                "message_type": "node_heartbeat",
                "timestamp": datetime.utcnow().isoformat(),
                "sender": {
                    "id": self.node_id,
                    "type": "node_agent"
                },
                "payload": {
                    "node_id": self.node_id,
                    "status": status,
                    "current_load": {
                        "cpu_percent": current_load["cpu_percent"],
                        "memory_percent": current_load["memory_percent"],
                        "available_memory_mb": current_load["available_memory_mb"],
                        "active_jobs": active_jobs
                    }
                }
            }
            
            # Send heartbeat to server
            success = await self.api.send_heartbeat(heartbeat_data)
            self.last_heartbeat_time = time.time()
            
            return success
                
        except Exception as e:
            logger.error("Error sending heartbeat: %s", str(e))
            return False
    
    async def request_jobs(self):
        """Request new jobs from the server if capacity is available."""
        # Check if we have capacity for more jobs
        if not self.job_manager.has_capacity():
            logger.debug("No capacity for new jobs, skipping job request")
            return
        
        # Check resource constraints
        if not self.resource_monitor.can_accept_jobs():
            logger.debug("Resource constraints prevent accepting new jobs")
            return
        
        # Check scheduling constraints
        if not self._can_process_jobs_now():
            logger.debug("Outside of scheduled processing hours, not requesting jobs")
            return
        
        try:
            # How many jobs can we accept?
            capacity = self.job_manager.get_available_capacity()
            
            # Request jobs from server
            jobs = await self.api.request_jobs(self.node_id, capacity)
            
            if not jobs:
                logger.debug("No jobs available from server")
                return
                
            # Process each assigned job
            for job in jobs:
                try:
                    logger.info("Received job assignment: %s", job.get("job_id", "unknown"))
                    
                    # Submit job to job manager
                    await self.job_manager.add_job(job)
                    
                except Exception as e:
                    logger.error("Error handling job assignment: %s", str(e), exc_info=True)
                    # TODO: Report job failure to server
                    
        except Exception as e:
            logger.error("Error requesting jobs: %s", str(e), exc_info=True)
    
    async def process_job_results(self):
        """Process completed job results and send them to the server."""
        completed_jobs = self.job_manager.get_completed_jobs()
        
        for job_id, result in completed_jobs.items():
            try:
                # Send result to server
                success = await self.api.send_job_result(job_id, result)
                
                if success:
                    logger.info("Successfully reported result for job %s", job_id)
                    # Remove job from completed queue
                    self.job_manager.remove_completed_job(job_id)
                else:
                    logger.warning("Failed to report result for job %s, will retry later", job_id)
                    
            except Exception as e:
                logger.error("Error reporting job result for %s: %s", job_id, str(e), exc_info=True)
    
    async def main_loop(self):
        """Main agent operation loop."""
        heartbeat_interval = self.config.node_agent.connection.heartbeat_interval_seconds
        
        while self.is_running:
            try:
                # Ensure we're registered
                if not self.is_registered:
                    registered = await self.register_with_server()
                    if not registered:
                        # Wait before retrying
                        await asyncio.sleep(30)
                        continue
                
                # Send heartbeat if it's time
                current_time = time.time()
                if current_time - self.last_heartbeat_time >= heartbeat_interval:
                    await self.send_heartbeat()
                
                # Request new jobs if we have capacity
                await self.request_jobs()
                
                # Process any completed job results
                await self.process_job_results()
                
                # Short sleep to avoid CPU spinning
                await asyncio.sleep(1)
                
            except Exception as e:
                logger.error("Error in main loop: %s", str(e), exc_info=True)
                await asyncio.sleep(5)  # Wait a bit before retrying
    
    def run(self):
        """Run the agent (blocking call)."""
        logger.info("Starting Node Agent with node ID: %s", self.node_id)
        logger.info("Connecting to server: %s", self.config.node_agent.connection.server_url)
        
        self.is_running = True
        
        try:
            # Start resource monitoring
            self.resource_monitor.start()
            
            # Start job manager
            self.job_manager.start()
            
            # Run the main event loop
            asyncio.run(self.main_loop())
            
        except KeyboardInterrupt:
            logger.info("Keyboard interrupt received, shutting down...")
        except Exception as e:
            logger.critical("Unhandled exception: %s", str(e), exc_info=True)
        finally:
            self.is_running = False
            self.job_manager.stop()
            self.resource_monitor.stop()
            logger.info("Node Agent shutdown complete")
    
    def stop(self):
        """Stop the agent gracefully."""
        logger.info("Stopping Node Agent...")
        self.is_running = False
    
    def _get_available_hours(self) -> List[Dict]:
        """
        Get the hours during which this node is available to process jobs.
        
        Returns:
            List[Dict]: List of available time windows
        """
        scheduling = self.config.node_agent.scheduling
        
        # If we're not restricted to working hours, we're always available
        if not scheduling.get("working_hours_only", False):
            return [{"day_of_week": "All", "start_time": "00:00", "end_time": "23:59"}]
        
        # Otherwise, return configured working hours
        working_hours = []
        
        start_time = scheduling.working_hours.get("start", "18:00")
        end_time = scheduling.working_hours.get("end", "08:00")
        
        for day in scheduling.working_days:
            working_hours.append({
                "day_of_week": day,
                "start_time": start_time,
                "end_time": end_time
            })
        
        return working_hours
    
    def _can_process_jobs_now(self) -> bool:
        """
        Check if jobs can be processed based on current time and scheduling config.
        
        Returns:
            bool: True if jobs can be processed now, False otherwise
        """
        scheduling = self.config.node_agent.scheduling
        
        # If we're not restricted to working hours, we can always process jobs
        if not scheduling.get("working_hours_only", False):
            return True
        
        # Get current day and time
        now = datetime.now()
        day_name = now.strftime("%A")  # Monday, Tuesday, etc.
        current_time = now.strftime("%H:%M")  # 24-hour format
        
        # Check if today is a working day
        if day_name not in scheduling.working_days:
            return False
        
        # Parse working hours
        start_time = scheduling.working_hours.get("start", "18:00")
        end_time = scheduling.working_hours.get("end", "08:00")
        
        # Handle overnight hours (end time less than start time)
        if end_time < start_time:
            # We're within bounds if current_time >= start_time OR current_time <= end_time
            return current_time >= start_time or current_time <= end_time
        else:
            # Normal hours: we're within bounds if start_time <= current_time <= end_time
            return start_time <= current_time <= end_time