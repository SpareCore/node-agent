"""
Resource Monitor

Monitors system resources (CPU, memory, disk) to ensure the node agent
only accepts jobs when sufficient resources are available.
"""

import logging
import threading
import time
from typing import Dict, Optional

import psutil

logger = logging.getLogger(__name__)

class ResourceMonitor:
    """
    Monitors system resources to control job acceptance.
    
    Tracks:
    - CPU usage
    - Memory usage
    - Disk space
    - Network activity
    
    And determines whether the system has capacity for additional jobs.
    """
    
    def __init__(
        self,
        max_cpu_percent: float = 80.0,
        max_memory_percent: float = 70.0,
        min_free_disk_space_mb: int = 1000,
        check_interval_seconds: int = 5
    ):
        """
        Initialize the resource monitor.
        
        Args:
            max_cpu_percent (float): Maximum CPU usage percentage to accept jobs
            max_memory_percent (float): Maximum memory usage percentage to accept jobs
            min_free_disk_space_mb (int): Minimum free disk space in MB to accept jobs
            check_interval_seconds (int): How often to check resource usage
        """
        self.max_cpu_percent = max_cpu_percent
        self.max_memory_percent = max_memory_percent
        self.min_free_disk_space_mb = min_free_disk_space_mb
        self.check_interval_seconds = check_interval_seconds
        
        # Current resource usage
        self.current_cpu_percent = 0.0
        self.current_memory_percent = 0.0
        self.current_free_disk_space_mb = 0
        
        # Averages (over the last minute)
        self.avg_cpu_percent = 0.0
        self.avg_memory_percent = 0.0
        
        # CPU usage history (for averaging)
        self.cpu_history = []
        self.memory_history = []
        self.history_size = 12  # Last minute (12 * 5 seconds)
        
        # Threading
        self.is_running = False
        self.monitor_thread = None
        self.lock = threading.RLock()
    
    def start(self):
        """Start the resource monitoring thread."""
        if self.is_running:
            return
        
        logger.info("Starting resource monitor (max CPU: %.1f%%, max memory: %.1f%%)",
                  self.max_cpu_percent, self.max_memory_percent)
        
        self.is_running = True
        self.monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self.monitor_thread.start()
    
    def stop(self):
        """Stop the resource monitoring thread."""
        if not self.is_running:
            return
        
        logger.info("Stopping resource monitor...")
        self.is_running = False
        
        if self.monitor_thread:
            self.monitor_thread.join(timeout=5.0)
        
        logger.info("Resource monitor stopped")
    
    def get_current_load(self) -> Dict[str, float]:
        """
        Get the current system load.
        
        Returns:
            Dict[str, float]: Dictionary with current resource usage metrics
        """
        with self.lock:
            return {
                "cpu_percent": self.current_cpu_percent,
                "memory_percent": self.current_memory_percent,
                "free_disk_space_mb": self.current_free_disk_space_mb,
                "available_memory_mb": psutil.virtual_memory().available // (1024 * 1024),
                "avg_cpu_percent": self.avg_cpu_percent,
                "avg_memory_percent": self.avg_memory_percent
            }
    
    def can_accept_jobs(self) -> bool:
        """
        Check if the system has enough resources to accept new jobs.
        
        Returns:
            bool: True if there are sufficient resources, False otherwise
        """
        with self.lock:
            # Check if any resource is over the threshold
            if self.avg_cpu_percent > self.max_cpu_percent:
                logger.debug("CPU usage too high to accept jobs: %.1f%%", self.avg_cpu_percent)
                return False
            
            if self.avg_memory_percent > self.max_memory_percent:
                logger.debug("Memory usage too high to accept jobs: %.1f%%", self.avg_memory_percent)
                return False
            
            if self.current_free_disk_space_mb < self.min_free_disk_space_mb:
                logger.debug("Not enough free disk space to accept jobs: %d MB", 
                           self.current_free_disk_space_mb)
                return False
            
            return True
    
    def _monitor_loop(self):
        """Main monitoring loop that collects system resource metrics."""
        logger.info("Resource monitor thread started")
        
        while self.is_running:
            try:
                # Get CPU usage (as a percentage of all cores)
                cpu_percent = psutil.cpu_percent(interval=0.1)
                
                # Get memory usage
                memory = psutil.virtual_memory()
                memory_percent = memory.percent
                
                # Get disk usage for the system drive
                disk = psutil.disk_usage("/")
                free_disk_space_mb = disk.free // (1024 * 1024)
                
                # Update current values
                with self.lock:
                    self.current_cpu_percent = cpu_percent
                    self.current_memory_percent = memory_percent
                    self.current_free_disk_space_mb = free_disk_space_mb
                    
                    # Update history
                    self.cpu_history.append(cpu_percent)
                    self.memory_history.append(memory_percent)
                    
                    # Keep history to the desired size
                    if len(self.cpu_history) > self.history_size:
                        self.cpu_history.pop(0)
                    if len(self.memory_history) > self.history_size:
                        self.memory_history.pop(0)
                    
                    # Calculate averages
                    self.avg_cpu_percent = sum(self.cpu_history) / len(self.cpu_history)
                    self.avg_memory_percent = sum(self.memory_history) / len(self.memory_history)
                
                # Log if system is under heavy load
                if self.avg_cpu_percent > self.max_cpu_percent:
                    logger.warning("System CPU usage is high: %.1f%%", self.avg_cpu_percent)
                
                if self.avg_memory_percent > self.max_memory_percent:
                    logger.warning("System memory usage is high: %.1f%%", self.avg_memory_percent)
                
                if self.current_free_disk_space_mb < self.min_free_disk_space_mb:
                    logger.warning("System disk space is low: %d MB free", 
                                 self.current_free_disk_space_mb)
                
                # Sleep until next check
                time.sleep(self.check_interval_seconds)
                
            except Exception as e:
                logger.error("Error in resource monitor loop: %s", str(e), exc_info=True)
                time.sleep(self.check_interval_seconds)