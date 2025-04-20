"""
Logging Configuration

Sets up logging for the node agent with colorized console output and file logging.
"""

import logging
import logging.handlers
import os
import sys
from typing import Optional

import colorlog

def setup_logging(
    level: int = logging.INFO,
    log_file: Optional[str] = None,
    max_size_mb: int = 10,
    backup_count: int = 5
):
    """
    Configure logging for the node agent.
    
    Args:
        level (int): Logging level (e.g., logging.INFO)
        log_file (Optional[str]): Path to the log file (None for console only)
        max_size_mb (int): Maximum log file size in MB before rotation
        backup_count (int): Number of backup log files to keep
    """
    # Create root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    
    # Remove any existing handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # Create console handler with colorized output
    console_handler = logging.StreamHandler(stream=sys.stdout)
    console_handler.setLevel(level)
    
    # Define color scheme
    colors = {
        'DEBUG': 'cyan',
        'INFO': 'green',
        'WARNING': 'yellow',
        'ERROR': 'red',
        'CRITICAL': 'bold_red',
    }
    
    # Create formatter
    console_formatter = colorlog.ColoredFormatter(
        '%(log_color)s%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        log_colors=colors
    )
    
    console_handler.setFormatter(console_formatter)
    root_logger.addHandler(console_handler)
    
    # Add file handler if log file is specified
    if log_file:
        try:
            # Create directory for log file if it doesn't exist
            log_dir = os.path.dirname(log_file)
            if log_dir and not os.path.exists(log_dir):
                os.makedirs(log_dir)
            
            # Create rotating file handler
            file_handler = logging.handlers.RotatingFileHandler(
                filename=log_file,
                maxBytes=max_size_mb * 1024 * 1024,  # Convert MB to bytes
                backupCount=backup_count,
                encoding='utf-8'
            )
            
            file_handler.setLevel(level)
            
            # Create formatter (without colors for file)
            file_formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
            
            file_handler.setFormatter(file_formatter)
            root_logger.addHandler(file_handler)
            
            logging.info(f"Logging to file: {log_file}")
        except Exception as e:
            logging.error(f"Failed to set up file logging: {str(e)}")
    
    # Set level for some verbose libraries
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("websockets").setLevel(logging.WARNING)
    
    logging.debug("Logging initialized")
    return root_logger