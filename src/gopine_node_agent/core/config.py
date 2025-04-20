"""
Configuration Management

Handles loading and managing configuration for the node agent.
"""

import logging
import os
import platform
import tempfile
from typing import Dict, Any, Optional

import yaml
from pydantic import BaseModel, Field, field_validator

logger = logging.getLogger(__name__)

class Config:
    """
    Simple dot-notation access dictionary to handle configuration.
    """
    
    def __init__(self, config_dict: Dict[str, Any]):
        """
        Initialize a config object from a dictionary.
        
        Args:
            config_dict (Dict[str, Any]): Configuration dictionary
        """
        for key, value in config_dict.items():
            if isinstance(value, dict):
                # Recursively convert nested dictionaries
                setattr(self, key, Config(value))
            else:
                setattr(self, key, value)
    
    def __getitem__(self, key):
        """Allow dictionary-like access."""
        return getattr(self, key)
    
    def get(self, key, default=None):
        """Get a config value with a default."""
        return getattr(self, key, default)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert the config back to a dictionary."""
        result = {}
        for key, value in self.__dict__.items():
            if isinstance(value, Config):
                result[key] = value.to_dict()
            else:
                result[key] = value
        return result

def get_default_config() -> Dict[str, Any]:
    """
    Get the default configuration for the node agent.
    
    Returns:
        Dict[str, Any]: Default configuration
    """
    # Default working directory location depends on the platform
    if platform.system() == "Windows":
        default_work_dir = os.path.join(tempfile.gettempdir(), "gopine")
    else:
        default_work_dir = "/tmp/gopine"
    
    return {
        "node_agent": {
            "version": "0.1.0",
            "connection": {
                "server_url": "http://localhost:8080",
                "websocket_url": "ws://localhost:8081",
                "reconnect_interval_seconds": 30,
                "heartbeat_interval_seconds": 60,
                "max_reconnect_attempts": 10
            },
            "resources": {
                "max_cpu_percent": 80,
                "max_memory_percent": 70,
                "min_free_disk_space_mb": 1000,
                "concurrent_jobs": 2
            },
            "job_processing": {
                "work_dir": default_work_dir,
                "cleanup_after_job": True,
                "timeout_safety_margin_seconds": 60
            },
            "scheduling": {
                "working_hours_only": False,
                "working_hours": {
                    "start": "18:00",
                    "end": "08:00"
                },
                "working_days": [
                    "Monday",
                    "Tuesday",
                    "Wednesday",
                    "Thursday",
                    "Friday"
                ]
            },
            "logging": {
                "level": "info",
                "file": None,
                "max_size_mb": 10,
                "max_files": 5
            }
        }
    }

def load_config(config_path: Optional[str] = None) -> Config:
    """
    Load the configuration from a file, with fallback to default config.
    
    Args:
        config_path (Optional[str]): Path to configuration file (YAML format)
        
    Returns:
        Config: Configuration object
    """
    # Start with default config
    config_dict = get_default_config()
    
    # Load from config file if provided
    if config_path and os.path.exists(config_path):
        try:
            with open(config_path, 'r') as f:
                user_config = yaml.safe_load(f)
                
            # Merge with default config (user config takes precedence)
            _merge_configs(config_dict, user_config)
            
            logger.info("Loaded configuration from %s", config_path)
        except Exception as e:
            logger.error("Error loading config from %s: %s", config_path, str(e), exc_info=True)
            logger.warning("Using default configuration")
    else:
        if config_path:
            logger.warning("Config file not found at %s, using default configuration", config_path)
        else:
            logger.info("No config file specified, using default configuration")
    
    # Create and return the Config object
    return Config(config_dict)

def _merge_configs(base_config: Dict[str, Any], override_config: Dict[str, Any]):
    """
    Recursively merge two configuration dictionaries.
    
    Args:
        base_config (Dict[str, Any]): Base configuration (will be modified)
        override_config (Dict[str, Any]): Configuration to override with
    """
    for key, value in override_config.items():
        if key in base_config and isinstance(base_config[key], dict) and isinstance(value, dict):
            # Recursively merge nested dictionaries
            _merge_configs(base_config[key], value)
        else:
            # Override or add the key-value pair
            base_config[key] = value