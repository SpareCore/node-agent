#!/usr/bin/env python3
"""
GoPine Node Agent CLI

Command-line interface for the GoPine Node Agent.
"""

import argparse
import logging
import os
import signal
import sys
from typing import Optional

from gopine_node_agent import __version__
from gopine_node_agent.core.agent import NodeAgent
from gopine_node_agent.core.logger import setup_logging
from gopine_node_agent.windows.service import run_as_service, install_service, uninstall_service

logger = logging.getLogger(__name__)

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="GoPine Node Agent - Distributed computing client"
    )
    
    parser.add_argument(
        "--version", action="version", version=f"GoPine Node Agent {__version__}"
    )
    
    parser.add_argument(
        "--config", 
        type=str, 
        help="Path to configuration file"
    )
    
    parser.add_argument(
        "--log-level", 
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="INFO", 
        help="Set logging level"
    )
    
    parser.add_argument(
        "--work-dir", 
        type=str, 
        help="Working directory for temporary files"
    )
    
    # Windows service commands
    service_group = parser.add_argument_group("Windows Service")
    service_group.add_argument(
        "--install-service", 
        action="store_true", 
        help="Install Windows service"
    )
    
    service_group.add_argument(
        "--uninstall-service", 
        action="store_true", 
        help="Uninstall Windows service"
    )
    
    service_group.add_argument(
        "--run-as-service", 
        action="store_true", 
        help="Run as Windows service"
    )
    
    # Server connection
    server_group = parser.add_argument_group("Server Connection")
    server_group.add_argument(
        "--server-url", 
        type=str, 
        help="URL of the GoPine Job Server"
    )
    
    server_group.add_argument(
        "--node-id", 
        type=str, 
        help="Unique ID for this node (if not specified, hostname will be used)"
    )
    
    return parser.parse_args()

def handle_signals():
    """Set up signal handlers for graceful shutdown."""
    def signal_handler(sig, frame):
        logger.info("Received signal %s, shutting down...", sig)
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Windows doesn't have SIGQUIT
    if hasattr(signal, "SIGQUIT"):
        signal.signal(signal.SIGQUIT, signal_handler)

def main():
    """Main entry point for the application."""
    args = parse_args()
    
    # Set up logging
    log_level = getattr(logging, args.log_level)
    setup_logging(log_level)
    
    # Handle Windows service commands
    if args.install_service:
        return install_service()
    
    if args.uninstall_service:
        return uninstall_service()
    
    if args.run_as_service:
        return run_as_service()
    
    # Normal operation as console application
    try:
        logger.info("Starting GoPine Node Agent v%s", __version__)
        handle_signals()
        
        # Create and start the node agent
        agent = NodeAgent(
            config_path=args.config,
            work_dir=args.work_dir,
            server_url=args.server_url,
            node_id=args.node_id
        )
        
        # Run the agent (blocking call)
        agent.run()
        
    except Exception as e:
        logger.critical("Unhandled exception: %s", str(e), exc_info=True)
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())