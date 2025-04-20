"""
Windows Service

Provides functionality to run the Node Agent as a Windows service.
"""

import logging
import os
import sys
import time
from typing import Optional

logger = logging.getLogger(__name__)

# Check if running on Windows
if os.name == 'nt':
    import win32serviceutil
    import win32service
    import win32event
    import servicemanager
    import socket
    import win32timezone
else:
    # Mock classes for non-Windows platforms
    class win32serviceutil:
        @staticmethod
        def ServiceFramework(*args, **kwargs):
            return type('MockServiceFramework', (object,), {})
    
    class servicemanager:
        @staticmethod
        def LogMsg(*args, **kwargs):
            pass
        
        @staticmethod
        def RegisterServiceCtrlHandler(*args, **kwargs):
            pass

# Constants
SERVICE_NAME = "GoPineNodeAgent"
SERVICE_DISPLAY_NAME = "GoPine Node Agent"
SERVICE_DESCRIPTION = "Background processing agent for the GoPine distributed computing system"

class NodeAgentService(win32serviceutil.ServiceFramework):
    """
    Windows service implementation for the GoPine Node Agent.
    """
    
    _svc_name_ = SERVICE_NAME
    _svc_display_name_ = SERVICE_DISPLAY_NAME
    _svc_description_ = SERVICE_DESCRIPTION
    
    def __init__(self, args):
        """
        Initialize the service.
        
        Args:
            args: Service initialization arguments
        """
        try:
            if os.name == 'nt':
                win32serviceutil.ServiceFramework.__init__(self, args)
                self.stop_event = win32event.CreateEvent(None, 0, 0, None)
            
            self.logger = logging.getLogger(__name__)
            self.agent = None
            self.is_running = False
        except Exception as e:
            if os.name == 'nt':
                servicemanager.LogErrorMsg(str(e))
            raise
    
    def SvcStop(self):
        """Stop the service."""
        try:
            self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
            win32event.SetEvent(self.stop_event)
            self.is_running = False
            
            if self.agent:
                self.logger.info("Stopping Node Agent service...")
                self.agent.stop()
        except Exception as e:
            self.logger.error("Error stopping service: %s", str(e), exc_info=True)
    
    def SvcDoRun(self):
        """Run the service."""
        try:
            servicemanager.LogMsg(
                servicemanager.EVENTLOG_INFORMATION_TYPE,
                servicemanager.PYS_SERVICE_STARTED,
                (self._svc_name_, '')
            )
            
            self.is_running = True
            self.main()
        except Exception as e:
            self.logger.error("Error in service main loop: %s", str(e), exc_info=True)
            servicemanager.LogErrorMsg(str(e))
    
    def main(self):
        """Main service function."""
        self.logger.info("Starting GoPine Node Agent service")
        
        try:
            # Import here to avoid circular imports
            from gopine_node_agent.core.agent import NodeAgent
            from gopine_node_agent.core.logger import setup_logging
            
            # Set up logging
            log_dir = os.path.join(os.environ.get("PROGRAMDATA", "C:\\ProgramData"), "GoPine", "logs")
            os.makedirs(log_dir, exist_ok=True)
            log_file = os.path.join(log_dir, "gopine-node-agent.log")
            setup_logging(level=logging.INFO, log_file=log_file)
            
            # Get config from environment or default location
            config_path = os.environ.get(
                "GOPINE_CONFIG",
                os.path.join(os.environ.get("PROGRAMDATA", "C:\\ProgramData"), "GoPine", "config.yaml")
            )
            
            # Create and start the node agent
            self.agent = NodeAgent(config_path=config_path)
            
            # Run the agent
            self.agent.run()
            
        except Exception as e:
            self.logger.error("Service error: %s", str(e), exc_info=True)
            if os.name == 'nt':
                servicemanager.LogErrorMsg(str(e))

def run_as_service():
    """
    Run the Node Agent as a Windows service.
    
    This function is called when the service is started by the service control manager.
    """
    if os.name != 'nt':
        logger.error("Windows service mode is only available on Windows")
        return 1
    
    try:
        servicemanager.Initialize()
        servicemanager.PrepareToHostSingle(NodeAgentService)
        servicemanager.StartServiceCtrlDispatcher()
        return 0
    except Exception as e:
        logger.error("Error running as service: %s", str(e), exc_info=True)
        return 1

def install_service():
    """
    Install the Node Agent as a Windows service.
    """
    if os.name != 'nt':
        logger.error("Windows service mode is only available on Windows")
        return 1
    
    try:
        # Get the path to the current executable
        exe_path = sys.executable
        
        # If this is a frozen executable (PyInstaller), use sys.argv[0]
        if getattr(sys, 'frozen', False):
            exe_path = sys.argv[0]
        
        logger.info("Installing Windows service: %s", SERVICE_NAME)
        
        # Install the service
        win32serviceutil.InstallService(
            pythonClassString="gopine_node_agent.windows.service.NodeAgentService",
            serviceName=SERVICE_NAME,
            displayName=SERVICE_DISPLAY_NAME,
            description=SERVICE_DESCRIPTION,
            startType=win32service.SERVICE_AUTO_START
        )
        
        logger.info("Service installed successfully")
        print(f"Service '{SERVICE_NAME}' installed successfully")
        
        return 0
    except Exception as e:
        logger.error("Error installing service: %s", str(e), exc_info=True)
        print(f"Error installing service: {str(e)}")
        return 1

def uninstall_service():
    """
    Uninstall the Node Agent Windows service.
    """
    if os.name != 'nt':
        logger.error("Windows service mode is only available on Windows")
        return 1
    
    try:
        logger.info("Uninstalling Windows service: %s", SERVICE_NAME)
        
        # Stop the service if it's running
        try:
            win32serviceutil.StopService(SERVICE_NAME)
        except Exception:
            pass
        
        # Uninstall the service
        win32serviceutil.RemoveService(SERVICE_NAME)
        
        logger.info("Service uninstalled successfully")
        print(f"Service '{SERVICE_NAME}' uninstalled successfully")
        
        return 0
    except Exception as e:
        logger.error("Error uninstalling service: %s", str(e), exc_info=True)
        print(f"Error uninstalling service: {str(e)}")
        return 1