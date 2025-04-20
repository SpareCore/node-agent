"""
System Information Utilities

Utilities for gathering system information.
"""

import logging
import os
import platform
import socket
import uuid
from typing import Dict, Any

import psutil

logger = logging.getLogger(__name__)

def get_system_info() -> Dict[str, Any]:
    """
    Get detailed system information.
    
    Returns:
        Dict[str, Any]: System information
    """
    try:
        # Basic system info
        system_info = {
            "hostname": socket.gethostname(),
            "machine_id": get_machine_id(),
            "platform": {
                "system": platform.system(),
                "release": platform.release(),
                "version": platform.version(),
                "architecture": platform.machine(),
                "processor": platform.processor()
            },
            "python": {
                "version": platform.python_version(),
                "implementation": platform.python_implementation(),
                "compiler": platform.python_compiler()
            }
        }
        
        # CPU info
        system_info["cpu"] = {
            "physical_cores": psutil.cpu_count(logical=False),
            "logical_cores": psutil.cpu_count(logical=True),
            "frequency_mhz": get_cpu_frequency(),
            "usage_percent": psutil.cpu_percent(interval=0.1)
        }
        
        # Memory info
        mem = psutil.virtual_memory()
        system_info["memory"] = {
            "total_mb": mem.total // (1024 * 1024),
            "available_mb": mem.available // (1024 * 1024),
            "used_mb": mem.used // (1024 * 1024),
            "percent_used": mem.percent
        }
        
        # Disk info
        system_info["disk"] = get_disk_info()
        
        # Network info
        system_info["network"] = {
            "interfaces": get_network_interfaces(),
            "ip_address": get_ip_address()
        }
        
        return system_info
    
    except Exception as e:
        logger.error("Error getting system info: %s", str(e), exc_info=True)
        # Return basic info if full info fails
        return {
            "hostname": socket.gethostname(),
            "platform": platform.system(),
            "error": str(e)
        }

def get_machine_id() -> str:
    """
    Get a unique identifier for this machine.
    
    Returns:
        str: Machine ID
    """
    try:
        if platform.system() == 'Windows':
            # Try to get Windows product ID
            try:
                import winreg
                with winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, 
                                   r"SOFTWARE\Microsoft\Windows NT\CurrentVersion") as key:
                    return winreg.QueryValueEx(key, "ProductId")[0]
            except Exception:
                # Fall back to MAC address
                mac = uuid.getnode()
                return ':'.join(("%012X" % mac)[i:i+2] for i in range(0, 12, 2))
        
        elif platform.system() == 'Linux':
            # Try to get machine ID from /etc/machine-id
            if os.path.isfile('/etc/machine-id'):
                with open('/etc/machine-id', 'r') as f:
                    return f.read().strip()
            
            # Fall back to MAC address
            mac = uuid.getnode()
            return ':'.join(("%012X" % mac)[i:i+2] for i in range(0, 12, 2))
        
        elif platform.system() == 'Darwin':  # macOS
            # Try to get the hardware UUID
            try:
                import subprocess
                result = subprocess.run(['ioreg', '-rd1', '-c', 'IOPlatformExpertDevice'],
                                     stdout=subprocess.PIPE, text=True)
                for line in result.stdout.splitlines():
                    if 'IOPlatformUUID' in line:
                        return line.split('"')[-2]
            except Exception:
                pass
            
            # Fall back to MAC address
            mac = uuid.getnode()
            return ':'.join(("%012X" % mac)[i:i+2] for i in range(0, 12, 2))
        
        # Default fallback
        return str(uuid.getnode())
    
    except Exception as e:
        logger.error("Error getting machine ID: %s", str(e), exc_info=True)
        return str(uuid.uuid4())

def get_cpu_frequency() -> float:
    """
    Get CPU frequency in MHz.
    
    Returns:
        float: CPU frequency
    """
    try:
        cpu_freq = psutil.cpu_freq()
        if cpu_freq and cpu_freq.current:
            return cpu_freq.current
        
        # Fallback for systems where psutil can't get CPU frequency
        if platform.system() == 'Windows':
            import winreg
            with winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, 
                              r"HARDWARE\DESCRIPTION\System\CentralProcessor\0") as key:
                return float(winreg.QueryValueEx(key, "~MHz")[0])
        
        elif platform.system() == 'Linux':
            try:
                with open('/proc/cpuinfo', 'r') as f:
                    for line in f:
                        if line.startswith('cpu MHz') or line.startswith('clock'):
                            return float(line.split(':')[1].strip())
            except Exception:
                pass
        
        # Default fallback
        return 0.0
    
    except Exception as e:
        logger.error("Error getting CPU frequency: %s", str(e), exc_info=True)
        return 0.0

def get_disk_info() -> Dict[str, Any]:
    """
    Get disk information.
    
    Returns:
        Dict[str, Any]: Disk information
    """
    try:
        disks = {}
        
        # Get partitions
        partitions = psutil.disk_partitions()
        
        for partition in partitions:
            try:
                usage = psutil.disk_usage(partition.mountpoint)
                
                disks[partition.mountpoint] = {
                    "device": partition.device,
                    "fstype": partition.fstype,
                    "opts": partition.opts,
                    "total_gb": usage.total / (1024**3),
                    "used_gb": usage.used / (1024**3),
                    "free_gb": usage.free / (1024**3),
                    "percent_used": usage.percent
                }
            except Exception as e:
                logger.debug("Error getting disk info for %s: %s", 
                           partition.mountpoint, str(e))
        
        return disks
    
    except Exception as e:
        logger.error("Error getting disk info: %s", str(e), exc_info=True)
        return {}

def get_network_interfaces() -> Dict[str, Any]:
    """
    Get network interface information.
    
    Returns:
        Dict[str, Any]: Network interface information
    """
    try:
        interfaces = {}
        
        # Get network addresses for each interface
        addrs = psutil.net_if_addrs()
        
        for interface_name, addr_list in addrs.items():
            interfaces[interface_name] = []
            
            for addr in addr_list:
                addr_info = {
                    "family": str(addr.family),
                    "address": addr.address
                }
                
                if addr.netmask:
                    addr_info["netmask"] = addr.netmask
                
                if addr.broadcast:
                    addr_info["broadcast"] = addr.broadcast
                
                interfaces[interface_name].append(addr_info)
        
        return interfaces
    
    except Exception as e:
        logger.error("Error getting network interfaces: %s", str(e), exc_info=True)
        return {}

def get_ip_address() -> str:
    """
    Get the primary IP address of this machine.
    
    Returns:
        str: Primary IP address
    """
    try:
        # This creates a socket but doesn't actually establish a connection
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))  # Google's DNS server
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        # Fallback to hostname resolution
        try:
            host_name = socket.gethostname()
            return socket.gethostbyname(host_name)
        except Exception:
            return "127.0.0.1"  # Localhost if all else fails