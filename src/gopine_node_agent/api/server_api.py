"""
Server API

Handles communication with the GoPine Job Server via HTTP and WebSockets.
"""

import asyncio
import json
import logging
import uuid
from datetime import datetime
from typing import Dict, List, Optional, Any, Union

import requests
import websockets

logger = logging.getLogger(__name__)

class ServerAPI:
    """
    API client for communication with the GoPine Job Server.
    
    Provides methods for:
    - Node registration
    - Heartbeat signals
    - Job requests and status updates
    - Result submission
    """
    
    def __init__(
        self,
        server_url: str,
        websocket_url: str,
        node_id: str,
        connection_timeout: int = 10,
        max_retries: int = 3
    ):
        """
        Initialize the server API client.
        
        Args:
            server_url (str): HTTP URL of the job server
            websocket_url (str): WebSocket URL of the job server
            node_id (str): Unique ID of this node agent
            connection_timeout (int): Connection timeout in seconds
            max_retries (int): Maximum number of retries for failed requests
        """
        self.server_url = server_url
        self.websocket_url = websocket_url
        self.node_id = node_id
        self.connection_timeout = connection_timeout
        self.max_retries = max_retries
        
        # WebSocket connection
        self.ws_connection = None
        self.ws_connected = False
        self.ws_task = None
        
        # Create a session for HTTP requests
        self.session = requests.Session()
        self.session.headers.update({
            'Content-Type': 'application/json',
            'User-Agent': f'GoPine-Node-Agent/{self.node_id}'
        })
    
    async def register_node(self, registration_data: Dict) -> bool:
        """
        Register this node with the server.
        
        Args:
            registration_data (Dict): Node registration data
            
        Returns:
            bool: True if registration was successful, False otherwise
        """
        try:
            endpoint = f"{self.server_url}/api/nodes/register"
            response = await self._http_post(endpoint, registration_data)
            
            if response.status_code == 200:
                logger.info("Node registered successfully")
                
                # Start WebSocket connection after successful registration
                await self._ensure_websocket_connection()
                
                return True
            else:
                logger.error("Node registration failed: %s", response.text)
                return False
                
        except Exception as e:
            logger.error("Error during node registration: %s", str(e), exc_info=True)
            return False
    
    async def send_heartbeat(self, heartbeat_data: Dict) -> bool:
        """
        Send a heartbeat to the server.
        
        Args:
            heartbeat_data (Dict): Heartbeat data
            
        Returns:
            bool: True if heartbeat was successful, False otherwise
        """
        try:
            # Try to send via WebSocket if connected
            if self.ws_connected:
                await self._ws_send(heartbeat_data)
                return True
            
            # Fall back to HTTP
            endpoint = f"{self.server_url}/api/nodes/{self.node_id}/heartbeat"
            response = await self._http_post(endpoint, heartbeat_data)
            
            if response.status_code == 200:
                return True
            else:
                logger.warning("Heartbeat failed: %s", response.text)
                return False
                
        except Exception as e:
            logger.error("Error sending heartbeat: %s", str(e))
            return False
    
    async def request_jobs(self, node_id: str, capacity: int) -> List[Dict]:
        """
        Request jobs from the server.
        
        Args:
            node_id (str): ID of this node
            capacity (int): Number of jobs the node can accept
            
        Returns:
            List[Dict]: List of job assignments, empty list if none available
        """
        try:
            endpoint = f"{self.server_url}/api/jobs/request"
            
            data = {
                "node_id": node_id,
                "capacity": capacity,
                "capabilities": ["ocr", "pdf_parse"]
            }
            
            response = await self._http_post(endpoint, data)
            
            if response.status_code == 200:
                jobs = response.json().get("jobs", [])
                logger.info("Received %d job assignment(s) from server", len(jobs))
                return jobs
            else:
                logger.warning("Failed to request jobs: %s", response.text)
                return []
                
        except Exception as e:
            logger.error("Error requesting jobs: %s", str(e), exc_info=True)
            return []
    
    async def update_job_status(self, job_id: str, status: str, progress: float = 0) -> bool:
        """
        Update the status of a job on the server.
        
        Args:
            job_id (str): ID of the job
            status (str): New status
            progress (float): Progress percentage (0-100)
            
        Returns:
            bool: True if update was successful, False otherwise
        """
        try:
            # Prepare the status update message
            status_data = {
                "message_id": str(uuid.uuid4()),
                "message_type": "job_status_update",
                "timestamp": datetime.utcnow().isoformat(),
                "sender": {
                    "id": self.node_id,
                    "type": "node_agent"
                },
                "payload": {
                    "job_id": job_id,
                    "node_id": self.node_id,
                    "status": status,
                    "progress": progress,
                    "status_message": f"Job {status} at {progress:.1f}%"
                }
            }
            
            # Try to send via WebSocket if connected
            if self.ws_connected:
                await self._ws_send(status_data)
                return True
            
            # Fall back to HTTP
            endpoint = f"{self.server_url}/api/jobs/{job_id}/status"
            response = await self._http_post(endpoint, status_data)
            
            if response.status_code == 200:
                return True
            else:
                logger.warning("Failed to update job status: %s", response.text)
                return False
                
        except Exception as e:
            logger.error("Error updating job status: %s", str(e))
            return False
    
    async def send_job_result(self, job_id: str, result_data: Dict) -> bool:
        """
        Send job result to the server.
        
        Args:
            job_id (str): ID of the job
            result_data (Dict): Job result data
            
        Returns:
            bool: True if result was successfully sent, False otherwise
        """
        try:
            # Prepare the result message
            message = {
                "message_id": str(uuid.uuid4()),
                "message_type": "job_result",
                "timestamp": datetime.utcnow().isoformat(),
                "sender": {
                    "id": self.node_id,
                    "type": "node_agent"
                },
                "payload": {
                    "job_id": job_id,
                    "node_id": self.node_id,
                    "status": result_data.get("status", "completed"),
                    "result": result_data.get("result", {}),
                    "processing_stats": result_data.get("processing_stats", {})
                }
            }
            
            # For large results, adjust the payload size
            if "text_content" in message["payload"].get("result", {}):
                text_content = message["payload"]["result"]["text_content"]
                if text_content and len(text_content) > 10000:
                    # Truncate large text content
                    message["payload"]["result"]["text_content"] = (
                        text_content[:10000] + "... [truncated]"
                    )
            
            # Try to send via WebSocket if connected and if the payload is not too large
            payload_size = len(json.dumps(message))
            if self.ws_connected and payload_size < 1000000:  # 1MB limit
                await self._ws_send(message)
                return True
            
            # Fall back to HTTP
            endpoint = f"{self.server_url}/api/jobs/{job_id}/result"
            response = await self._http_post(endpoint, message)
            
            if response.status_code == 200:
                return True
            else:
                logger.warning("Failed to send job result: %s", response.text)
                return False
                
        except Exception as e:
            logger.error("Error sending job result: %s", str(e), exc_info=True)
            return False
    
    async def _ensure_websocket_connection(self):
        """Ensure there's an active WebSocket connection to the server."""
        if self.ws_connected:
            return
        
        # Start WebSocket connection in the background
        if self.ws_task is None or self.ws_task.done():
            self.ws_task = asyncio.create_task(self._websocket_loop())
    
    async def _websocket_loop(self):
        """Main WebSocket connection loop."""
        logger.info("Starting WebSocket connection to %s", self.websocket_url)
        
        while True:
            try:
                # Connect to WebSocket server
                async with websockets.connect(
                    f"{self.websocket_url}/nodes/{self.node_id}",
                    ping_interval=30,
                    ping_timeout=10,
                    close_timeout=5
                ) as websocket:
                    logger.info("WebSocket connection established")
                    self.ws_connection = websocket
                    self.ws_connected = True
                    
                    # Listen for messages from the server
                    while True:
                        try:
                            message = await websocket.recv()
                            await self._handle_websocket_message(message)
                        except websockets.exceptions.ConnectionClosed:
                            logger.warning("WebSocket connection closed by server")
                            break
            
            except Exception as e:
                logger.error("WebSocket error: %s", str(e), exc_info=True)
            
            # Connection failed or closed, reset state
            self.ws_connection = None
            self.ws_connected = False
            
            # Wait before reconnecting
            logger.info("Reconnecting WebSocket in 10 seconds...")
            await asyncio.sleep(10)
    
    async def _handle_websocket_message(self, message_str: str):
        """
        Handle incoming WebSocket messages from the server.
        
        Args:
            message_str (str): Raw message string
        """
        try:
            message = json.loads(message_str)
            
            message_type = message.get("message_type")
            logger.debug("Received WebSocket message: %s", message_type)
            
            # Handle different message types
            if message_type == "job_assignment":
                # New job assignment
                pass  # This would be handled by the main agent loop
                
            elif message_type == "system_notification":
                # System notification
                notification = message.get("payload", {})
                severity = notification.get("severity", "info")
                msg = notification.get("message", "No message")
                
                if severity == "critical":
                    logger.critical("SYSTEM NOTIFICATION: %s", msg)
                elif severity == "error":
                    logger.error("SYSTEM NOTIFICATION: %s", msg)
                elif severity == "warning":
                    logger.warning("SYSTEM NOTIFICATION: %s", msg)
                else:
                    logger.info("SYSTEM NOTIFICATION: %s", msg)
            
            # Other message types can be added here
            
        except json.JSONDecodeError:
            logger.error("Failed to parse WebSocket message: %s", message_str)
        except Exception as e:
            logger.error("Error handling WebSocket message: %s", str(e), exc_info=True)
    
    async def _ws_send(self, data: Dict):
        """
        Send data via WebSocket connection.
        
        Args:
            data (Dict): Data to send
        """
        if not self.ws_connected or self.ws_connection is None:
            raise ConnectionError("WebSocket connection not established")
        
        await self.ws_connection.send(json.dumps(data))
    
    async def _http_post(self, url: str, data: Dict) -> requests.Response:
        """
        Send HTTP POST request to the server.
        
        Args:
            url (str): Endpoint URL
            data (Dict): Data to send
            
        Returns:
            requests.Response: HTTP response
        """
        # Convert the async execution to run in a separate thread
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None,
            lambda: self.session.post(
                url,
                json=data,
                timeout=self.connection_timeout
            )
        )
    
    async def _http_get(self, url: str) -> requests.Response:
        """
        Send HTTP GET request to the server.
        
        Args:
            url (str): Endpoint URL
            
        Returns:
            requests.Response: HTTP response
        """
        # Convert the async execution to run in a separate thread
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None,
            lambda: self.session.get(
                url,
                timeout=self.connection_timeout
            )
        )