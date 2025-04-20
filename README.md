# GoPine Node Agent

## Overview
The Node Agent is a lightweight Windows service that transforms idle desktop computers into powerful nodes in the GoPine distributed computing network. It runs silently in the background, leveraging unused CPU and RAM resources to process computation jobs.

## Purpose
- Harnesses underutilized computing resources on organizational desktop PCs
- Operates as a background service with minimal impact on user experience
- Processes distributed computing tasks during idle times
- Returns completed job results to the central Job Server

## Key Features
- **Silent Background Operation**: Runs as a Windows service without disrupting normal computer use
- **Resource Management**: Intelligently uses available CPU and RAM while respecting system needs
- **Job Processing**: Handles various job types (OCR, PDF parsing, etc.) using local computing power
- **Secure Communication**: Establishes reliable connections with the central Job Server
- **Fault Tolerance**: Handles connection drops and system shutdowns gracefully

## Technical Details
- **Tech Stack**: Python + PyInstaller
- Windows service implementation for seamless operation
- Asynchronous communication with the Job Server
- Local job processing with configurable resource limits
- Integration with Job Schemas for standardized task definitions
- Automatic updates and health reporting

## Supported Job Types
- **OCR (Optical Character Recognition)**: Extract text from images and PDFs
- **PDF Parsing**: Extract text, tables, forms, and metadata from PDF documents

## Installation & Configuration

### Prerequisites
- Python 3.8 or higher
- Required libraries (see requirements.txt)
- Windows operating system

### Installation
1. Clone the repository
2. Install dependencies: `pip install -r requirements.txt`
3. Install as a Windows service: `python -m gopine_node_agent --install-service`

### Configuration
Configuration is done through a YAML file, which can be specified when starting the agent:

```bash
python -m gopine_node_agent --config path/to/config.yaml
```

### Windows Service Management
- Install service: `python -m gopine_node_agent --install-service`
- Uninstall service: `python -m gopine_node_agent --uninstall-service`
- Run as console application: `python -m gopine_node_agent`

## Building with PyInstaller
To create a standalone executable:

```bash
pyinstaller pyinstaller.spec
```

This will create both a standard executable and a Windows service executable in the `dist` directory.

## Security Considerations
- Operates with limited permissions for data safety
- Processes only approved job types
- No access to sensitive system resources
- Data transmission encryption for privacy

## Development

### Project Structure
```
gopine-node-agent/
├── src/
│   └── gopine_node_agent/
│       ├── api/             # Communication with the job server
│       ├── assets/          # Icons and resources
│       ├── config/          # Configuration handling
│       ├── core/            # Core functionality
│       ├── jobs/            # Job type implementations
│       ├── utils/           # Utility functions
│       ├── windows/         # Windows service implementation
│       └── cli.py           # Command-line interface
├── requirements.txt         # Dependencies
├── setup.py                 # Package setup
└── pyinstaller.spec         # PyInstaller configuration
```

### Adding a New Job Type
1. Create a new job class in the `jobs` directory
2. Inherit from `BaseJob` and implement the `_process_job` method
3. Register the job type in the `JobFactory` class

### Customizing Resource Management
The `ResourceMonitor` class can be customized to adjust how the agent decides when to accept new jobs.