#!/usr/bin/env python3
"""
WSGI entry point for Gunicorn
This ensures the scheduler is properly initialized in the worker process
"""

import os
import sys

# Add the current directory to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app import app, initialize_scheduler
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_app():
    """Create and configure the Flask application"""
    # Initialize the scheduler when the app is created
    try:
        initialize_scheduler()
        logger.info("Application created with scheduler initialized")
    except Exception as e:
        logger.error(f"Failed to initialize scheduler: {e}")
    
    return app

# Create the application instance
application = create_app()

if __name__ == "__main__":
    application.run(host='0.0.0.0', port=8080) 