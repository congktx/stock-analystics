"""
Stock Analytics Package
Main entry point for the application
"""

__version__ = "2.0.0"
__author__ = "Stock Analytics Team"

# Import main modules for easy access
from src import crawlers, streaming, storage, core
from config import settings, kafka_config

__all__ = [
    'crawlers',
    'streaming', 
    'storage',
    'core',
    'settings',
    'kafka_config',
]
