#!/usr/bin/env python3
"""
config.py - Centralized configuration management
"""

import os
from typing import Optional
from dataclasses import dataclass


@dataclass
class SupabaseConfig:
    """Supabase configuration"""
    url: str
    anon_key: str
    service_key: str
    database_url: str


@dataclass
class AppConfig:
    """Application configuration"""
    upload_folder: str
    output_folder: str
    allowed_extensions: set
    max_file_size: int
    supabase: SupabaseConfig


def get_config() -> AppConfig:
    """Get application configuration from environment variables"""
    
    # Supabase configuration
    supabase_url = os.environ.get("SUPABASE_URL")
    if not supabase_url:
        raise ValueError("SUPABASE_URL environment variable is required")
    
    supabase_anon_key = os.environ.get("SUPABASE_KEY")
    if not supabase_anon_key:
        raise ValueError("SUPABASE_KEY environment variable is required")
    
    supabase_service_key = os.environ.get("SUPABASE_SERVICE_KEY")
    if not supabase_service_key:
        raise ValueError("SUPABASE_SERVICE_KEY environment variable is required")
    
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise ValueError("DATABASE_URL environment variable is required")
    
    supabase_config = SupabaseConfig(
        url=supabase_url,
        anon_key=supabase_anon_key,
        service_key=supabase_service_key,
        database_url=database_url
    )
    
    # Application configuration
    return AppConfig(
        upload_folder=os.environ.get("UPLOAD_FOLDER", "uploads"),
        output_folder=os.environ.get("OUTPUT_FOLDER", "outputs"),
        allowed_extensions={".csv", ".xlsx", ".xls"},
        max_file_size=int(os.environ.get("MAX_FILE_SIZE", "50")) * 1024 * 1024,  # 50MB default
        supabase=supabase_config
    )


# Global config instance
config = get_config()
