#!/usr/bin/env python3
"""
Configuration loader for BigQuery Migration Helper.
Handles loading of environment variables and configuration files.
"""

import json
import os
from typing import Dict, Any, Optional


class ConfigLoader:
    """Loads and manages configuration from environment variables and config files."""

    def __init__(self, config_file="config.json", env_file=".env"):
        self.config_file = config_file
        self.env_file = env_file
        self.config = {}
        self.load_config()

    def load_env_file(self):
        """Load environment variables from .env file if it exists."""
        if os.path.exists(self.env_file):
            try:
                from dotenv import load_dotenv
                load_dotenv(self.env_file)
                print(f"✅ Loaded environment variables from {self.env_file}")
            except ImportError:
                print(f"⚠️ python-dotenv not installed. Install with: pip install python-dotenv")
                print(f"⚠️ Skipping {self.env_file} file loading")
        else:
            print(f"⚠️ No {self.env_file} file found. Using system environment variables only.")

    def load_config(self):
        """Load configuration from JSON file and environment variables."""
        # Load environment variables first
        self.load_env_file()

        # Load JSON config file
        if os.path.exists(self.config_file):
            try:
                with open(self.config_file, 'r') as f:
                    self.config = json.load(f)
                print(f"✅ Loaded configuration from {self.config_file}")
            except json.JSONDecodeError as e:
                print(f"❌ Error loading {self.config_file}: {e}")
                self.config = {}
            except Exception as e:
                print(f"❌ Error reading {self.config_file}: {e}")
                self.config = {}
        else:
            print(f"⚠️ No {self.config_file} file found. Using default values.")
            self.config = self.get_default_config()

        # Override config values with environment variables if present
        self.override_with_env_vars()

    def get_default_config(self) -> Dict[str, Any]:
        """Return default configuration values."""
        return {
            "paths": {
                "local_bucket_dir": "./bucket",
                "fixed_bucket_dir": "./bucket_fixed",
                "schemas_dir": "./schemas"
            },
            "csv": {
                "default_delimiter": ";",
                "file_format": "CSV",
                "skip_leading_rows": "1",
                "write_disposition": "WRITE_TRUNCATE"
            },
            "transfer": {
                "schedule": "every day 00:30",
                "add_test_suffix": True
            }
        }

    def override_with_env_vars(self):
        """Override configuration values with environment variables if present."""
        # Required environment variables that create config sections
        env_mappings = {
            "GCP_PROJECT_ID": ("gcp", "project_id"),
            "BQ_DATASET": ("gcp", "dataset"),
            "TRANSFER_LOCATION": ("gcp", "transfer_location"),
            "S3_BUCKET": ("aws", "s3_bucket"),
        }

        for env_var, (section, key) in env_mappings.items():
            value = os.getenv(env_var)
            if value:
                if section not in self.config:
                    self.config[section] = {}
                self.config[section][key] = value
                print(f"✅ Loaded from environment: {env_var}")

    def get_aws_credentials(self) -> tuple:
        """Get AWS credentials from environment variables."""
        access_key = os.getenv("AWS_ACCESS_KEY_ID")
        secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        
        if not access_key or not secret_key:
            print("⚠️ AWS credentials not found in environment variables.")
            print("⚠️ Make sure AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are set.")
            return None, None
            
        return access_key, secret_key

    def get_gcp_credentials_path(self) -> Optional[str]:
        """Get Google Cloud credentials file path if specified."""
        return os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

    def get(self, section: str, key: str, default: Any = None) -> Any:
        """Get a configuration value by section and key."""
        return self.config.get(section, {}).get(key, default)

    def get_section(self, section: str) -> Dict[str, Any]:
        """Get an entire configuration section."""
        return self.config.get(section, {})

    def get_gcp_project_id(self) -> str:
        """Get GCP project ID."""
        value = self.get("gcp", "project_id")
        if not value:
            raise ValueError(
                "❌ GCP_PROJECT_ID is required! Please set it in your .env file.\n"
                "Example: GCP_PROJECT_ID=your-project-id"
            )
        return value

    def get_bq_dataset(self) -> str:
        """Get BigQuery dataset name."""
        value = self.get("gcp", "dataset")
        if not value:
            raise ValueError(
                "❌ BQ_DATASET is required! Please set it in your .env file.\n"
                "Example: BQ_DATASET=your_dataset_name"
            )
        return value

    def get_transfer_location(self) -> str:
        """Get transfer location."""
        value = self.get("gcp", "transfer_location")
        if not value:
            raise ValueError(
                "❌ TRANSFER_LOCATION is required! Please set it in your .env file.\n"
                "Example: TRANSFER_LOCATION=eu"
            )
        return value

    def get_s3_bucket(self) -> str:
        """Get S3 bucket name."""
        value = self.get("aws", "s3_bucket")
        if not value:
            raise ValueError(
                "❌ S3_BUCKET is required! Please set it in your .env file.\n"
                "Example: S3_BUCKET=your-bucket-name"
            )
        return value

    def get_local_bucket_dir(self) -> str:
        """Get local bucket directory path."""
        return self.get("paths", "local_bucket_dir", "./bucket")

    def get_fixed_bucket_dir(self) -> str:
        """Get fixed bucket directory path."""
        return self.get("paths", "fixed_bucket_dir", "./bucket_fixed")

    def get_schemas_dir(self) -> str:
        """Get schemas directory path."""
        return self.get("paths", "schemas_dir", "./schemas")

    def get_csv_delimiter(self) -> str:
        """Get default CSV delimiter."""
        return self.get("csv", "default_delimiter", ";")

    def get_csv_format(self) -> str:
        """Get CSV file format."""
        return self.get("csv", "file_format", "CSV")

    def get_skip_leading_rows(self) -> str:
        """Get number of rows to skip."""
        return self.get("csv", "skip_leading_rows", "1")

    def get_write_disposition(self) -> str:
        """Get write disposition."""
        return self.get("csv", "write_disposition", "WRITE_TRUNCATE")

    def get_transfer_schedule(self) -> str:
        """Get transfer schedule."""
        return self.get("transfer", "schedule", "every day 00:30")

    def get_add_test_suffix(self) -> bool:
        """Get whether to add test suffix."""
        return self.get("transfer", "add_test_suffix", True)

    def display_config(self):
        """Display current configuration (without sensitive data)."""
        print("\n" + "="*50)
        print("CURRENT CONFIGURATION")
        print("="*50)
        
        # Display config sections from file
        for section, values in self.config.items():
            print(f"\n[{section.upper()}]")
            for key, value in values.items():
                print(f"  {key}: {value}")
        
        # Display environment-based settings
        print(f"\n[ENVIRONMENT SETTINGS]")
        try:
            print(f"  GCP Project ID: {self.get_gcp_project_id()}")
            print(f"  BigQuery Dataset: {self.get_bq_dataset()}")
            print(f"  Transfer Location: {self.get_transfer_location()}")
            print(f"  S3 Bucket: {self.get_s3_bucket()}")
        except ValueError as e:
            print(f"  ⚠️ Configuration incomplete: {str(e).split('!')[0]}")
        
        # Display credential status
        print(f"\n[CREDENTIALS]")
        access_key, secret_key = self.get_aws_credentials()
        if access_key:
            print(f"  AWS Access Key: {access_key[:4]}...{access_key[-4:]}")
            print(f"  AWS Secret Key: {'*' * len(secret_key)}")
        else:
            print(f"  AWS Credentials: ❌ Not configured")
        
        gcp_creds = self.get_gcp_credentials_path()
        if gcp_creds:
            print(f"  GCP Service Account: {gcp_creds}")
        else:
            print(f"  GCP Service Account: Using gcloud auth")
        
        print("="*50)


# Global config instance
config = ConfigLoader()


def get_config() -> ConfigLoader:
    """Get the global configuration instance."""
    return config


if __name__ == "__main__":
    # Test the configuration loader
    config = ConfigLoader()
    config.display_config()