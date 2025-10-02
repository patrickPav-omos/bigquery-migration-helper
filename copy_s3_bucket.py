#!/usr/bin/env python3
import os
import subprocess
import sys
import argparse
from config_loader import get_config

def copy_s3_bucket(s3_bucket=None, 
                  destination_dir=None,
                  aws_access_key_id=None,
                  aws_secret_access_key=None):
    """
    Copy contents of an S3 bucket to a local directory.
    
    Args:
        s3_bucket (str): S3 bucket name (without s3:// prefix)
        destination_dir (str): Local directory to copy files to
        aws_access_key_id (str, optional): AWS access key ID
        aws_secret_access_key (str, optional): AWS secret access key
        
    Returns:
        bool: True if successful, False otherwise
    """
    # Load config and set defaults
    config = get_config()
    if not s3_bucket:
        s3_bucket = config.get_s3_bucket()
    if not destination_dir:
        destination_dir = config.get_local_bucket_dir()
    
    print("\nCopying S3 Bucket Contents")
    print("-------------------------")
    print(f"S3 Bucket: {s3_bucket}")
    print(f"Destination Directory: {destination_dir}")
    
    # Use provided credentials or get from config
    if not aws_access_key_id or not aws_secret_access_key:
        aws_access_key_id, aws_secret_access_key = config.get_aws_credentials()
    
    if aws_access_key_id and aws_secret_access_key:
        print(f"Using AWS credentials: {aws_access_key_id[:4]}...{aws_access_key_id[-4:]}")
        # Set AWS credentials as environment variables
        os.environ["AWS_ACCESS_KEY_ID"] = aws_access_key_id
        os.environ["AWS_SECRET_ACCESS_KEY"] = aws_secret_access_key
    else:
        print("⚠️ No AWS credentials found. Using system default credentials.")
    
    # Create destination directory if it doesn't exist
    os.makedirs(destination_dir, exist_ok=True)
    
    try:
        # Construct the AWS S3 copy command
        s3_uri = f"s3://{s3_bucket}/"
        cmd = ["aws", "s3", "cp", s3_uri, destination_dir, "--recursive"]
        
        print(f"\n🔄 Copying files from {s3_uri} to {destination_dir}...")
        
        # Execute the command
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        
        print(f"✅ Successfully copied files from {s3_uri} to {destination_dir}")
        print(result.stdout)
        return True
        
    except subprocess.CalledProcessError as e:
        print(f"❌ Error copying files: {str(e)}")
        print(f"Error details: {e.stderr}")
        return False
    except Exception as e:
        print(f"❌ Unexpected error: {str(e)}")
        return False

def main():
    """Command line interface for copying S3 bucket contents."""
    # Load config for defaults
    config = get_config()
    
    parser = argparse.ArgumentParser(description="Copy S3 bucket contents to a local directory")
    parser.add_argument("--bucket", default=config.get_s3_bucket(), 
                        help="S3 bucket name (without s3:// prefix)")
    parser.add_argument("--destination", default=config.get_local_bucket_dir(), 
                        help="Local directory to copy files to")
    parser.add_argument("--access-key", help="AWS access key ID")
    parser.add_argument("--secret-key", help="AWS secret access key")
    
    args = parser.parse_args()
    
    return copy_s3_bucket(
        args.bucket, 
        args.destination,
        args.access_key,
        args.secret_key
    )

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
