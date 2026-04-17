import boto3
import os
import json
from botocore.exceptions import ClientError
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class S3CloudStorage:
    def __init__(self, bucket_name, region='us-east-1'):
        """
        Initialize S3 client and bucket name
        
        Args:
            bucket_name (str): Name of the S3 bucket (must be globally unique)
            region (str): AWS region where bucket will be created
        """
        self.bucket_name = bucket_name
        self.region = region
        self.s3_client = boto3.client('s3', region_name=region)
        self.s3_resource = boto3.resource('s3', region_name=region)
        
    def create_bucket(self):
        """Create an S3 bucket"""
        try:
            if self.region == 'us-east-1':
                response = self.s3_client.create_bucket(
                    Bucket=self.bucket_name
                )
            else:
                response = self.s3_client.create_bucket(
                    Bucket=self.bucket_name,
                    CreateBucketConfiguration={'LocationConstraint': self.region}
                )
            
            logger.info(f"✅ Bucket '{self.bucket_name}' created successfully in {self.region}")
            return True
            
        except ClientError as e:
            if e.response['Error']['Code'] == 'BucketAlreadyExists':
                logger.error(f"❌ Bucket '{self.bucket_name}' already exists. Please choose a unique name.")
            elif e.response['Error']['Code'] == 'BucketAlreadyOwnedByYou':
                logger.info(f"ℹ️ Bucket '{self.bucket_name}' already exists and is owned by you.")
                return True
            else:
                logger.error(f"❌ Error creating bucket: {e}")
            return False
    
    def configure_bucket_policy(self):
        """Configure bucket policy for public read access (optional)"""
        bucket_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "PublicReadGetObject",
                    "Effect": "Allow",
                    "Principal": "*",
                    "Action": "s3:GetObject",
                    "Resource": f"arn:aws:s3:::{self.bucket_name}/*",
                    "Condition": {
                        "IpAddress": {
                            "aws:SourceIp": "0.0.0.0/0"
                        }
                    }
                }
            ]
        }
        
        try:
            # Convert policy to JSON string
            policy_string = json.dumps(bucket_policy)
            
            # Set the bucket policy
            self.s3_client.put_bucket_policy(
                Bucket=self.bucket_name,
                Policy=policy_string
            )
            logger.info(f"✅ Bucket policy configured for {self.bucket_name}")
            return True
            
        except ClientError as e:
            logger.error(f"❌ Error configuring bucket policy: {e}")
            return False
    
    def enable_versioning(self):
        """Enable versioning on the bucket"""
        try:
            versioning = self.s3_resource.BucketVersioning(self.bucket_name)
            versioning.enable()
            logger.info(f"✅ Versioning enabled for bucket '{self.bucket_name}'")
            return True
            
        except ClientError as e:
            logger.error(f"❌ Error enabling versioning: {e}")
            return False
    
    def upload_file(self, file_path, object_name=None):
        """
        Upload a file to the S3 bucket
        
        Args:
            file_path (str): Path to the file to upload
            object_name (str): S3 object name (optional)
        """
        if object_name is None:
            object_name = os.path.basename(file_path)
        
        try:
            self.s3_client.upload_file(file_path, self.bucket_name, object_name)
            logger.info(f"✅ File '{file_path}' uploaded as '{object_name}'")
            
            # Generate URL for uploaded file
            url = self.s3_client.generate_presigned_url(
                'get_object',
                Params={'Bucket': self.bucket_name, 'Key': object_name},
                ExpiresIn=3600
            )
            logger.info(f"🔗 Temporary URL (valid for 1 hour): {url}")
            return True
            
        except FileNotFoundError:
            logger.error(f"❌ File '{file_path}' not found")
            return False
        except ClientError as e:
            logger.error(f"❌ Error uploading file: {e}")
            return False
    
    def list_files(self):
        """List all files in the bucket"""
        try:
            response = self.s3_client.list_objects_v2(Bucket=self.bucket_name)
            
            if 'Contents' in response:
                logger.info(f"\n📁 Files in bucket '{self.bucket_name}':")
                for obj in response['Contents']:
                    size_kb = obj['Size'] / 1024
                    logger.info(f"   - {obj['Key']} ({size_kb:.2f} KB) - Last modified: {obj['LastModified']}")
                return response['Contents']
            else:
                logger.info(f"📁 Bucket '{self.bucket_name}' is empty")
                return []
                
        except ClientError as e:
            logger.error(f"❌ Error listing files: {e}")
            return []
    
    def set_bucket_acl(self, acl_type='private'):
        """
        Set bucket ACL (Access Control List)
        
        Args:
            acl_type (str): 'private', 'public-read', 'public-read-write', 'authenticated-read'
        """
        try:
            self.s3_client.put_bucket_acl(
                Bucket=self.bucket_name,
                ACL=acl_type
            )
            logger.info(f"✅ Bucket ACL set to '{acl_type}'")
            return True
            
        except ClientError as e:
            logger.error(f"❌ Error setting bucket ACL: {e}")
            return False
    
    def create_sample_files(self):
        """Create sample files for demonstration"""
        sample_files = [
            ("sample1.txt", "This is sample file 1 for CODTECH internship task."),
            ("sample2.txt", "This is sample file 2 demonstrating cloud storage setup."),
            ("config.json", json.dumps({"task": "cloud_storage", "status": "completed"}))
        ]
        
        created_files = []
        for filename, content in sample_files:
            with open(filename, 'w') as f:
                f.write(content)
            created_files.append(filename)
            logger.info(f"📝 Created sample file: {filename}")
        
        return created_files
    
    def cleanup_sample_files(self, files):
        """Remove sample files from local system"""
        for file in files:
            if os.path.exists(file):
                os.remove(file)
                logger.info(f"🗑️ Removed local file: {file}")


def main():
    """Main function to demonstrate S3 cloud storage setup"""
    
    print("=" * 60)
    print("☁️  AWS S3 CLOUD STORAGE SETUP")
    print("CODTECH Internship Task 1")
    print("=" * 60)
    
    # Configuration
    # IMPORTANT: Replace with your unique bucket name (must be globally unique)
    BUCKET_NAME = "codtech-internship-storage-2024"  # CHANGE THIS TO A UNIQUE NAME
    REGION = "us-east-1"  # You can change this to your preferred region
    
    print(f"\n📋 Configuration:")
    print(f"   Bucket Name: {BUCKET_NAME}")
    print(f"   Region: {REGION}")
    
    # Initialize storage
    storage = S3CloudStorage(BUCKET_NAME, REGION)
    
    # Step 1: Create bucket
    print("\n🔧 Step 1: Creating S3 bucket...")
    if not storage.create_bucket():
        print("❌ Failed to create bucket. Exiting.")
        return
    
    # Step 2: Configure bucket versioning
    print("\n🔧 Step 2: Enabling versioning...")
    storage.enable_versioning()
    
    # Step 3: Set bucket permissions (ACL)
    print("\n🔧 Step 3: Setting bucket permissions...")
    storage.set_bucket_acl('private')
    
    # Step 4: Configure bucket policy (optional - uncomment if needed)
    # print("\n🔧 Step 4: Configuring bucket policy...")
    # storage.configure_bucket_policy()
    
    # Step 5: Create sample files
    print("\n📝 Step 4: Creating sample files...")
    sample_files = storage.create_sample_files()
    
    # Step 6: Upload files to bucket
    print("\n📤 Step 5: Uploading files to S3 bucket...")
    for file in sample_files:
        storage.upload_file(file)
    
    # Step 7: List uploaded files
    print("\n📋 Step 6: Listing uploaded files...")
    storage.list_files()
    
    # Step 8: Cleanup local sample files
    print("\n🧹 Step 7: Cleaning up local files...")
    storage.cleanup_sample_files(sample_files)
    
    # Summary
    print("\n" + "=" * 60)
    print("✅ TASK COMPLETED SUCCESSFULLY!")
    print("=" * 60)
    print(f"\n📊 Summary:")
    print(f"   ✓ Bucket '{BUCKET_NAME}' created and configured")
    print(f"   ✓ Versioning enabled")
    print(f"   ✓ Access permissions configured (private ACL)")
    print(f"   ✓ Sample files uploaded to bucket")
    print(f"\n💡 To view your files in AWS Console:")
    print(f"   https://s3.console.aws.amazon.com/s3/buckets/{BUCKET_NAME}")
    print("\n⚠️  Remember to delete the bucket when no longer needed to avoid charges!")
    print(f"   Run: aws s3 rb s3://{BUCKET_NAME} --force")


if __name__ == "__main__":
    # Prerequisites check
    print("\n📋 PREREQUISITES CHECK:")
    print("   1. AWS CLI configured with credentials (aws configure)")
    print("   2. boto3 library installed (pip install boto3)")
    print("   3. Python 3.6+ installed")
    print("\n" + "=" * 60)
    
    response = input("\nHave you configured AWS credentials? (yes/no): ").lower()
    if response != 'yes':
        print("\n⚠️  Please configure AWS credentials first:")
        print("   1. Install AWS CLI: pip install awscli")
        print("   2. Run: aws configure")
        print("   3. Enter your Access Key, Secret Key, and region")
        print("\nThen run this program again.")
    else:
        main()
