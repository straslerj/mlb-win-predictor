import boto3
import os
import shutil
import tempfile

from datetime import datetime

"""
This script can be run to upload the latest source code ot the S3 bucket where the lambda function pulls the source code from

"""

current_time = str(datetime.now()).replace(" ", "_")[:19].replace(":", "-")


client = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("SECRET_ACCESS_KEY"),
)

os.system(f"chmod 0755 ../src/lambda_function.py")  # ensures read access

temp = tempfile.NamedTemporaryFile(prefix=current_time, suffix="_temp", mode="w")

shutil.make_archive(temp.name, "zip", "../src/")

s3 = boto3.resource("s3")
s3.meta.client.upload_file(
    Filename=f"{temp.name}.zip",
    Bucket=os.getenv("AWS_S3_SRC_CODE_BUCKET"),
    Key="src-script.zip",
)
