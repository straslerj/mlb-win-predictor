import boto3
import datetime
import os
import pickle
import tempfile

now = datetime.datetime.now()
current_time = str(now).replace(" ", "_")[:19].replace(":", "-")

file_path = "model_objects/current_models.pkl"
with open(file_path, "rb") as pickle_file:
    pickle_content = pickle_file.read()

temp = tempfile.NamedTemporaryFile(
    prefix=current_time, suffix="_temp", mode="wb", delete=False
)

temp.write(pickle_content)
temp.flush()

temp.seek(0)

client = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("SECRET_ACCESS_KEY"),
)

s3 = boto3.resource("s3")
key = f"{current_time}-model.pickle"

s3.meta.client.upload_file(
    Filename=temp.name,
    Bucket=os.getenv("AWS_S3_MODEL_SRC"),
    Key=key,
)

print(f"File uploaded to bucket {os.getenv('AWS_S3_MODEL_SRC')} as {key}")

temp.close()
os.remove(temp.name)
