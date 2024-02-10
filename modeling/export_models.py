import boto3
import datetime
import os
import tempfile

MODEL_ACCESS_KEY_ID = os.getenv("MODEL_ACCESS_KEY_ID")
MODEL_SECRET_ACCESS_KEY = os.getenv("MODEL_SECRET_ACCESS_KEY")
LOGS_ENDPOINT_URL = os.getenv("LOGS_ENDPOINT_URL")
MODEL_BUCKET = os.getenv("MODEL_BUCKET")


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

s3 = boto3.resource(
    service_name="s3",
    aws_access_key_id=MODEL_ACCESS_KEY_ID,
    aws_secret_access_key=MODEL_SECRET_ACCESS_KEY,
    endpoint_url=LOGS_ENDPOINT_URL,
)

key = f"{current_time}-model.pickle"
data = open(temp.name, "rb")

s3.Bucket(MODEL_BUCKET).put_object(Key=f"{key}", Body=data)

print(f"File uploaded to bucket {MODEL_BUCKET} as {key}")

temp.close()
os.remove(temp.name)
