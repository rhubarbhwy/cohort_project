from apify_client import ApifyClient
import json
import boto3
import configparser
from datetime import datetime


# time range
cur_time =  datetime.now().strftime("%Y_%m_%d-%I_%M_%S_%p")
from_date = "2023-08-21"
to_date = "2023-09-01"

# load the aws credentials value
parser  =configparser.ConfigParser()
parser.read("pipeline.conf")
access_key = parser.get("aws_boto_credentials",
                   "access_key")
secret_key = parser.get("aws_boto_credentials",
                   "secret_key")
bucket_name = parser.get("aws_boto_credentials",
                   "bucket_name")
apify_token = parser.get("apify","apify_token")
                
# GET DATA From APIFY
client = ApifyClient(apify_token)
session = boto3.Session(
        aws_access_key_id = access_key,
        aws_secret_access_key= secret_key
    )
s3 = session.client('s3')

competetors_lst = ["lacroixwater","bublywater","perriercanada"]

for brand in competetors_lst:
    print("Get apify data from brand {}, from {} to {}".format(brand,from_date,to_date))
    url = brand 
    # Prepare the Actor input
    run_input = {
        "hashtags": [url],
        "resultsPerPage": 20,
        "shouldDownloadVideos": False,
        "shouldDownloadCovers": False,
        "shouldDownloadSlideshowImages": False,
        "disableEnrichAuthorStats": False,
        "disableCheerioBoost": False,
    }

    # Run the Actor and wait for it to finish
    run = client.actor("clockworks/free-tiktok-scraper").call(run_input=run_input)
    data_variable = client.dataset(run["defaultDatasetId"]).list_items().items
    filtered_data = [item for item in data_variable if from_date <= item.get("createTimeISO") <= to_date]
    payload = {
        "data": filtered_data
    }
    data_length = str(len(payload['data']))
    print("Get {} rows of data".format(data_length))

    # Convert payload to a valid JSON string
    fileName = brand + "-" + from_date + "-" +to_date + "-" + data_length + ".json"
    data = json.dumps(payload).encode('UTF-8')
    object_key = "campaign/" + fileName

    s3.put_object(Body = data, Bucket = bucket_name, Key = object_key)
    print("file upload into s3 {}/{}".format(bucket_name,object_key))

