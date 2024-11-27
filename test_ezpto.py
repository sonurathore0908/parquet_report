import boto3
from boto3.session import Session
def iam_role(local_path,bucket,keys):
    session = boto3.Session(
        aws_access_key_id='AWS_SESSION_KEY',
        aws_secret_access_key='AWS_SECRET_ACCESS_KEY')
#session = boto3.Session(profile_name="default"))
    sts = session.client("sts")
    response = sts.assume_role(
#        RoleArn="arn:aws:iam::632228229419:role/s3-mfilterit-role",
#        244051943027
        RoleArn="arn:aws:iam::2440519:role/s3-role",
        RoleSessionName="default"
)
    print(response)
 
    new_session = Session(aws_access_key_id=response['Credentials']['AccessKeyId'],
                      aws_secret_access_key=response['Credentials']['SecretAccessKey'],
                      aws_session_token=response['Credentials']['SessionToken'])
 
    s3 = new_session.client("s3")
#with open('test.txt', 'rb') as data:
    op = s3.upload_fileobj(local_path, bucket, keys)
    return op
