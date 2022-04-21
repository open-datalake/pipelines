from open_connector.events.sns import EmailsS3SNSEvent
from open_connector.pipeline import Pipeline
from source_s3.library import EmailLandingSource
from destination_s3 import OpenS3Destination
import boto3


# Create S3 client
s3 = boto3.client('s3')


def lambda_handler(event, context):

    # Create a source to dest pipeline
    pipeline = Pipeline(
        source=EmailLandingSource(event),
        destination=OpenS3Destination()
    )

    # Execute the pipeline
    pipeline.execute()

    # Remove the landed emails
    for s3_object in EmailsS3SNSEvent(event):
        s3.delete_object(
            Bucket=s3_object.bucket,
            Key=s3_object.key,
        )

    return {'statusCode': 200}
