import logging
import os
from aws_access.s3_access import S3Access
from buisness_logic.dump_logs import DumpLogs
from buisness_logic.get_expired_objects import ExpiredObjects



def object_access_logging_lambda(event, context):
      logger = logging.getLogger(__name__)
      """
      This function is called by AWS Lambda and logs the object access
      """
      logger.info("Starting to log object access to dynamoDb")
      # Get the bucket name and object key from the event
      bucket = event['Records'][0]['s3']['bucket']['name']      
      key = event['Records'][0]['s3']['object']['key']


      # Get the object data in string format
      s3 = S3Access(bucket_name=bucket)
      s3obj = s3.get_file(file_name=key)

      # Dump logs to dynamoDb
      DumpLogs(raw_logs=s3obj).dump_logs_to_dynamoDb()
      logger.info("Finished")
      return event


def delete_unaccessed_s3objects(event, context):
      logger = logging.getLogger(__name__)
      logger.info("Starting to delete unaccessed objects")
      print(event)
      
      ExpiredObjects().delete_expired_objects()

      logger.info("Finished")
      return event

def delete_unused_s3objects(event, context):
      logger = logging.getLogger(__name__)
      print(event)
      logger.info("Starting to delete unused objects")
      ExpiredObjects().delete_unused_objects()
      logger.info("Finished")
      
      return event




