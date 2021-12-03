import boto3
import os
import sys
import logging
from buisness_logic.util import parse_urlstr

class S3Access:
      def __init__(self, bucket_name):
            self.bucket_name = bucket_name
            self.s3 = boto3.resource('s3',
                                     region_name= "ap-south-1" ,   #os.environ['AWS_REGION'],
                                     aws_access_key_id= 'AKIAYDOLFVU2IN6SOCGC' ,#os.environ['AWSACCESS_KEY_ID'],
                                     aws_secret_access_key= "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"   #os.environ['AWSSECRET_ACCESS_KEY']
                                          )


            self.bucket = self.s3.Bucket(self.bucket_name)
            self.logger=logging.getLogger(__name__)


      #gets the contents of a file in the bucket
      def get_file(self, file_name):
            try:
                  obj = self.bucket.Object(parse_urlstr(file_name))
                  return obj.get()['Body'].read().decode('utf-8')
            except Exception as e:
                  logging.error(e)
                  print(e)
                  return None

      #deletes a list of files in the bucket
      def delete_files(self, file_list):
            try:
                  for file in file_list:
                        file=parse_urlstr(file)
                        self.bucket.Object(parse_urlstr(file)).delete()
                  return True
            except Exception as e:
                  logging.error(e)
                  print(e)
                  return False

      #lists all files in the bucket
      def list_files(self):
            try:
                  return [obj.key for obj in self.bucket.objects.all()]
            except Exception as e:
                  logging.error(e)
                  print(e)
                  return None           
      
      # copy files from one bucket to another
      def copy_files(self, file_names, new_bucket_name):
            try:
                  for file_name in file_names:
                        file_name=parse_urlstr(file_name)
                        try:
                              self.s3.Bucket(new_bucket_name).copy({
                                    'Bucket': self.bucket_name,
                                    'Key': parse_urlstr(file_name)
                              }, file_name)
                              self.logger.info("Copied file: {}".format(file_name))
                        except Exception as e:
                              self.logger.error("Failed to copy file: {}. Error: {}".format(file_name, e))
                  return True
            except Exception as e:
                  self.logger.error(e)
                  print(e)
                  return False
