from typing import Dict
import boto3
import datetime
import logging
import os
import sys
import time
from aws_access.dynamoDb_access import DynamoDbAccess
from aws_access.s3_access import S3Access



class ExpiredObjects:
      def __init__(self):
            self.logger = logging.getLogger()
            self.days=os.environ['S3_OBJECT_EXPIRATION_DAYS']
            self.dynamoDbTable = DynamoDbAccess().get_table_access(os.environ['OBJECT_ACCESS_TABLE_NAME'])
            self.expired_object_dict = self.list_expired_objects()
            self.unused_object_dict = self.list_unused_objects()

      def list_expired_objects(self):
            expiary_date = datetime.datetime.now() - datetime.timedelta(days=int(self.days))
            expiary_date = expiary_date.isoformat()  #.strftime('%Y-%m-%d')
            self.logger.info("Expiry date is: {}".format(expiary_date))
            if(self.dynamoDbTable):
                  expired_objects = self.dynamoDbTable.query_data(key='last_access_dt', value=expiary_date)
                  self.logger.info("Expired objects are: {}".format(expired_objects))
                  expired_object_details:Dict[str,list[str]] = {}
            
                  if(expired_objects):
                        for expired_object in expired_objects:
                              try:
                                    expired_object_details[str(expired_object['bucket_name'])].append(str(expired_object['object_key'])) 
                                    #note:bucket_name and object_key are partition and sort keys in dynamoDb table respectively
                              except Exception as e:
                                    expired_object_details[str(expired_object['bucket_name'])] = [str(expired_object['object_key'])]
                        self.logger.info("Expired objects are: {}".format(expired_object_details))           
                        return expired_object_details
                  else:
                        return None  # No expired objects found
            else:
                  self.logger.error("DynamoDb table not found")
                  return None

      def delete_expired_objects(self):
            try:
                  if(self.expired_object_dict):
                        for bucket_name, expired_object_list in self.expired_object_dict.items():
                              s3_access = S3Access(bucket_name)
                              res=s3_access.delete_files(expired_object_list)
                              if(res):
                                    self.logger.info("Deleted {0} from bucket: {1}".format(expired_object_list, bucket_name))
                  else:
                        self.logger.info("No expired objects found")
            except Exception as e:
                  self.logger.error("Error in deleting expired objects: {}".format(e))
                  return None

      def list_unused_objects(self):
            all_logged_objects:Dict[str,list[str]] = {}
            unlogged_s3objects:Dict[str,list[str]] = {}          
            try:
                  if(self.dynamoDbTable):  
                        all_table_data = self.dynamoDbTable.get_all_data()
                        if(all_table_data):
                              for each_object in all_table_data:
                                    try:
                                          all_logged_objects[str(each_object['bucket_name'])].append(str(each_object['object_key']))
                                    except Exception as e:
                                          all_logged_objects[str(each_object['bucket_name'])] = [str(each_object['object_key'])]
                        else:
                              self.logger.error("No data found in dynamoDb table")
                              return None
                  else:
                        self.logger.error("DynamoDb table not found")
                        return None

            except Exception as e:
                  self.logger.error("Error in listing all unused_objects: {}".format(e))
                  return None      

            try:  
                  if(all_logged_objects):
                        for logged_bucket_name, logged_s3object_list in all_logged_objects.items():
                              s3_access = S3Access(bucket_name=logged_bucket_name)
                              s3files=s3_access.list_files()
                              if(s3files and logged_s3object_list):
                                    if(len(logged_s3object_list) < len(s3files)):
                                          unlogged_s3objects[str(logged_bucket_name)] = list(set(logged_s3object_list) - set(s3files))
                                    else:
                                          self.logger.info("No unlogged objects found in the {} bucket".format(logged_bucket_name))
                  else:
                        self.logger.info("No logged data found")
                  return unlogged_s3objects

            except Exception as e:
                  self.logger.error("Error in listing all unused_objects: {}".format(e))
                  return None




            except Exception as e:
                  self.logger.error("Error in getting unused objects: {}".format(e))
                  return None

      def delete_unused_objects(self):
            try:
                  if(self.unused_object_dict):
                        for bucket_name, unused_object_list in self.unused_object_dict.items():
                              s3_access = S3Access(bucket_name)
                              res=s3_access.delete_files(unused_object_list)
                              if(res):
                                    self.logger.info("Deleted {0} from bucket: {1}".format(unused_object_list, bucket_name))
                  else:
                        self.logger.info("No unused objects found")
            except Exception as e:
                  self.logger.error("Error in deleting unused objects: {}".format(e))
                  return None

