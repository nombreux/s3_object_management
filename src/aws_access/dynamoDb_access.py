import boto3
import os
import logging
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key, Attr


class DynamoDbAccess:
      def __init__(self):
            
            self.dynamodb = boto3.resource('dynamodb',aws_access_key_id=os.environ['AWSACCESS_KEY_ID'],
                                          aws_secret_access_key=os.environ['AWSSECRET_ACCESS_KEY'],
                                          region_name=os.environ['AWS_REGION'])
            
            self.logger=logging.getLogger('DynamoDbAccess')
            

      def get_table_access(self,table_name:str):
            try:
                  self.table = self.dynamodb.Table(table_name)
                  return self
            except ClientError as e:
                  logging.error(e.response['Error']['Message']) #type:ignore
                  return None
            except Exception as e:
                  self.logger.error(e)   
                  return None   

      def post_data(self, data):
            try:
                  response = self.table.put_item(
                        Item=data
                  )
                  return response
            except ClientError as e:
                  print(e.response['Error']['Message']) #type:ignore
                  return None
            except Exception as e:
                  self.logger.error(e) 
                  return None

      def query_data(self, key, value):
            try:
                  response = self.table.scan(
                        FilterExpression=Attr(key).lt(value)

                  )
                  return response['Items']
            except ClientError as e:
                  print(e.response['Error']['Message']) #type:ignore
                  return None
            except Exception as e:
                  print(e)
                  self.logger.error(e) 

      #get all the records from the table
      def get_all_data(self):
            try:
                  response = self.table.scan()
                  return response['Items']
            except ClientError as e:
                  print(e.response['Error']['Message']) #type:ignore
            except Exception as e:                  
                  self.logger.error(e) 
                  

