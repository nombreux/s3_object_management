import os
import logging
from log_parser import LogParser
from aws_access.dynamoDb_access import DynamoDbAccess

class DumpLogs:
      def __init__(self, raw_logs):
            self.raw_logs = raw_logs
            self.s3object_logs = LogParser(raw_logs).get_s3object_info()
            self.logger=logging.getLogger(__name__)

      def dump_logs_to_dynamoDb(self):
            dynamoDb_tableAccess = DynamoDbAccess().get_table_access(os.environ['OBJECT_ACCESS_TABLE_NAME'])
            try:
                  if(self.s3object_logs):
                        for s3object in self.s3object_logs:
                              if(dynamoDb_tableAccess):
                                    d=dynamoDb_tableAccess.post_data({
                                          'bucket_name': s3object['s3bucket'],
                                          'object_key': s3object['s3object'],
                                          'last_accessed_date': s3object['last_accessed_date']
                                    })
                                    if(d):
                                          self.logger.info("Successfully dumped logs to dynamoDb")
                                    else:
                                          self.logger.info("Failed to dump logs to dynamoDb")
                              else:
                                    self.logger.info("Failed to get dynamoDb table access")
                  else:
                        self.logger.info("No logs to dump")

            except Exception as e:
                  self.logger.error(e)
                  

