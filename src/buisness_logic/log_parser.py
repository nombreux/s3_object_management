import json
import os
import logging
import datetime
import pandas as pd
from io import StringIO

class LogParser:
      def __init__(self, logs:str):
            self.raw_logs = logs
            self.logger=logging.getLogger(__name__)
            self.logs_csv=pd.read_csv(StringIO(self.raw_logs),sep=' ',header=None,engine='python') #type: ignore
            self.tokens=self.generate_row_tokens()

      def generate_row_tokens(self):
            tokens:list=[]
            for index,row in self.logs_csv.iterrows():
                  tokens.append(row.tolist())
            
            return tokens

      def get_s3object_info(self):
            s3_objects:list=[]
            for index,row in self.logs_csv.iterrows():
                  dt=datetime.datetime.strptime(row[2].strip('['),'%d/%b/%Y:%H:%M:%S')
                  s3_objects.append({
                        's3bucket':row[1],
                        's3object':row[8],
                        'last_accessed_date': dt.isoformat()
                  })

            if(len(s3_objects)>0):
                  return s3_objects
            else: 
                  return None