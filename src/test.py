#lambda function to write to the dynamodb table

def lambda_handler(event, context):
      import boto3
      import uuid
      import datetime
      dynamodb = boto3.resource('dynamodb')
      table = dynamodb.Table('test')
      #get the bucket bucket name and object name from the event
      bucket = event['Records'][0]['s3']['bucket']['name']
      key = event['Records'][0]['s3']['object']['key']

      #parsing the s3 server access logs to get the request time and key
      server_access_log = '5e0f7b3b7ce0bdafb9d4a327303c13ec735a2497f6d21a1583cddcd44ebcbd69 qwerty654321 [29/Oct/2021:04:42:09 +0000] 157.40.165.207 5e0f7b3b7ce0bdafb9d4a327303c13ec735a2497f6d21a1583cddcd44ebcbd69 ZPZ1BB10KGR4ZH0X REST.GET.VERSIONING - "GET /qwerty654321?versioning= HTTP/1.1" 200 - 162 - 14 - "-" "S3Console/0.4, aws-internal/3 aws-sdk-java/1.11.1030 Linux/5.4.141-78.230.amzn2int.x86_64 OpenJDK_64-Bit_Server_VM/25.302-b08 java/1.8.0_302 vendor/Oracle_Corporation cfg/retry-mode/standard" - quzXcu2kygMwZnnvg0iCBT2h1IshqcBHImiWTQJmzsuDn8ObLee7LRxRtFYlXpc+AHj5BVMVPkY= SigV4 ECDHE-RSA-AES128-GCM-SHA256 AuthHeader s3.ap-south-1.amazonaws.com TLSv1.2 -'
      request_time = server_access_log.split(' ')[1]
      key_name = server_access_log.split(' ')[3]
      key=server_access_log.split()[6]
      #inserting the data into the table
      table.put_item(
            Item={
                  'id': str(uuid.uuid4()),
                  'bucket': bucket,
                  'key': key,
                  'request_time': request_time,
                  'key_name': key_name,
                  'created_at': str(datetime.datetime.now())
            }
      )
      return {
            'statusCode': 200,
            'body': 'Successfully processed {} records.'.format(len(event['Records']))
      }
 


   