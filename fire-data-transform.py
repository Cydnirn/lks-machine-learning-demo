import base64
import json
import datetime

def lambda_handler(event, context):
    output_records = []
    
    # Process each record from Firehose
    for record in event['records']:
        # Decode and parse the record data
        payload = base64.b64decode(record['data']).decode('utf-8')
        
        try:
            # Parse the JSON data
            data = json.loads(payload)
            
            # Add processed timestamp and source info
            data['processed_timestamp'] = datetime.datetime.now().isoformat()
            data['source'] = 'fire_sensor_network'
            
            
            # Add data partition fields for S3 organization
            timestamp = datetime.datetime.fromisoformat(data['timestamp'])
            data['year'] = timestamp.year
            data['month'] = timestamp.month
            data['day'] = timestamp.day
            data['hour'] = timestamp.hour
            
            # Encode the transformed data
            transformed_payload = json.dumps(data) + '\n'  # Adding newline for better S3 query performance
            
            # Create the output record
            output_record = {
                'recordId': record['recordId'],
                'result': 'Ok',
                'data': base64.b64encode(transformed_payload.encode('utf-8')).decode('utf-8')
            }
            
        except Exception as e:
            # Handle errors - log the error but don't drop the record
            print(f"Error processing record: {str(e)}")
            # Pass through the original record in case of error
            output_record = {
                'recordId': record['recordId'],
                'result': 'ProcessingFailed',
                'data': record['data']
            }
        
        # Add the record to output
        output_records.append(output_record)
    
    return {'records': output_records}