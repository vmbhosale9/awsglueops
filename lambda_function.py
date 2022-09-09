import boto3
import json
from AWSGlueOps import AWSGlueOperations

def list_jobs():
    AWSGOps = AWSGlueOperations()
    response = AWSGOps.aws_glue_list_jobs()
    if response['ResponseMetadata']['HTTPStatusCode'] == 200:
        return json.dumps(response)
    else:
        return "you are in trouble!"

def start_glue_job(jobname):
    AWSGOps = AWSGlueOperations()
    response = AWSGOps.aws_glue_start_job(jobname)
    if response['ResponseMetadata']['HTTPStatusCode'] == 200:
        return json.dumps(response)
    else:
        return "you are in trouble!"

def delete_glue_job(jobname):
    AWSGOps = AWSGlueOperations()
    response = AWSGOps.aws_glue_delete_job(jobname)
    if response['ResponseMetadata']['HTTPStatusCode'] == 200:
        return json.dumps(response)
    else:
        return "you are in trouble!"

# def start_glue_job():
#     response = client.start_job_run(
#         JobName='IrisJob'
#     )
#     print(json.dumps(response, indent=4, sort_keys=True, default=str))

# def create_glue_job():
#     response = client.create_job(
#         Name='IrisJob2',
#         Role='awsgluerole',
#         Command={
#             'Name': 'glueetl',
#             'ScriptLocation': 's3://awsglue081222/iris_onboarder.py',
#             'PythonVersion': '3'
#         },
#         DefaultArguments={
#           '--TempDir': 's3://awsglue081222/temp_dir',
#           '--job-bookmark-option': 'job-bookmark-disable'
#         },
#         MaxRetries=1,
#         GlueVersion='3.0',
#         NumberOfWorkers=2,
#         WorkerType='Standard'
#     )
#     print(json.dumps(response, indent=4, sort_keys=True, default=str))

def create_crawler():
    response = client.create_crawler(
        Name='S3Crawler',
        Role='arn:aws:iam::272350763384:role/awsgluerole',
        DatabaseName='S3CrawlerHOC',
        Targets={
            'S3Targets': [
                {
                    'Path': 's3://awsglue081222/read',
                    'Exclusions': [
                        'string',
                    ],
                    'SampleSize': 2
                },
                {
                    'Path': 's3://awsglue081222/write',
                    'Exclusions': [
                        'string',
                    ],
                    'SampleSize': 2
                },
            ]
        },
        Schedule='cron(15 12 * * ? *)',
        SchemaChangePolicy={
            'UpdateBehavior': 'UPDATE_IN_DATABASE',
            'DeleteBehavior': 'DEPRECATE_IN_DATABASE'
        },
        RecrawlPolicy={
            'RecrawlBehavior': 'CRAWL_EVERYTHING'
        },
        LineageConfiguration={
            'CrawlerLineageSettings': 'DISABLE'
        }
    )
    print(json.dumps(response, indent=4, sort_keys=True, default=str))    

def aws_get_glue_jobs():
    AWSGOps = AWSGlueOperations()
    response = AWSGOps.aws_glue_get_jobs()
    Jobs = ["None"]
    if response:
        Jobs.pop(0)
        for job in response['Jobs']:
            Jobs.append(job['Name'])
    return json.dumps(Jobs)        
    # print(type(response))
    # print(response)
    # print(json.dumps(response))
    # return json.dumps(response)
    # print("response is {}".format(response))
    # if response:
    #     return response
    # else:
    #     return "I don't see any AWS Glue jobs..."    

def list_crawlers():
    AWSGOps = AWSGlueOperations()
    response = AWSGOps.aws_glue_list_crawlers()
    if response['ResponseMetadata']['HTTPStatusCode'] == 200:
        return json.dumps(response)
    else:
        return "listcrawlers failed to run as expected..."    

def start_crawler(crwlername: str):
    AWSGOps = AWSGlueOperations()
    response = AWSGOps.start_crawler()    
    print(json.dumps(response))

def validateJSON(jsonData):
    try:
        json.loads(jsonData)
    except ValueError as err:
        return False
    return True

def create_job(job_details):
    AWSGOps = AWSGlueOperations()
    response = AWSGOps.aws_glue_create_job(**job_details)
    print(json.dumps(response, indent=4, sort_keys=True, default=str))
    if response['ResponseMetadata']['HTTPStatusCode'] == 200:
        return json.dumps(response)
    else:
        return "you are in trouble!"

def update_job(job_name, job_details):
    AWSGOps = AWSGlueOperations()
    response = AWSGOps.aws_glue_update_job(job_name, **job_details)
    print(json.dumps(response, indent=4, sort_keys=True, default=str))
    if response['ResponseMetadata']['HTTPStatusCode'] == 200:
        return json.dumps(response)
    else:
        return "you are in trouble!"        

def lambda_handler(event, context):
    responsedata = None
    if event['httpMethod'] == 'POST' and event['path'] == '/list_jobs':
        print("Calling listjobs method!")
        responsedata = list_jobs()
    elif event['httpMethod']=='POST' and event['path']=='/create_job':
        # In real-world, job_details will come via a DB query or HTTP parameters or a static file on s3 or ?
        requestparams = json.loads(event['body'])
        if bool(requestparams):
            print("requestparams are: {}".format(requestparams))
            script_bucket = requestparams['script_bucket']
            description = requestparams['description']            
            output_script_path = requestparams['output_script_path']
            temp_dir = requestparams['temp_dir']
            job_name = requestparams['job_name'] 
            role = requestparams['role']
            name = requestparams['name']
            glueversion = requestparams['glueversion']             
            contact = requestparams['contact']
            env = requestparams['environment']            
            job_spec = {
                'Name': name,
                "Description": description,
                'AllocatedCapacity': 2,
                'ScriptLocation': 's3://{}/{}'.format(script_bucket, output_script_path),
                'TempDir': 's3://{}/{}'.format(script_bucket, temp_dir),
                'MaxRetries': 0,
                'Name': job_name,
                'GlueVersion': glueversion,                
                'Role': role,
                'Contact': contact,
                'Environment': env
            }            
            print("Calling create_job method!") 
            responsedata = create_job(job_spec)
    elif event['httpMethod']=='POST' and event['path']=='/update_job':
        # In real-world, job_details will come via a DB query or HTTP parameters or a static file on s3 or ?
        requestparams = json.loads(event['body'])
        if bool(requestparams):
            print("requestparams are: {}".format(requestparams))
            script_bucket = requestparams['script_bucket']
            description = requestparams['description']            
            output_script_path = requestparams['output_script_path']
            temp_dir = requestparams['temp_dir']
            job_name = requestparams['job_name'] 
            role = requestparams['role']
            name = requestparams['name']
            glueversion = requestparams['glueversion']             
            contact = requestparams['contact']
            env = requestparams['environment']            
            job_spec = {
                "Description": description,
                'AllocatedCapacity': 2,
                'ScriptLocation': 's3://{}/{}'.format(script_bucket, output_script_path),
                'TempDir': 's3://{}/{}'.format(script_bucket, temp_dir),
                'MaxRetries': 0,
                'Name': job_name,
                'GlueVersion': glueversion,                
                'Role': role,
                'Contact': contact,
                'Environment': env
            }            
            print("Calling create_job method!") 
            responsedata = update_job(job_name, job_spec)            
    elif event['httpMethod']=='POST' and event['path']=='/delete_job':
        requestparams = json.loads(event['body'])
        if bool(requestparams):
            print("requestparams are: {}".format(requestparams))
            jobname = requestparams['jobname']        
        print("Calling delete_glue_job method!") 
        responsedata = delete_glue_job(jobname)            
    elif event['httpMethod']=='POST' and event['path']=='/list_crawlers':
        print("Calling listcrawlers method!") 
        responsedata = list_crawlers()
    elif event['httpMethod']=='POST' and event['path']=='/start_job':
        requestparams = json.loads(event['body'])
        if bool(requestparams):
            print("requestparams are: {}".format(requestparams))
            jobname = requestparams['jobname']           
        print("Calling start_glue_job method!") 
        responsedata = start_glue_job(jobname)        
    elif event['httpMethod']=='POST' and event['path']=='/aws_get_glue_jobs':
        print("Calling aws_get_glue_jobs method!") 
        responsedata = aws_get_glue_jobs()
        print(responsedata)
    else:
        print("Request method is not POST")
        responsedata = "Request method is not POST"

    # return {
    #     "statusCode": 200,
    #     "headers": {
    #         "Content-Type": "application/txt"
    #     },
    #     "body": responsedata
    # }   
    
    # return {
    #     "statusCode": 200,
    #     "headers": {
    #         "Content-Type": "application/json"
    #     },
    #     "body": json.dumps({
    #         "Response ": responsedata
    #     })
    # }    
        
    if validateJSON(responsedata) == True:
        print("responsedata is a valid json")  
        return {
            "statusCode": 200,
            "headers": {
                "Content-Type": "application/json"
            },
            "body": responsedata
        }         
    else:
        print("responsedata is not a valid json") 
        return {
            "statusCode": 200,
            "headers": {
                "Content-Type": "application/json"
            },
            "body": json.dumps({
                "Response ": responsedata
            })
        }
