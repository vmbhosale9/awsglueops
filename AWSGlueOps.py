import boto3
from botocore.exceptions import ClientError
import json
import pkg_resources

aws_region = boto3.session.Session().region_name

# run glue job with params
# glue.start_job_run(JobName=myJob['Name'], Arguments={"VAL1":"value11","VAL2":"value22","VAL3":"value33"})
# https://stackoverflow.com/questions/52316668/aws-glue-job-input-parameters

class AWSGlueOperations:
    def __init__(self) -> None:
        print("AWSGlueOperations constructor called...")
        self.client = boto3.client("glue", region_name=aws_region)
        
    def aws_glue_create_job(self, **kwargs):
        template_io = pkg_resources.resource_stream(__name__, "./glue_job_template.json")
        template = json.load(template_io)        
        if 'Name' in kwargs:
            template["Name"] = kwargs['Name']
        else :
            raise ValueError("You must give your job a name, using the Name kwarg")
    
        if 'Role' in kwargs:
            template["Role"] = kwargs["Role"]
        else :
            raise ValueError("You must give your job a role, using the Role kwarg")
    
        if 'ScriptLocation' in kwargs:
            template["Command"]["ScriptLocation"] = kwargs["ScriptLocation"]
        else :
            raise ValueError("You must assign a ScriptLocation to your job, using the ScriptLocation kwarg")
    
        if 'TempDir' in kwargs:
            template["DefaultArguments"]["--TempDir"] = kwargs["TempDir"]
        else :
            raise ValueError("You must give your job a temporary directory to work in, using the TempDir kwarg")
    
        if 'extra-files' in kwargs:
            template["DefaultArguments"]["--extra-files"] = kwargs["extra-files"]
        else :
            template["DefaultArguments"].pop("--extra-files", None)
    
        if 'extra-py-files' in kwargs:
            template["DefaultArguments"]["--extra-py-files"] = kwargs["extra-py-files"]
        else :
            template["DefaultArguments"].pop("--extra-py-files", None)
    
        if 'MaxConcurrentRuns' in kwargs:
            template["ExecututionProperty"]["MaxConcurrentRuns"] = kwargs["MaxConcurrentRuns"]
    
        if 'MaxRetries' in kwargs:
            template["MaxRetries"] = kwargs["MaxRetries"]
    
        if 'AllocatedCapacity' in kwargs:
            template["AllocatedCapacity"] = kwargs["AllocatedCapacity"]
        else:
            template["AllocatedCapacity"] = 2  
            
        print("AWSGlueOperations.aws_glue_create_job called...")
        
        try:
            return self.client.create_job(**template)
        except ClientError as e:
            if e.response.get('Error', {}).get('Code') == 'InvalidInputException':
                print("create_glue_job - InvalidInputException")
            elif e.response.get('Error', {}).get('Code') == 'IdempotentParameterMismatchException':
                print("create_glue_job - IdempotentParameterMismatchException")
            elif e.response.get('Error', {}).get('Code') == 'AlreadyExistsException':
                print("create_glue_job - AlreadyExistsException")
            elif e.response.get('Error', {}).get('Code') == 'InternalServiceException':
                print("create_glue_job - InternalServiceException")
            elif e.response.get('Error', {}).get('Code') == 'OperationTimeoutException':
                print("create_glue_job - OperationTimeoutException")                
            elif e.response.get('Error', {}).get('Code') == 'ResourceNumberLimitExceededException':
                print("create_glue_job - ResourceNumberLimitExceededException") 
            elif e.response.get('Error', {}).get('Code') == 'ConcurrentModificationException':
                print("create_glue_job - ConcurrentModificationException")                 
            else:
                print("create_glue_job - Something out of the world may have happened!")
        except Exception as e:
          raise Exception( "Unexpected error in create_glue_job: " + e.__str__())
          
    def aws_glue_list_jobs(self):
        print("AWSGlueOperations.list_jobs called...")
        try:
            return self.client.list_jobs()
        except ClientError as e:
          raise Exception( "boto3 client error in aws_glue_list_jobs: " + e.__str__())
        except Exception as e:
          raise Exception( "Unexpected error in aws_glue_list_jobs: " + e.__str__()) 
        
    def aws_glue_list_crawlers(self):
        print("AWSGlueOperations.aws_glue_list_crawlers called...")
        try:
            return self.client.list_crawlers()
        except ClientError as e:
          raise Exception( "boto3 client error in aws_glue_list_crawlers: " + e.__str__())
        except Exception as e:
          raise Exception( "Unexpected error in aws_glue_list_crawlers: " + e.__str__()) 

    def aws_glue_delete_job(self, jobname):
       try:
          return self.client.delete_job(JobName=jobname)
       except ClientError as e:
          raise Exception( "boto3 client error in aws_glue_delete_job: " + e.__str__())
       except Exception as e:
          raise Exception( "Unexpected error in aws_glue_delete_job: " + e.__str__()) 

    def aws_glue_get_jobs(self):
       try:
          jobs = self.client.get_jobs()
          return jobs
       except ClientError as e:
          raise Exception( "boto3 client error in aws_glue_get_jobs: " + e.__str__())
       except Exception as e:
          raise Exception( "Unexpected error in aws_glue_get_jobs: " + e.__str__())   
          
    def start_crawler(self, crwlername: str):
        try:
            response = self.client.start_crawler(crwlername)
            return response
        except ClientError as e:
            if e.response.get('Error', {}).get('Code') == 'CrawlerRunningException':
                print("start_crawler - Schema crawler already running.")
            elif e.response.get('Error', {}).get('Code') == 'EntityNotFoundException':
                print("start_crawler - EntityNotFoundException")
            elif e.response.get('Error', {}).get('Code') == 'OperationTimeoutException':
                print("start_crawler - OperationTimeoutException")
            else:
                print("start_crawler - Something out of the world may have happened!")
        # except ClientError as e:
        #   raise Exception( "boto3 client error in start_crawler: " + e.__str__())
        except Exception as e:
          raise Exception( "Unexpected error in start_crawler: " + e.__str__())
