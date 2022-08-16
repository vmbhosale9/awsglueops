import boto3
from botocore.exceptions import ClientError

aws_region = boto3.session.Session().region_name

class AWSGlueOperations:
    def __init__(self) -> None:
        print("AWSGlueOperations constructor called...")
        self.client = boto3.client("glue", region_name=aws_region)
        
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
