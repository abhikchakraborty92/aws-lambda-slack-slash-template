import pandas as pd
import json
import os
import logging
from base64 import b64decode
import requests
import re
from threading import Thread
import boto3

# Configuration

# Data file configuration
# This is optional and can be replaced with RDS or any other source based on which slack notification have to be sent out
file_key = "S3 of the S3 File" # you can find the s3 key under the file details inside your s3 bucket
bucket = 'S3 Bucket' # S3 Bucket where the data file is stored

# This value is taken from the environment variable of slack. It has to be manually placed there for this to work
slack_verification_token = os.environ['slack_verification_token'] 

# Activating the logger to log into cloudwatch
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

# Activating an S3 Client
# Please note that access to S3 is only granted if the required permissions are provided to the role used for the lambda function. To define a role, go to AWS IAM and create new role with necessary read permission to S3
s3 = boto3.client('s3')

def getdata(url):
    '''
    Use this function to do all the necessary changes to the data. Here I have read a csv file from S3 but this can be modified based on requirement'''
    
    # Preprocessing of data
    file = s3.get_object(Bucket=bucket,Key=file_key) # Accessing the S3 file
    raw_data = pd.read_csv(file['body'],sep=',')
    raw_data = raw_data.drop_duplicates()
    
    return raw_data

def request_parse(event):
    ''' This particular function will be used to decode the base64 encoded slack event body. This function is envoked from the dispatcher and other functions here to gather different components of the request body like authentication token, enterprise name, response URL, etc. Using that, we can run various other functions in this code'''

     #Decoding the request from base64 encoded format
    request_body = b64decode(str(event["body"])).decode()

    # Some general replacements of string values to convert the request body into a dictionary like structure
    request_body = request_body.replace("&",", ")
    request_body = request_body.replace("=",":")
    
    # Spliting the request body into elements of a key-value payer
    request_list = request_body.split(', ')
    

    logger.debug("Inside Request Parser. Request body: "+request_body)
    logger.debug("Inside Request Parser. Request list length: "+str(len(request_list)))
    
    # Creating a blank dictionary to store the request
    request_json = {}
    
    # Getting request body key-value payers and loading into request_json dictionary. The key value payer are paired into single element but separated by :. Therefore, the string is first split and the first element is treated as key and the second one is treated as a value
    for i in range(0 , len(request_list)):
        request_json[request_list[i].split(":")[0]] = request_list[i].split(":")[1]
    
    # Cleaning request parameters. The spaces here are represented as + so we are replacing them back with spaces. 'text' is actually the search trigger which comes with the slack command and is needed for further processing
    request_json['text'] = request_json['text'].replace('+',' ')
    
    return request_json


def verify(event):

    '''
    This function uses the contents of the request body obtained from the request_parse function to verify if the request is coming from a valid slack based point of source. The conditions checked are:
    - Validity of slack verification token
    - [OPTIONAL] Enterprise Name
    '''

    # Calling the parsing function to decode the event body to get slack request data
    request_json = request_parse(event)
    
    # Enter the verification token found in the basic information page of your app here. The token can also be stored under environment variables of the lambda function
    expected_verification_token = slack_verification_token

    # Uncomment below if you have an enterprise account. Enter the enterprise name and use + instead of spaces
    #expected_enterprise_name = 'My+Good+Company'              
    
    # Setting the default authentication token as 400 (invalid)
    code = 400
    
    # Some slack requests body might not have enterprise name at all so using a try block below to catch related error
    try:
        enterprise_name = request_json['enterprise_name']
    except:
        enterprise_name = ""
    
    logger.debug("Inside Verification Function: Token: %s  & Enterprise Name = %s" %(str(request_json['token']),enterprise_name))
    
    
    # Add an additional expected_enterprise_name ==  enterprise_name condition if using enterprise slack account. 
    # This is an optional authentication step which can be ignored as the token can be used on its own to validate the account. 
    # However, it would be a good practice to include this level if this solution is being made for an enterprise slack to ensure that requests from slack sources outside the enterprise domain is not authenticated
    if request_json['token']==expected_verification_token:
        code = 200

    return code


def api_handler(event):

    ''' One of the most important functions of this template is the API handler. Here we add the routes (slack slash commands) and related functions or steps to be triggered whenever that route is called
    '''

    logger.debug("Inside API HANDLER")

    # Running the getdata function to get relevant data that will serve as a response for the slack command
    data = getdata(file_url)
    logger.debug("Data read complete")
    
    # Parsing the request body to get the relevant slash command and text prompt and response URL
    request_json = request_parse(event)
    
    # Getting the slash command. This will help in understanding what function to trigger
    slashcommand = request_json['command'].replace('%2F','')
    
    # Getting the response URL to respond based on a request
    url = request_json['response_url'].replace('%2F','/').replace('%3A',':')
    
    logger.debug("Event data cleaned")
    
    # Getting the text string parameter which would be used in the triggering function based on the slash command
    search_string = request_json['text']
    
    
    # The slash command if-else block to re-direct the commands to their specific functions. If the command is not recognized, the response would be a default statement

    if slashcommand.lower() == 'firstslashcommand':

        # Sending an initial_resp (initial response) within 3 seconds of the request to prevent request timeout error on slack
        initial_resp = {
        "text": 'Fetching results...'
        }
        requests.post(url, data = json.dumps(initial_resp))
        
        # This is the part where the required function gets triggered for a slash command. Many functions can be written based on requirement of a particular case. For this case, we assume that we have to send some text insights which we will generate on the slack_message_generator function
        text = slack_message_generator(search_string,data)
    
    # Default response if the slack slash command is not recognized
    else:
        text = "I don't know what to do"
    
    
    # Creating a message json object to respond. After the initial response is done, this response can be provided within the next 30 minutes of the initial response

    response = {
    "text": text
    }
    
    # Using the response url 'url' to respond with the necessary results
    return requests.post(url, data = json.dumps(response))


def slack_message_generator(search_string,dataframe):
    ''' 
    Use this function to write relevant steps for producing text based responses for the slash command. In this example I have used a pandas dataframe and using the 'text' obtained from the request body as a 'search_string' I am filtering relevant data, converting them into readable message format and then delivering it to the user

    WARNING: The code in this function block is just an example. Please create your own steps for customizing
    '''

    text = ''
    
    if search_string != '':
        relevant_data = dataframe[dataframe['Filter Column'].str.contains(pat=search_string, flags=re.IGNORECASE, na=False)]
        
        # If the relevant_data has some data
        if len(relevant_data)!=0:
            for row,index in relevant_data.iterrows():
                text = text + ''' Necessary Text using columns of the relevant_data dataframe. Please refer to the slack documentation 
                to create nice formatted text outputs'''
        
        # If the filtered dataset is empty
        else:
            text = "```ERROR: No Data found for the search string: %s```"%(search_string)
    
    # If the search_string is empty in the request body
    elif search_string == '':
        text = '```ERROR: Search string is empty```'
    return text
    
def dispatcher(event,context):

    '''
    This function is a central function dispatcher that is deployed from the lambda handler as a thread
    '''

    verification_token = 400

    # Trying to extract slack request body
    try:
        body = event["body"]
    except:
        body = ""
    
    # If body is present
    if body != "":
        request = request_parse(event) # Parse the request body from base64 encoded format to a dictionary
        logger.debug("EVENT BODY: \n\n"+json.dumps(request)) # Logging the parsed request

        verification_token = verify(event) # Gathering the verification token to ensure that the request is genuine
    
    # General Logging of the raw event and context of the lambda function
    logger.debug("RAW EVENT: \n\n"+str(json.dumps(event)))
    logger.debug("RAW CONTEXT: \n\n"+str(context))

    # If the verification is successful a 200 code is generated
    if verification_token == 200:
            code = verification_token
            logger.debug("SUCCESSFUL AUTHENTICATION")
            api_handler(event)
            logger.debug("DEPLOYED")
            
    # If the verification is invalid        
    elif verification_token == 400 or body == "":
            logger.debug("INVALID AUTHENTICATION")
            text = 'Invalid request'
            code = 401
            response = {
            'statusCode': code,
            'headers': { 'Content-Type': 'application/json' },
            'body': text
            }
            return response
    
'''
This is the handler for the lambda function. This receives the slack request and starts dispatching them into different functions.
'''
def lambda_handler(event,context):
    thr = Thread(target=dispatcher, args=[event,context])
    thr.start()
    thr.join()
    return json.dumps({'text':'Done!'})

            