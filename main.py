import pandas as pd
import json
import os
import logging
from base64 import b64decode
import requests
import re
from threading import Thread

# Configuration
file_url = "https://dpiadirectory.s3-us-west-2.amazonaws.com/LM/LM+Master+View.csv" # Public URL of the S3 file
slack_verification_token = os.environ['slack_verification_token'] # This value is taken from the environment variable of slack. It has to be manually placed there for this to work
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


def getdata(url):
    # Preprocessing of data
    raw_data = pd.read_csv(url)
    raw_data = raw_data.drop_duplicates()
    raw_data['Jira ID'] = raw_data['Jira ID'].fillna('Not Found')
    print(raw_data.head(5))
    return raw_data

def request_parse(event):

    request_body = b64decode(str(event["body"])).decode() #Decoding the request from base64 encoded format

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
    request_json = request_parse(event)
    expected_verification_token = slack_verification_token    # Enter the verification token found in the basic information page of your app here
    #expected_enterprise_name = 'Expedia+Group'              # If you have an enterprise account, enter the enterprise name here. Use + instead of spaces
    
    code = 400

    try:
        enterprise_name = request_json['enterprise_name']
    except:
        enterprise_name = ""
    
    logger.debug("Inside Verification Function: Token: %s  & Enterprise Name = %s" %(str(request_json['token']),enterprise_name))
    
    
    if request_json['token']==expected_verification_token:
        code = 200

    return code


def api_handler(event):
    logger.debug("Inside API HANDLER")
    data = getdata(file_url)
    logger.debug("Data read complete")
    
    request_json = request_parse(event)
    slashcommand = request_json['command'].replace('%2F','')
    url = request_json['response_url'].replace('%2F','/').replace('%3A',':')
    
    logger.debug("Event data cleaned")
    
    
    search_string = request_json['text']
    if slashcommand.lower() == 'lmsearch':
        initial_resp = {
        "text": 'Fetching results...'
        }
        requests.post(url, data = json.dumps(initial_resp))
        text = slack_message_generator(search_string,data)
    
    else:
        text = "I don't know what to do"
    
    
    response = {
    "text": text
    }
    
    return requests.post(url, data = json.dumps(response))


def slack_message_generator(search_string,dataframe):
    text = ''
    
    if search_string != '':
        relevant_data = dataframe[dataframe['Ref ID'].str.contains(pat=search_string, flags=re.IGNORECASE, na=False)]
        
        if len(relevant_data)!=0:
            for row,index in relevant_data.iterrows():
                text = text + f"```Reference ID: {relevant_data['Ref ID'][row]} \nRelated JIRA ID: {relevant_data['Jira ID'][row]} \nVendor (Finance listed): {relevant_data['Vendor (As listed by Finance)'][row]} \nSummary: {relevant_data['Summary'][row]} \nDescription: {relevant_data['Description'][row]} \nPriority: {relevant_data['Priority'][row]} \nCategory: {relevant_data['Combined Category'][row]} \nAssignee: {relevant_data['Assignee'][row]} \nProposed Action: {relevant_data['Combined Proposed Action'][row]} \n2020 Baseline: ${relevant_data['2020 Baseline'][row]:0.0f} \n2020 Outlook vs Baseline: ${relevant_data['2020 Outlook vs Baseline'][row]:0.0f} \nAnnualized Savings or Increase: ${relevant_data['Annualized Savings or Increase'][row]:0.0f} \nNew 2020 Outlook: ${relevant_data['New 2020 Outlook'][row]:0.0f} \n2021 Outlook: ${relevant_data['2021 Outlook'][row]:0.0f} \nMar FC vs 2021 Outlook: ${relevant_data['Mar FC vs 2021 Outlook'][row]:0.0f}``` \n\n"
        else:
            text = "```ERROR: No Data found for the search string: %s```"%(search_string)
    elif search_string == '':
        text = '```ERROR: Search string is empty```'
    return text
    
def dispatcher(event,context):
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
    

def lambda_handler(event,context):
    thr = Thread(target=dispatcher, args=[event,context])
    thr.start()
    thr.join()
    return json.dumps({'text':'Done!'})

            