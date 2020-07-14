import json
import boto3
import requests
import pandas as pd

'''
With lambda functions there is always an issue of the function going cold because of its serverless architecture. Due to this, if the function is called after a while, AWS takes some time to gather relevant resources to fire the function and it sometimes causes timeout issues in slack(it needs its response within 3 seconds) and bigger issues in critical systems. To ensure that the function doesn't sleep after not being called out for a while, this lambda_warmer program would send a ping to the function every n minutes where the n can be decided by the user, to keep the function warm and ready.
'''



# Boto3 library is used to create a relevant lambda client to call other lambda functions
client = boto3.client('lambda')

def trigger_lambda(arn_string):
    '''
    Simple trigger function to send pinging triggers to lambda functions. This is done by using the ARN of a function and sending it a very simple trigger request using boto3 library
    '''


    # Input trigger simple triggering parameter
    inputParams = {
        'text':'hello'
    }
    

    # Invoking the function using ARN string. This function however needs special privileges to run and cannot work with the basic lambda execution role.

    response = client.invoke(
        FunctionName = arn_string,
        InvocationType = 'RequestResponse',
        Payload = json.dumps(inputParams)
    )
    
    # Gathering and sending relevant response from the triggered lambda process
    responseFromChild = json.load(response['Payload'])
    return responseFromChild

def lambda_handler(event, context):
    

    # Using the pandas library to read the function_list.csv file to get the names of the lambda functions and their ARMs
    function_list = pd.read_csv('function_list.csv')
    

    # Triggering the functions with a loop
    for row,index in function_list.iterrows():
        try:
            resp = trigger_lambda(function_list['arn'][row])
            notification_text = 'LAMBDA FUNCTION NAME: %s | TRIGGER RESPONSE: Function Trigger Successful'%(str(function_list['function'][row]))

            print(notification_text)
        except:
            notification_text = 'LAMBDA FUNCTION NAME: %s | TRIGGER RESPONSE: Function Trigger Failed'%(str(function_list['function'][row]))

        
    return 'Done'