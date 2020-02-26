from __future__ import print_function
from datetime import date, datetime
from json import dumps
import boto3
import botocore
import sys
import argparse
import json


__version__ = '0.1'
__author__ = 'Luciana Marinelli'

lGroups = []
dUsers = []
LIMIT = 10;

def datetime_handler(x):
    if isinstance(x, (datetime, date)):
        return x.isoformat()
    raise TypeError ("Type %s not serializable" % type(x))

# list user groups from a user pool id based on a user

def listUserGroups(inUsername, user_pool_id, client_id):
    lGroups.clear()
    groupResponse = client_id.admin_list_groups_for_user(
        Username = inUsername,
        UserPoolId = user_pool_id
        )
    for group in groupResponse['Groups']:
        # print(group['GroupName'])
        lGroups.append(group)
        
    return(lGroups)        
 
 
# list Cognito users from a user pool id

def listUsers(user_pool_id, client_id):
    print(client_id)
    userGroups = []
    count = 0
    continueListing = True
    
    # Retrieves the first group of users and its token
    userResponse = client_id.list_users(
        UserPoolId = user_pool_id,
        Limit=LIMIT,
        # AttributesToGet=[],
        # Filter="'username' = '2b7e3ec0-8371-4bf4-ae22-b5a46dcb1266'"
        )
    pToken = userResponse.get('PaginationToken') 
    count +=len(userResponse['Users'])
    print('Length ='+str(len(userResponse['Users'])));
   
    # for each user call listUserGroups to append its groups

    for user in userResponse['Users']:
        userGroups={}
        userGroups = listUserGroups(user['Username'], user_pool_id, client_id)
        user['Groups'] = list(userGroups);
        dUsers.append(user)
    # print (dUsers)
   
    # Continue to retrieve users until group reaches limit so no more pagination

    while (continueListing):
        userResponse = client_id.list_users(
            UserPoolId = user_pool_id,
            Limit=LIMIT,
            PaginationToken=pToken,
        )
        print('Length ='+str(len(userResponse['Users'])));
        count +=len(userResponse['Users'])

        # for each user call listUserGroups to append its groups
       
        for user in userResponse['Users']:
            userGroups={}
            userGroups = listUserGroups(user['Username'], user_pool_id, client_id)
            user['Groups'] = list(userGroups);
            dUsers.append(user)

        # Append results and get next pagination token if len greater than limit
       
        if (len(userResponse['Users']) < LIMIT):
            continueListing = False
        else:
            pToken = userResponse.get('PaginationToken')

    # Print total of users

    print('Total users = '+str(count))      
    
    # Return dictionary to print to JSON
    return (dUsers)    
    
def main(arguments):

    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--profile', required=False, default='tharrpdev')
    parser.add_argument('--user-pool-id', required=False, help="id of the user pool where accounts will be created", default='ca-central-1_EHHcOBNVv')
    parser.add_argument('--region', default='ca-central-1')
    parser.add_argument('--output-file', required=True, help="Output filename")
    args = parser.parse_args(arguments)

    # print(args)

    session = boto3.Session(profile_name=args.profile,region_name=args.region)
    cognito = session.client('cognito-idp')
    client = boto3.client('cognito-idp')

    print(client)

    cognitoUsers = listUsers(args.user_pool_id, client)

    f = open(args.output_file,'w')
    f.write(json.dumps(cognitoUsers, default=datetime_handler, indent = 4))
    f.close()

if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
