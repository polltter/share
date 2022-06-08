
import json
import random
import requests
import string


################# READ CREDENTIALS #####################################

def linkedin_read_credentials():
    
    with open('C:/Users/gffar/TweepyProject/Spyder/Projeto/Test_Linkedin/credentials.json') as f:
        credentials = json.load(f)
    return credentials

################# CREATE CSRF TOKEN - POR SEGURANÇA (proteger de cross-site request forgery) #####################################

def create_CSRF_token():
    
    letters = string.ascii_lowercase
    token = ''.join(random.choice(letters) for i in range(20))
    return token

################### OPEN URL #############################################3

def open_url(url):

    import webbrowser
    print(url)
    webbrowser.open(url)

################## EXTRACT ACCESS TOKEN  FROM REDIRECT URI ############################

def parse_redirect_uri(redirect_response):
    
    from urllib.parse import urlparse, parse_qs
 
    url = urlparse(redirect_response)
    url = parse_qs(url.query)
    return url['code'][0]

################# AUTHORIZE ##################################################
 
def authorize():
    
    # redirect URL : http://localhost:8080
    
    api_url = 'https://www.linkedin.com/oauth/v2'
    
    # Request authentication URL
    csrf_token = create_CSRF_token()
    
    params = {'response_type': 'code',
              'client_id': '',          #### Preencher com dados de conta
              'redirect_uri': 'http://localhost:8080',
              'state': csrf_token,
              'scope': 'r_liteprofile,r_emailaddress,w_member_social'
              }
 
    response = requests.get(f'{api_url}/authorization',params=params)
 
    open_url(response.url)
 
    # Get the authorization verifier code from the callback url
    redirect_response = input('Paste the full redirect URL here:')
    auth_code = parse_redirect_uri(redirect_response)
    return auth_code

################### SAVE ACCESS TOKEN TO credentials.json #############################

def save_token(filename,data):

    
    data = json.dumps(data, indent = 4) 
    with open(filename, 'w') as f: 
        f.write(data)
 
################ MAKE HEADERS TO ATTACH TO API CALL ################        
 
def headers(access_token):
    
   
    headers = {
                'Authorization': f'Bearer {access_token}',
                'cache-control': 'no-cache',
                'X-Restli-Protocol-Version': '2.0.0'
              }
    return headers

################# NEW ACCESS TOKEN ##################################3

def refresh_token(auth_code,client_id,client_secret,redirect_uri):
    
    access_token_url = 'https://www.linkedin.com/oauth/v2/accessToken'
 
    data = {
        'grant_type': 'authorization_code',
        'code': auth_code,      
        'redirect_uri': redirect_uri,
        'client_id': client_id,         #### Preencher com dados de conta         
        'client_secret': client_secret  #### Preencher com dados de conta
        }
 
    response = requests.post(access_token_url, data=data, timeout=30)
    response = response.json()
    print(response)
    access_token = response['access_token']
    return access_token

################                    ###############################
################  COMBINE FUNCTIONS ##############################
################                    ################
       
def auth():
    
    creds = linkedin_read_credentials()
    print(creds)
    client_id, client_secret = creds['client_id'], creds['client_secret']
    redirect_uri = creds['redirect_uri']
    api_url = 'https://www.linkedin.com/oauth/v2'
         
    if 'access_token' not in creds.keys(): 
        args = client_id,client_secret,redirect_uri
        auth_code = authorize(api_url,*args)
        access_token = refresh_token(auth_code,*args)
        creds.update({'access_token':access_token})
        save_token('credentials.json',creds)
    else: 
        access_token = creds['access_token']
    return access_token

if __name__ == '__main__':
    
    #creds = linkedin_read_credentials('C:/Users/gffar/TweepyProject/Spyder/Projeto/credentials.json')
    #client_id, client_secret = creds['client_id'], creds['client_secret']
    #redirect_url = creds['redirect_url']
    #print(redirect_url)
    
    abc = auth()
    
  
    
    