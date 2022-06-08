from linkedin_search_posts_bot import *
from bot_studio import *
import requests

def linkedin_read_credentials():
    
    with open('C:/Users/gffar/TweepyProject/Spyder/Projeto/Test_Linkedin/LinkedInCredentials.json') as f:
        credentials = json.load(f)
        
    return credentials["LinkedinUser"], credentials["LinkedinPass"]

def login():
    
    creds = linkedin_read_credentials()
    user = creds[0]
    password = creds[1]
    
    linkedin.login(username=user,password=password)

def get_posts():
    
    login()
    linkedin.search_posts(keyword='data')
    response=linkedin.posts_results()
    data=response['body']
    
    return data

if __name__ == '__main__':
    
    posts = get_posts()