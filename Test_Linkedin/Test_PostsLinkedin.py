import Test_AuthLinkedin as al
import requests

######################## GET USER INFO #############################

def get_user_info():
    
    
    access_token = al.auth() # Authenticate the API
    headers = al.headers(access_token) # Make the headers to attach to the API call. 
    response = requests.get('https://api.linkedin.com/v2/me', headers = headers)
    user_info = response.json()
    return user_info


if __name__ == '__main__':
    
    abc = get_user_info()