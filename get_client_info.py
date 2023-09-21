import requests
import json
from collections import defaultdict
import os

# API_URL = 'http://saas.test'
# API_URL = 'https://esg-maturity.com'
if os.getenv("API_URL") is not None:
    API_URL = os.getenv("API_URL")
    print('API_URL found ', API_URL)
else:
    API_URL = 'http://saas.test'

if os.getenv("BEARER_TOKEN") is not None:
    BEARER_TOKEN = os.getenv("BEARER_TOKEN")
    print('BEARER_TOKEN found ', BEARER_TOKEN)
else:
    exit('BEARER_TOKEN not found')

def get_tenants():
    tenants_url_dev = f"{API_URL}/api/v1/tenants/7XOfemJW0VLmm11NXAuEVOCtOzZgpomqU8JGXkqJ17EAuswHCwU2/reputation"

    headers = {'Accept': 'application/json',
               'Authorization': BEARER_TOKEN}

    response = requests.get(tenants_url_dev, headers=headers)

    list_of_tenants = []

    for tenant in response.json()['data']:
        list_of_tenants.append(tenant['tenancy_db_name'].replace('tenant', ''))

    return list_of_tenants


def get_analysis():
    analysis_per_tenant = defaultdict(list)

    analysis_url = f"{API_URL}/api/v1/reputational/analysis-info"

    list_of_tenants = get_tenants()
    print(len(list_of_tenants))

    for tenant in list_of_tenants:

        headers = {'Accept': 'application/json',
                   'Authorization': BEARER_TOKEN,
                   'X-Tenant': tenant}

        response = requests.get(analysis_url, headers=headers)

        # print("tennant:\n", tenant, "\n", response, "\n")
        # print(analysis_url, "\n")
        # return response.json()

        list_of_analysis_ids = []
        try:
            a = response.json()['data']
        except:
            print("falhou")
            continue
        for analysis in response.json()['data']:
            list_of_analysis_ids.append(analysis['id'])

        # print(list_of_analysis_ids)

        for analysis_id in list_of_analysis_ids:
            path = "/" + str(analysis_id)

            headers = {'Accept': 'application/json',
                       'Authorization': BEARER_TOKEN,
                       'X-Tenant': tenant}

            response = requests.get(analysis_url + path, headers=headers)

            # print("analysis_id\n", response.json())
            analysis_per_tenant[tenant].append({
                'id': analysis_id,
                'name': response.json()['data']['name'],
                'search_terms': response.json()['data']['search_terms'],
            })
            # dic with the format: {tenant: [(analysis_id, analysis_name, [search_terms])]}
            # in the future we are also interested in storing further info like language, etc.
    return analysis_per_tenant


def get_analysis_offline():
    with open("client_new.json", 'r') as f:
        data = json.load(f)
    f.close()
    return data


if __name__ == '__main__':
    # l = get_tenants()
    l = get_analysis()
    # with open("client_new_luis.json", 'w') as f:
    #     json.dump(l, f, indent=2)

    print("l: ", l)

