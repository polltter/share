import requests

from collections import defaultdict


def get_tenants():

    tenants_url = "https://esg-maturity.com/api/v1/tenants"

    path = "/7XOfemJW0VLmm11NXAuEVOCtOzZgpomqU8JGXkqJ17EAuswHCwU2/?feature=reputation"

    headers = {'Accept': 'application/json', 
               'Authorization': 'Bearer 2|IIvhcPW0VLmm11NXAuEVOxQMI1GLdyJ8cUntGzBB'}

    response = requests.get(tenants_url + path, headers=headers)

    list_of_tenants = []

    for tenant in response.json()['data']:
        list_of_tenants.append(tenant['id'])
        
    return list_of_tenants


def get_analysis():

    analysis_per_tenant = defaultdict(list)

    analysis_url = "https://esg-maturity.com/api/v1/reputational/analysis-info"

    list_of_tenants = get_tenants()

    for tenant in list_of_tenants:

        headers = {'Accept': 'application/json', 
                   'Authorization': 'Bearer 2|IIvhcPW0VLmm11NXAuEVOxQMI1GLdyJ8cUntGzBB', 
                   'X-Tenant': tenant}

        response = requests.get(analysis_url, headers=headers)
        
        #print(response.json())
        
        list_of_analysis_ids = []
        
        for analysis in response.json()['data']:
            #print(analysis)
            list_of_analysis_ids.append(analysis['id'])
        
        #print(list_of_analysis_ids)
        
        for analysis_id in list_of_analysis_ids:

            path = "/" + str(analysis_id)

            headers = {'Accept': 'application/json', 
                       'Authorization': 'Bearer 2|IIvhcPW0VLmm11NXAuEVOxQMI1GLdyJ8cUntGzBB', 
                       'X-Tenant': tenant}

            response = requests.get(analysis_url + path, headers=headers)

            #print(response.json())
            
            analysis_per_tenant[tenant].append((analysis_id, 
                                                response.json()['data']['name'], 
                                                response.json()['data']['search_terms']))
            # dic with the format: {tenant: [(analysis_id, analysis_name, [search_terms])]}
            # in the future we are also interested in storing further info like language, etc.

    return analysis_per_tenant


if __name__ == '__main__':

    print(get_tenants())
    #print(get_analysis())