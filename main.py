import schedule
import time
import datetime
import subprocess
from reputational_analysi import ReputationalAnalysi
from get_client_info import *

def execute_reputational_analysi():
    execute_scrapy("globo2", "/scripts/scraps/globo-br/globo2")
    execute_scrapy("forbes", "/scripts/scraps/forbes-br")
    execute_scrapy("exame", "/scripts/scraps/revista-exame-br")
    execute_scrapy("economico2", "//scripts/scraps/valor-economico-br/economico2")
    log("start reputation analysis")
    yesterday = datetime.date.today().strftime('%Y-%m-%d')
    path = f"/data/{yesterday}"    
    tenants = get_analysis()

    for tenant in tenants:
        print(tenant)
        analysi = ReputationalAnalysi(tenant, tenants[tenant], "", "", [f"{path}/economico2.json", f"{path}/exame.json", f"{path}/globo2.json", f"{path}/forbes.json"])
        # analysi.dataframe = pd.read_pickle('analise_feita.pickle')
        analysi.sentiment()
        analysi.extract_keywords()
        analysi.emotion()
        analysi.save()

def execute_scrapy(scrapy_name: str, path: str):
    try:
        result = subprocess.run("scrapy crawl " + scrapy_name, shell=True, capture_output=False, text=False, cwd=path)
        if (result.returncode == 0):
            log(f"scrapy: {scrapy_name} executed successfully")
        else:
            log(f"scrapy: {scrapy_name} execution failure status code: {result.returncode}")
    except Exception:
        log(f"error when trying to run scrapy: {scrapy_name}")

def log(text: str):
    with open("/data/log.txt", 'a') as file:
        file.write(f"[{datetime.date.today()}] {text}\n")

def main():
    log("started routine...")
    schedule.every().day.at("04:00:00").do(execute_reputational_analysi)   

    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == '__main__':   
    main()

   
