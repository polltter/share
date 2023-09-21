#!/usr/bin/bash
#source venv/bin/activate
(
cd scrapers/all || exit
scrapy crawl all_scrapers -O data.json -s LOG_ENABLED=0
)
python3 reputational_analysi.py
#deactivate