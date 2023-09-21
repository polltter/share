FROM python:3.10.2

# Set the working directory to /app
WORKDIR .

# Set env variables
ENV API_URL="https://esg-maturity.com"
ENV BEARER_TOKEN="Bearer 3|7XOfemJabZDyJCCtOzZgpomqU8JMRl4gRADZ1HZp"

# Copy the current directory contents into the container at /app
COPY scrapers scrapers
COPY requirements.txt .
COPY run.sh .
COPY get_client_info.py .
COPY reputational_analysi.py .

# Install any needed packages specified in requirements.txt
RUN pip install -r requirements.txt

RUN python -m spacy download pt_core_news_sm

RUN apt-get update && apt-get install -y locales && rm -rf /var/lib/apt/lists/* \
       && sed -i -e 's/# pt_PT.UTF-8 UTF-8/pt_PT.UTF-8 UTF-8/' /etc/locale.gen \
       && locale-gen

ENTRYPOINT [ "/bin/bash" ]

CMD [ "run.sh" ]