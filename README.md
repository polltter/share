# Reputational Analysis


## About

Reputational Analysis lets you access, extract and analyse text data from different sources and languages based on a predefined set of criteria.

The analysis includes three main steps:

- keyword extraction
- sentiment analysis
- emotion analysis
<br><br>


## Installation

Use the `requirements.txt` file together with the package manager pip:

```
pip install requirements.txt
```
<br>


## Usage

Run the get_data_and_post.py file to get new data and save it in our database.

```
python get_data_and_post.py
```


Run the analyse_data.py file to get the data from our database and analyse it. This analysis performs keyword extraction, sentiment analysis and emotion analysis.

```
python analyse_data.py
```

**Note:** Regarding the `*.py` files, only the files
- `get_client_info.py`
- `get_data_and_post.py`
- `analyse_data.py`
- `kw_extraction_api.py`
- `sentiment_analysis_api.py`
- `emotion_analysis_api.py`

are needed. The remaining `*.py` files were only kept for reference and can be deleted.