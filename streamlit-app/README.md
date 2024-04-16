# Streamlit Setup

Use python 3.9 for this project
```
pyenv install 3.9.10
pyenv local 3.9.10
```

Setup virtual environment
```
python3.9 -m venv venv 
```

Activate venv
```
# For mac
source venv/bin/activate

# For windows
.\venv\Scripts\activate
```

Install requirements
```
pip install -r requirements.txt
```

Run streamlit
```
streamlit run hdb_prices_streamlit.py
```