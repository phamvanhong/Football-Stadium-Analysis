import sys
sys.path.insert(0, '/opt/airflow/src/')
from objects.web_scraping import WebScraping
import pandas as pd
import json

def extract_data(**kwargs):
    """
    Extracts data from the webpage
    """
    # Setup variables
    url = kwargs['url']
    target_table_index = kwargs['target_table_index']
    target_table = []
    ws = WebScraping(url)
    table_elements = ws.get_html_element('table')

    for table in table_elements:
        target_table.append(pd.read_html(str(table))[0])
    json_target_table = target_table[target_table_index].to_json(orient='records')
    kwargs['ti'].xcom_push(key='football_stadium_data', value=json_target_table)
    
    return "Data extracted and pushed to XCom"

def transform_data(**kwargs):
    """
    Transform the data
    """
    # Pull the data from XCom
    data = kwargs['ti'].xcom_pull(key='football_stadium_data', task_ids='extract_wikipedia_data')
    data = json.loads(data)

    # Drop column Images & drop duplicates
    football_stadium_df = pd.DataFrame(data)
    football_stadium_df = football_stadium_df.drop_duplicates().reset_index(drop=True)
    football_stadium_df = football_stadium_df.drop("Images", axis = 1)

    # Remove special characters
    cols = ['Stadium', 'Seating capacity']
    football_stadium_df[cols] = football_stadium_df[cols].apply(lambda x: x.str.replace(r'[^\w\s]', '', regex=True))

    # Split City column into two columns City and State/Province
    football_stadium_df[['City', 'State/Province']] = football_stadium_df['City'].str.split(',', expand=True)

    # Push the transformed data to XCom
    kwargs['ti'].xcom_push(key='football_stadium_data', value=football_stadium_df.to_json(orient='records'))
    
    return "Data transformed and pushed to XCom"

def load_data(**kwargs):
    """
    Load the data to the database
    """
    pass