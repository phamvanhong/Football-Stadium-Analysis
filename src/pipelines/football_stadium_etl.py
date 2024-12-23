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
    kwargs['ti'].xcom_push(key='stadium_data', value=json_target_table)
    
    return "Data extracted and pushed to XCom"

def transformation(**kwargs):
    """
    Transform the data
    """
    # Pull the data from XCom
    data = kwargs['ti'].xcom_pull(key='stadium_data', task_ids='extract_wikipedia_data')
    data = json.loads(data)

    # Transformation
    football_stadium_df = pd.DataFrame(data)
    


    
    kwargs['ti'].xcom_push(key='transformed_data', value=df.to_json(orient='records'))
    
    return "Data transformed and pushed to XCom"