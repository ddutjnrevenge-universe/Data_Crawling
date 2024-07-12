from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import requests
from bs4 import BeautifulSoup
import firebase_admin
from firebase_admin import credentials, firestore, storage
import os
import tempfile

# Firebase setup
cred = credentials.Certificate("/opt/airflow/dags/firebase-service-account.json")
firebase_admin.initialize_app(cred, {
    'storageBucket': 'braille-music-library.appspot.com'
})
db = firestore.client()
bucket = storage.bucket()

def extract(**kwargs):
    url = "https://nhacnheo.com/"
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    songs = []
    for song_div in soup.find_all("div", class_="col-md-4 featured-responsive"):
        song = {}
        song['title'] = song_div.find("h6").text.strip()
        # remove all the text after "-" in the title
        song['title'] = song['title'].split("-")[0].strip()
        song['author'] = song_div.find("ul").find_all("li")[0].text.strip()
        song['song_page'] = song_div.find("a")['href']
        # find lyrics by the div class = "mb-3" in the song page
        song_page = requests.get(song['song_page'])
        soup = BeautifulSoup(song_page.text, 'html.parser')
        soup_song = soup.find_all("div", class_="mb-3")
        # print(len(soup_song))
        if len(soup_song) < 3:
            song['lyrics'] = soup_song[0].text
        elif len(soup_song) >= 4:
            # print all the song that has len(soup_song) >= 4
            # print(song['title'])
            song['lyrics'] = soup_song[2].text
        elif len(soup_song) == 3:
            # print(song['title'])
            song['lyrics'] = soup_song[1].text
        song['lyrics'] = song['lyrics'].replace(song['author'], "").strip()
        song['image_url'] = song_div.find("img")['src']
        #adding https://nhacnheo.com/ as prefix to the image url
        song['image_url'] = "https://nhacnheo.com" + song['image_url']
        songs.append(song)
    save_path = "/opt/airflow/dags/data_extracted.csv"
    pd.DataFrame(songs).to_csv(save_path, index=False)
    return songs

def transform(**kwargs):
    ti = kwargs['ti']
    songs = ti.xcom_pull(task_ids='extract')
    for song in songs:
        song_page = requests.get(song['song_page'])
        soup = BeautifulSoup(song_page.text, 'html.parser')
        # taking href from class featured-btn-wrap <a> tag class btn btn-danger
        pdf_link_element = soup.find("a", class_="btn btn-danger")
        if pdf_link_element:
            pdf_link = pdf_link_element['href']
            song['pdf_url'] = pdf_link
        # when the href exist but not in the format "https://nhacnheo.com/..., add the prefix"
            if not song['pdf_url'].startswith("https://nhacnheo.com/"):
                song['pdf_url'] = "https://nhacnheo.com" + song['pdf_url']
        else:
            song['pdf_url'] = None
    #save the updated songs list to a csv file
    save_path = "/opt/airflow/dags/data_transformed.csv"
    pd.DataFrame(songs).to_csv(save_path, index=False)
    return songs

# upload song info to firestore database
def upload_to_database(song):
    # Check if a document with the same title already exists
    query = db.collection('songs').where('title', '==', song['title']).limit(1)
    existing_docs = list(query.stream())

    # existing_docs_count = sum(1 for _ in existing_docs)  # Count the number of documents

    # if existing_docs_count == 0:
    if len(existing_docs) == 0:
        # No existing document found, add a new one
        file_name = f"{song['title'].replace(' ', '_')}.pdf"
        # file_url = upload_to_firebase(song['pdf_url'], file_name)
        song_data = {
            "title": song['title'],
            "author": song['author'],
            "lyrics": song['lyrics'],
            "original_url": song['pdf_url'],
            "braille_url": None,
            "timestamp": firestore.SERVER_TIMESTAMP
        }
        print(song_data)
        db.collection('songs').add(song_data)
    else:
        existing_doc_ref = existing_docs[0].reference
        existing_doc_ref.update({
            "author": song['author'],
            "lyrics": song['lyrics'],
            "original_url": song['pdf_url'],
            "braille_url": None,
            "timestamp": firestore.SERVER_TIMESTAMP
        })
        print(f"Document with title '{song['title']}' already exists, updated the document.")

# upload pdf file from song['pdf_url'] to firebase storage in folder named original
def upload_pdf_to_firebase(pdf_url, file_name):
    # check if the file with same song title already exists in the storage
    blob = bucket.blob(f"original/{file_name}")
    if not blob.exists():
        response = requests.get(pdf_url)
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_file.write(response.content)
            temp_file.seek(0)
            blob.upload_from_file(temp_file)
        return blob.public_url
    else:
        print(f"File with name '{file_name}' already exists in the storage.")
        return blob.public_url
    
        
        

def load(**kwargs):
    ti = kwargs['ti']
    songs = ti.xcom_pull(task_ids='transform')
    #save file uploaded to a json file
    save_path = "/opt/airflow/dags/data_loaded.json"
    with open(save_path, 'w') as f:
        f.write(str(songs))
    for song in songs:
        upload_to_database(song)
        upload_pdf_to_firebase(song['pdf_url'], f"{song['title'].replace(' ', '_')}.pdf")


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.now(),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'etl_dag',
    default_args=default_args,
    description='ETL DAG for crawling data from nhacnheo.com',
    schedule_interval=timedelta(days=1),
)

extract_task = PythonOperator(
    task_id='extract',
    python_callable=extract,
    provide_context=True,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform',
    python_callable=transform,
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load',
    python_callable=load,
    provide_context=True,
    dag=dag,
)

extract_task >> transform_task >> load_task