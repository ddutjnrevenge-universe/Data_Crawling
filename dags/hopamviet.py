from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup
import re
import firebase_admin
from firebase_admin import credentials, firestore, storage
import tempfile
import os
from urllib.parse import urlparse, unquote

# Firebase setup
cred = credentials.Certificate("/opt/airflow/dags/firebase-service-account.json")
firebase_admin.initialize_app(cred, {
    'storageBucket': 'braille-music-library.appspot.com'
})
db = firestore.client()
bucket = storage.bucket()

def fetch_html(url):
    response = requests.get(url)
    return response.content

def extract_data_from_row(url, row_index=0):
    html_content = fetch_html(url)
    soup = BeautifulSoup(html_content, 'html.parser')
    
    rows = soup.find('tbody').find_all('tr')
    
    if not rows or row_index >= len(rows):
        print(f"No <tr> found at index {row_index}.")
        return None
    
    row = rows[row_index]
    
    second_td = row.find_all('td')[1]
    second_link = second_td.find('a')['href']
    
    fourth_td = row.find_all('td')[3]
    fourth_link = fourth_td.find('a')['href']

    files = fetch_files_from_link(second_link)
    title, author, tags, lyrics = fetch_details_from_link(fourth_link)
    
    song = {
        "title": title,
        "author": author,
        "tags": tags,
        "lyrics": lyrics,
        "files": files,
        "second_link": second_link,
        "fourth_link": fourth_link,
    }

    return song

def fetch_files_from_link(url):
    html_content = fetch_html(url)
    soup = BeautifulSoup(html_content, 'html.parser')
    
    image_tags = soup.select('div.ct-box img')
    images = [img['src'] for img in image_tags]
    
    iframe_tags = soup.select('div.ct-box iframe')
    files = []
    for iframe in iframe_tags:
        src = iframe.get('src')
        if src and "google.com/viewer?url=" in src:
            file_url = src.split("url=")[1].split("&")[0]
            files.append(file_url)
    
    files.extend(images)
    
    return files

def fetch_details_from_link(url):
    html_content = fetch_html(url)
    soup = BeautifulSoup(html_content, 'html.parser')
    
    title = soup.find('h1', class_='h3 d-inline-block').text.strip()

    author_section = soup.find('i', title="Sáng tác")
    author = ""
    if author_section:
        author_text_node = author_section.find_next_sibling(text=True)
        if author_text_node:
            author_links = []
            current = author_text_node.find_next_sibling('a')
            while current and current.name == 'a':
                author_links.append(current.text.strip())
                current = current.find_next_sibling()
                if isinstance(current, str) and '|' in current:
                    break
            author = ' & '.join(author_links)

    tags_section = soup.find('i', class_='fas fa-book')
    tags = ""
    if tags_section:
        tags = tags_section.find_next('a').text.strip()

    lyrics_div = soup.find('div', id='lyric')
    lyrics = lyrics_div.text.strip()
    
    lyrics = re.sub(r'\[.*?\] ', '', lyrics)
    lyrics = re.sub(r'\[.*?\]', '', lyrics)    

    return title, author, tags, lyrics

def upload_to_database(song):
    query = db.collection('hopamviet').where('title', '==', song['title']).limit(1)
    existing_docs = list(query.stream())

    if len(existing_docs) == 0:
        file_name = f"{song['title'].replace(' ', '_')}.pdf"
        song_data = {
            "title": song['title'],
            "author": song['author'],
            "tags": song['tags'],
            "lyrics": song['lyrics'],
            "origin_url": song.get('files', []),
            "braille_url": None,
            "timestamp": firestore.SERVER_TIMESTAMP
        }
        db.collection('hopamviet').add(song_data)
    else:
        existing_doc_ref = existing_docs[0].reference
        existing_doc_ref.update({
            "author": song['author'],
            "tags": song['tags'],
            "lyrics": song['lyrics'],
            "origin_url": song.get('files', []),
            "braille_url": None,
            "timestamp": firestore.SERVER_TIMESTAMP
        })

def upload_file_to_firebase(file_url, title):
    # Determine the file extension from the URL
    parsed_url = urlparse(file_url)
    file_name = os.path.basename(parsed_url.path)
    file_name = unquote(file_name)  # Decode any URL-encoded characters
    file_extension = os.path.splitext(file_name)[1]

    # Ensure the title is URL-safe and append the file extension
    safe_title = title.replace(' ', '_')
    file_name = f"{safe_title}{file_extension}"

    blob = bucket.blob(f"origin/hopamviet/{file_name}")
    if not blob.exists():
        response = requests.get(file_url)
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_file.write(response.content)
            temp_file.seek(0)
            blob.upload_from_file(temp_file)
        return blob.public_url
    else:
        return blob.public_url

def crawl_pages(start_url, end_url, step=100):
    base_url = start_url.rsplit('/', 1)[0]
    start_index = int(start_url.rsplit('/', 1)[1])
    end_index = int(end_url.rsplit('/', 1)[1])

    songs = []
    for i in range(start_index, end_index + 1, step):
        url = f"{base_url}/{i}"
        try:
            html_content = fetch_html(url)
            soup = BeautifulSoup(html_content, 'html.parser')
            rows = soup.find('tbody').find_all('tr')
            max_row_index = min(99, len(rows) - 1)  # Crawl up to row 99 or the max row index
            for row_index in range(max_row_index + 1):
                song = extract_data_from_row(url, row_index=row_index)
                if song:
                    songs.append(song)
        except Exception as e:
            print(f"Error processing {url}: {e}")
    return songs

def extract(**kwargs):
    songs = crawl_pages("https://hopamviet.vn/sheet/index/0", "https://hopamviet.vn/sheet/index/7000", step=100)
    return songs

def transform(**kwargs):
    ti = kwargs['ti']
    songs = ti.xcom_pull(task_ids='extract')
    for song in songs:
        for file_url in song.get('files', []):
            file_name = f"{song['title'].replace(' ', '_')}{os.path.splitext(file_url)[1]}"
            song['file_url'] = upload_file_to_firebase(file_url, song['title'])
    return songs

def load(**kwargs):
    ti = kwargs['ti']
    songs = ti.xcom_pull(task_ids='transform')
    for song in songs:
        upload_to_database(song)

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
    'etl_dag_hopamviet',
    default_args=default_args,
    description='ETL DAG for crawling data from hopamviet.vn',
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
