# from googleapiclient.discovery import build
# from google.auth.transport.requests import Request
# from google.oauth2.service_account import Credentials
# import firebase_admin
# from firebase_admin import credentials, firestore, storage, initialize_app
# import tempfile
# import http
# from airflow import DAG
# from airflow.operators.python_operator import PythonOperator
# from datetime import datetime, timedelta


# # Google Drive setup
# SCOPES = ['https://www.googleapis.com/auth/drive.readonly']
# SERVICE_ACCOUNT_FILE = '/opt/airflow/dags/firebase-service-account.json'
# # Firebase setup
# cred = credentials.Certificate("/opt/airflow/dags/firebase-service-account.json")
# firebase_admin.initialize_app(cred, {
#     'storageBucket': 'braille-music-library.appspot.com'
# })
# db = firestore.client()
# bucket = storage.bucket()

# def extract(**kwargs):
#     # Authenticate with Google Drive API
#     creds = Credentials.from_service_account_file(
#         SERVICE_ACCOUNT_FILE,
#         scopes=SCOPES
#     )

#     service = build('drive', 'v3', credentials=creds)

#     # List all files in the specified folders
#     folder_ids = ['1vNFBKQx4lk5mX7dCMIodsEUs_XxV3rRn']
#     songs = []

#     for folder_id in folder_ids:
#         results = service.files().list(q=f"'{folder_id}' in parents",
#                                        fields='files(id, name, webViewLink)').execute()
#         items = results.get('files', [])

#         for item in items:
#             file_id = item['id']
#             file_name = item['name']
#             file_url = item['webViewLink']
            
#             # Download file content or metadata as needed
#             # Upload file to Firebase Storage
#             upload_to_firebase_storage(file_id, file_name)

#             # Create entry for Firestore
#             song_data = {
#                 "title": extract_title_from_filename(file_name),
#                 "author": "Unknown",
#                 "lyrics": "Unknown",
#                 "braille_url": "Unknown",
#                 "origin_url": get_firebase_storage_url(file_name),
#                 "tags": get_tags_from_folder(folder_id),
#                 # Add other fields as needed
                
#             }
#             songs.append(song_data)
#             print(f"Processed file: {file_name}")

#     # Store songs data in XCom for downstream tasks
#     return songs

# # Function to upload file to Firebase Storage
# def upload_to_firebase_storage(file_id, file_name):
#     # Implement your code to upload to Firebase Storage here
#     # check if file with same song title exist in the storage
#     blob = bucket.blob(f"origin/{file_name}")
#     if not blob.exists():
#         # import file to Firebase Storage from Google Drive
#         with tempfile.NamedTemporaryFile() as temp_file:
#             request = service.files().get_media(fileId=file_id)
#             media_request = http.MediaIoBaseDownload(temp_file, request)
#             while True:
#                 try:
#                     download_progress, done = media_request.next_chunk()
#                 except:
#                     break
#             temp_file.seek(0)
#             blob.upload_from_file(temp_file)
#     else:
#         print(f"File with name '{file_name}' already exists in the storage.")
#     return blob.public_url

# # Function to extract title from file name
# def extract_title_from_filename(file_name):
#     # Implement logic to extract title from file name
#     # Example: return file_name.split('.')[1].strip()
#     # e.g. for file named "10. Dàn đồng ca mùa hạ", return "Dàn đồng ca mùa hạ"
#     return file_name.split('.')[1].strip()

# # Function to get Firebase Storage URL
# def get_firebase_storage_url(file_name):
#     # Implement logic to get Firebase Storage URL
#     # Example: return f"https://storage.googleapis.com/your-bucket/{file_name}"
#     # fill in "origin_url" field in the collection "songs" with the link to the file in Firebase Storage of equivalent song
#     return f"https://storage.googleapis.com/braille-music-library.appspot.com/origin/{file_name}"

# # Function to get tags from folder ID
# def get_tags_from_folder(folder_id):
#     # Implement logic to map folder IDs to tags
#     # Example: return "Lớp 1" if folder_id == "1vNFBKQx4lk5mX7dCMIodsEUs_XxV3rRn" else "Unknown"
#     # for 5 folders in google drive "Lớp 1", "Lớp 2", "Lớp 3", "Lớp 4", "Lớp 5"
#     # we label each song with the corresponding folder name that it belongs to
#     folder_ids = ['1vNFBKQx4lk5mX7dCMIodsEUs_XxV3rRn']
#     tags = ["Lớp 1", "Lớp 2", "Lớp 3", "Lớp 4", "Lớp 5"]
#     return tags[folder_ids.index(folder_id)] if folder_id in folder_ids else "Unknown"

# def transform(**kwargs):
#     ti = kwargs['ti']
#     songs = ti.xcom_pull(task_ids='extract')

#     for song in songs:
#         # Additional transformations or data enrichment if needed
#         song["author"] = "Unknown"
#         song["lyrics"] = "Lyrics not available"
#         song["braille_url"] = None

#     # Store transformed data in XCom for downstream tasks
#     return songs

# def load(**kwargs):
#     ti = kwargs['ti']
#     songs = ti.xcom_pull(task_ids='transform')

#     for song in songs:
#         # Check if song with same title exists in Firestore
#         query = db.collection('songs').where('title', '==', song['title']).limit(1)
#         existing_docs = list(query.stream())

#         if len(existing_docs) == 0:
#             # No existing document found, add a new one
#             db.collection('songs').add(song)
#             print(f"Added new song: {song['title']}")
#         else:
#             # Existing document found, update it
#             existing_doc_ref = existing_docs[0].reference
#             existing_doc_ref.update(song)
#             print(f"Updated existing song: {song['title']}")

# default_args = {
#     'owner': 'airflow',
#     'depends_on_past': False,
#     'start_date': datetime.now(),
#     'email_on_failure': False,
#     'email_on_retry': False,
#     'retries': 1,
#     'retry_delay': timedelta(minutes=5),
# }

# dag = DAG(
#     'drive_dag',
#     default_args=default_args,
#     description='ETL DAG for processing files from Google Drive to Firestore',
#     schedule_interval=timedelta(days=1),
# )

# extract_task = PythonOperator(
#     task_id='extract',
#     python_callable=extract,
#     provide_context=True,
#     dag=dag,
# )

# transform_task = PythonOperator(
#     task_id='transform',
#     python_callable=transform,
#     provide_context=True,
#     dag=dag,
# )

# load_task = PythonOperator(
#     task_id='load',
#     python_callable=load,
#     provide_context=True,
#     dag=dag,
# )

# extract_task >> transform_task >> load_task
from googleapiclient.discovery import build
from google.auth.transport.requests import Request
from google.oauth2.service_account import Credentials
import firebase_admin
from firebase_admin import credentials, firestore, storage, initialize_app
import tempfile
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import io
from googleapiclient.http import MediaIoBaseDownload

# Google Drive setup
SCOPES = ['https://www.googleapis.com/auth/drive.readonly']
SERVICE_ACCOUNT_FILE = '/opt/airflow/dags/firebase-service-account.json'

# Firebase setup
cred = credentials.Certificate(SERVICE_ACCOUNT_FILE)
firebase_admin.initialize_app(cred, {
    'storageBucket': 'braille-music-library.appspot.com'
})
db = firestore.client()
bucket = storage.bucket()

def extract(**kwargs):
    # Authenticate with Google Drive API
    creds = Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=SCOPES
    )
    service = build('drive', 'v3', credentials=creds)

    # List all files in the specified folders
    folder_ids = ['1vNFBKQx4lk5mX7dCMIodsEUs_XxV3rRn']
    songs = []

    for folder_id in folder_ids:
        results = service.files().list(q=f"'{folder_id}' in parents",
                                       fields='files(id, name, webViewLink)').execute()
        items = results.get('files', [])

        for item in items:
            file_id = item['id']
            file_name = item['name']
            
            # Upload file to Firebase Storage
            file_url = upload_to_firebase_storage(service, file_id, file_name)

            # Create entry for Firestore
            song_data = {
                "title": extract_title_from_filename(file_name),
                "author": "Unknown",
                "lyrics": "Unknown",
                "braille_url": "Unknown",
                "origin_url": file_url,
                "tags": get_tags_from_folder(folder_id),
            }
            songs.append(song_data)
            print(f"Processed file: {file_name}")

    # Store songs data in XCom for downstream tasks
    return songs

# Function to upload file to Firebase Storage
def upload_to_firebase_storage(service, file_id, file_name):
    # Check if file with same name exists in the storage
    blob = bucket.blob(f"origin/{file_name}")
    if not blob.exists():
        # Import file to Firebase Storage from Google Drive
        with tempfile.NamedTemporaryFile() as temp_file:
            request = service.files().get_media(fileId=file_id)
            media_request = MediaIoBaseDownload(temp_file, request)
            done = False
            while not done:
                _, done = media_request.next_chunk()
            temp_file.seek(0)
            blob.upload_from_file(temp_file)
            print(f"Uploaded file: {file_name}")
    else:
        print(f"File with name '{file_name}' already exists in the storage.")
    return blob.public_url

# Function to extract title from file name
def extract_title_from_filename(file_name):
    # Example: return file_name.split('.')[1].strip()
    # e.g. for file named "10. Dàn đồng ca mùa hạ", return "Dàn đồng ca mùa hạ"
    return file_name.split('.')[1].strip()

# Function to get Firebase Storage URL
def get_firebase_storage_url(file_name):
    return f"https://storage.googleapis.com/braille-music-library.appspot.com/origin/{file_name}"

# Function to get tags from folder ID
def get_tags_from_folder(folder_id):
    folder_ids = ['1vNFBKQx4lk5mX7dCMIodsEUs_XxV3rRn']
    tags = ["Lớp 1", "Lớp 2", "Lớp 3", "Lớp 4", "Lớp 5"]
    return tags[folder_ids.index(folder_id)] if folder_id in folder_ids else "Unknown"

def transform(**kwargs):
    ti = kwargs['ti']
    songs = ti.xcom_pull(task_ids='extract')

    for song in songs:
        song["author"] = "Unknown"
        song["lyrics"] = "Lyrics not available"
        song["braille_url"] = None

    return songs

def load(**kwargs):
    ti = kwargs['ti']
    songs = ti.xcom_pull(task_ids='transform')

    for song in songs:
        query = db.collection('songs').where('title', '==', song['title']).limit(1)
        existing_docs = list(query.stream())

        if len(existing_docs) == 0:
            db.collection('songs').add(song)
            print(f"Added new song: {song['title']}")
        else:
            existing_doc_ref = existing_docs[0].reference
            existing_doc_ref.update(song)
            print(f"Updated existing song: {song['title']}")

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
    'drive_dag',
    default_args=default_args,
    description='ETL DAG for processing files from Google Drive to Firestore',
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
