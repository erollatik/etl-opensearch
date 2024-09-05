from fastapi import FastAPI, File, UploadFile, Request
from fastapi.templating import Jinja2Templates
from opensearchpy import OpenSearch, helpers
import pdfplumber
import os
import re
from dotenv import load_dotenv
from pdfminer.pdfparser import PDFSyntaxError
from concurrent.futures import ThreadPoolExecutor
from opensearchpy import OpenSearch, exceptions

# OpenSearch bağlantı ayarları
es = OpenSearch(
    hosts=[{'host': 'localhost', 'port': 9200, 'scheme': 'http'}]
)

index_name = 'articles'

# İndeksin var olup olmadığını kontrol etme ve yoksa oluşturma
if not es.indices.exists(index=index_name):
    es.indices.create(index=index_name)
    print(f"Index '{index_name}' created.")
else:
    print(f"Index '{index_name}' already exists.")


load_dotenv()

opensearch_host = os.getenv("OPENSEARCH_HOST")
opensearch_port = int(os.getenv("OPENSEARCH_PORT"))
opensearch_scheme = os.getenv("OPENSEARCH_SCHEME")

app = FastAPI()

es = OpenSearch(
    hosts=[{'host': opensearch_host, 'port': opensearch_port, 'scheme': opensearch_scheme}]
)

if es.ping():
    print("Connected to OpenSearch!")
else:
    print("Couldn't connect to OpenSearch")

index_name = 'articles'

# OpenSearch'teki refresh interval'ı kapatmak performansı artırabilir
es.indices.put_settings(
    index=index_name,
    body={"index": {"refresh_interval": "-1"}}
)

templates = Jinja2Templates(directory="templates")

usb_directory = "/Users/erolatik/Desktop/Kitap Arşivi"

max_size_limit = 20 * 1024 * 1024 * 1024  # 20 GB
total_size_processed = 0

def is_valid_pdf(file_path):
    try:
        with open(file_path, 'rb') as f:
            pdfplumber.open(f)
        return True
    except PDFSyntaxError:
        return False
    except Exception as e:
        print(f"Error processing file {file_path}: {e}")
        return False

def process_pdf(file_path):
    global total_size_processed
    file_size = os.path.getsize(file_path)

    if total_size_processed + file_size > max_size_limit:
        print("20 GB sınırına ulaşıldı. İşlem durduruluyor.")
        return None

    if not is_valid_pdf(file_path):
        print(f"Invalid or corrupted PDF: {file_path}")
        return None

    with pdfplumber.open(file_path) as pdf:
        text = ''.join(page.extract_text() for page in pdf.pages)

    total_size_processed += file_size
    print(f"İşlenen toplam veri boyutu: {total_size_processed / (1024 * 1024 * 1024):.2f} GB")

    return {
        "_op_type": "index",
        "_index": index_name,
        "_source": {
            'title': os.path.basename(file_path),
            'content': text
        }
    }

def bulk_index(actions):
    if actions:
        helpers.bulk(es, actions)
        print(f"{len(actions)} actions indexed.")

def process_pdfs_in_directory(directory):
    actions = []
    with ThreadPoolExecutor(max_workers=24) as executor:  # Aynı anda çalışacak iş parçacığı sayısı
        futures = []
        for root, _, files in os.walk(directory):
            for file in files:
                if file.endswith(".pdf"):
                    file_path = os.path.join(root, file)
                    future = executor.submit(process_pdf, file_path)
                    futures.append(future)

        # Tüm işlemler tamamlandığında sonuçları işle
        for future in futures:
            action = future.result()
            if action:
                actions.append(action)
                if len(actions) >= 1000:  # Belirli bir sayıya ulaşınca bulk işlemi yap
                    bulk_index(actions)
                    actions.clear()

        # Kalan işlemleri gönder
        bulk_index(actions)

process_pdfs_in_directory(usb_directory)

# endpoint for pdf and text upload
@app.post("/upload/")
async def upload_file(pdf_file: UploadFile = File(...)):
    bulk_actions = []  # Endpoint içinde yerel bulk_actions listesi
    file_location = f"./{pdf_file.filename}"

    with open(file_location, "wb+") as file_object:
        file_object.write(pdf_file.file.read())

    with pdfplumber.open(file_location) as pdf:
        text = ''.join(page.extract_text() for page in pdf.pages)

    article = {
        'title': pdf_file.filename,
        'content': text
    }

    # Bulk API için indeksleme işlemi hazırlama
    action = {
        "_op_type": "index",
        "_index": index_name,
        "_source": article
    }
    bulk_actions.append(action)

    # Bulk işlemleri belirli bir sayıya ulaştığında gönder
    if len(bulk_actions) >= 1000:
        helpers.bulk(es, bulk_actions)
        bulk_actions.clear()

    # Son kalan işlemleri gönder
    if bulk_actions:
        helpers.bulk(es, bulk_actions)

    os.remove(file_location)

    return {"message": "File uploaded successfully"}

# endpoint for search
@app.get("/search/")
async def search_articles(request: Request, query: str = "", size: int = 100):
    results = []
    if query:
        search_query = {
            "query": {
                "match": {
                    "content": {
                        "query": query,
                        "operator": "and",
                        "fuzziness": "AUTO"
                    }
                }
            },
            "size": size
        }

        response = es.search(index=index_name, body=search_query)
        for hit in response['hits']['hits']:
            title = hit["_source"]["title"]
            content = hit["_source"]["content"]

            sentences = re.split(r'(?<!\w\.\w.)(?<![A-Z][a-z]\.)(?<=\.|\?)\s', content)

            for sentence in sentences:
                if query.lower() in sentence.lower():
                    results.append({"title": title, "highlighted_text": sentence.strip()})

    return templates.TemplateResponse("search.html", {"request": request, "query": query, "results": results})

# İndeksleme işlemi tamamlandıktan sonra refresh interval'i tekrar etkinleştir
es.indices.put_settings(
    index=index_name,
    body={"index": {"refresh_interval": "1s"}}
)

# pip install -r requirements.txt
# uvicorn main:app --reload
# http://localhost:8000/search/?query
