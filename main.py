from fastapi import FastAPI, File, UploadFile, Request
from fastapi.templating import Jinja2Templates
from opensearchpy import OpenSearch
import pdfplumber
import os
import re
from dotenv import load_dotenv
from pdfminer.pdfparser import PDFSyntaxError


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

templates = Jinja2Templates(directory="templates")

usb_directory = "/Volumes/KINGSTON/Kitap Arşivi"

max_size_limit = 1 * 1024 * 1024  # 1gb
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



# USB'deki PDF dosyalarını işleyip OpenSearch'e ekleyen fonksiyon
def process_pdf_and_index(file_path):
    global total_size_processed
    file_size = os.path.getsize(file_path)

    # 20 GB sınırına ulaşıldıysa işlemi durdur
    if total_size_processed + file_size > max_size_limit:
        print("20 GB sınırına ulaşıldı. İşlem durduruluyor.")
        return False

    if not is_valid_pdf(file_path):
        print(f"Invalid or corrupted PDF: {file_path}")
        return True

    with pdfplumber.open(file_path) as pdf:
        text = ''
        for page in pdf.pages:
            text += page.extract_text()

        article = {
            'title': os.path.basename(file_path),
            'content': text
        }
        es.index(index=index_name, body=article)

        total_size_processed += file_size
        print(f"İşlenen toplam veri boyutu: {total_size_processed / (1024 * 1024 * 1024):.2f} GB")

    return True


def process_pdfs_in_directory(directory):
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith(".pdf"):
                file_path = os.path.join(root, file)
                if not process_pdf_and_index(file_path):
                    return

process_pdfs_in_directory(usb_directory)

# endpoint for pdf and text upload

@app.post("/upload/")
async def upload_file(pdf_file: UploadFile = File(...)):
    file_location = f"./{pdf_file.filename}"

    with open(file_location, "wb+") as file_object:
        file_object.write(pdf_file.file.read())

    with pdfplumber.open(file_location) as pdf:
        text = ''
        for page in pdf.pages:
            text += page.extract_text()

        article = {
            'title': pdf_file.filename,
            'content': text
        }
        es.index(index=index_name, body=article)

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

# pip install -r requirements.txt
# uvicorn main:app --reload
# http://localhost:8000/search/?query
