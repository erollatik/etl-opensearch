# ETL OpenSearch Project

This project is a FastAPI-based application integrated with OpenSearch for handling PDF uploads and enabling full-text search functionality. The application processes PDF files uploaded by users, indexes their content into OpenSearch, and allows for efficient keyword searches within the documents.

## Features

- **PDF Upload**: Upload PDF files and process them for content extraction.
- **Full-Text Search**: Perform keyword searches across the indexed PDF content using OpenSearch.
- **Data Size Limit**: Automatically stops processing when a specified data size limit is reached (e.g., 100 MB).
- **Error Handling**: Skips invalid or corrupted PDFs and logs errors.

## Prerequisites

- **Python 3.8+**
- **FastAPI**
- **OpenSearch**
- **pdfplumber**
- **dotenv**
- **Jinja2** (for templating)

## Installation

1. **Clone the repository:**
   ```bash
   git clone https://github.com/your-username/etl-opensearch.git
   cd etl-opensearch


2. **Install the required Python packages:**

    pip install -r requirements.txt

3. **Start OpenSearch and OpenSearch Dashboards using Docker:** Use the docker-compose.yml file in the project to run OpenSearch and OpenSearch Dashboards:

    docker-compose up -d

4. **Create a .env file:** Create a .env file in the root directory of the project and add the following information:

    OPENSEARCH_HOST=localhost
    OPENSEARCH_PORT=9200
    OPENSEARCH_SCHEME=http

5. **Start the application:** Run the FastAPI application using Uvicorn:

    uvicorn main:app --reload

6. **Access the application:**

    Open your browser and visit http://localhost:8000 to use the application.

## Usage

**PDF Upload:**

Use the "Upload" button on the main page to upload a new PDF file.

**Keyword Search:**

Enter the keyword you want to search for in the search box and click the "Search" button.
The sentences containing the keyword will be displayed.

## Requirements

Python 3.7+
Docker
OpenSearch and OpenSearch Dashboards

## License
This project is licensed under the MIT License. See the LICENSE file for more details.
