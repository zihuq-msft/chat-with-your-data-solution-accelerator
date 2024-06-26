import logging
import requests
from io import BytesIO
from typing import List
from langchain.docstore.document import Document
from langchain.document_loaders.csv_loader import CSVLoader
from .document_loading_base import DocumentLoadingBase
from ..common.source_document import SourceDocument

logger = logging.getLogger(__name__)

class CsvDocumentLoading(DocumentLoadingBase):
    def __init__(self) -> None:
        super().__init__()
        self._csv_args = {
            "delimiter": ",",
            "quotechar": '"',
            "fieldnames": ["primary_investigator", "program_manager", "institution", "cluster", "abstract", "proposal_body"],
        }

    def load(self, document_url: str) -> List[SourceDocument]:
        # log the document URL
        logger.debug(f"Loading document from {document_url}")
        documents = self._load_csv(document_url)
        source_documents: List[SourceDocument] = [
            SourceDocument(
                content=document.page_content,
                source=document.metadata["source"],
            )
            for document in documents
        ]
        return source_documents

    def _download_document(self, document_url: str) -> str:
        response = requests.get(document_url)
        file = BytesIO(response.content).getvalue()
        return file

    def _load_csv(self, document_url: str) -> List[Document]:
        documents: List[Document] = []
        csv_loader = CSVLoader(document_url, csv_args=self._csv_args)
        documents = csv_loader.load()
        return documents
