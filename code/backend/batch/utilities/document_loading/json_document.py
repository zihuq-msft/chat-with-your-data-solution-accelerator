import logging
import requests
import orjson
from io import BytesIO
from typing import List
from langchain.docstore.document import Document
from .document_loading_base import DocumentLoadingBase
from ..common.source_document import SourceDocument

logger = logging.getLogger(__name__)

class JsonDocumentLoading(DocumentLoadingBase):
    def __init__(self) -> None:
        super().__init__()
        self._keys_to_load = ["primary_investigator", "program_manager", "institution", "cluster", "abstract", "proposal_body"]
        self._strict_keys = False

    def load(self, document_url: str) -> List[SourceDocument]:
        # log the document URL
        logger.debug(f"Loading document from {document_url}")
        documents = self._load_json(document_url)
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

    def _load_schema_from_dict(self, data_dict: dict, document_url: str) -> str:
        if self._keys_to_load == None:
            return data_dict
        else:
            return_dict = {}
            for k in self._keys_to_load:
                value = data_dict.get(k)
                if value is None and self._strict_keys:
                    raise ValueError(
                        f"JSON file at path {document_url} must contain the field '{k}'"
                    )
                return_dict[k] = value
        return return_dict

    def _load_json(self, document_url: str) -> List[Document]:
        documents: List[Document] = []

        # Load JSON file from URL
        data = orjson.loads(self._download_document(document_url))
        page_content = []

        if not isinstance(data, list):
            raise ValueError(
                f"JSON file at path: {document_url} must be a list of objects and expects each object to contain the fields {self._keys_to_load}"
            )
        else:
            for entry in data:
                data_dict = self._load_schema_from_dict(entry, document_url)
                page_content.append(data_dict)

        metadata = {"source": document_url}
        documents.append(Document(page_content=str(page_content), metadata=metadata))

        return documents
