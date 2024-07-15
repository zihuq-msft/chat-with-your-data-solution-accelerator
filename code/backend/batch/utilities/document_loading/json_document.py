import logging
import orjson
import requests
import threading
from enum import Enum
from io import BytesIO
from typing import List
from langchain.docstore.document import Document
from .document_loading_base import DocumentLoadingBase
from ..common.source_document import SourceDocument

logger = logging.getLogger(__name__)


class JsonDocumentSettings:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        with cls._lock:
            if cls._instance is None:
                instance = super(JsonDocumentSettings, cls).__new__(cls)
                instance.__init__()
                cls._instance = instance
            return cls._instance

    class KeyType(Enum):
        CLUSTER = "Cluster Details"
        CONTENT = "Content Fields"
        SUMMARY = "Project Summary"
        PROGRAM_MANAGER = "Program Managers"

    def __init__(self) -> None:
        self._strict_keys = False
        self._valid_key_types = [x for x in self.KeyType]
        self._cluster_keys = [
            "cluster_name",
            "cluster_description",
            "summary",
            "outcomes",
            "approach",
            "Novelty",
            "Challenges",
        ]
        self._cluster_metadata_keys = [
            "afmr_cluster_url",
            "afmr_proposals",
        ]
        self._content_keys = [
            "topic",
            "abstract",
            "Impact",
            "Benchmark",
            "Outcomes",
            "Approach",
            "Novelty",
            "Task",
            "Challenges",
        ]
        self._content_metadata_keys = [
            "program_manager",
            "primary_investigator",
            "institution",
            "cluster",
            "Keywords",
        ]
        self._summary_keys = [
            "afmr_program_summary",
            "afmr_program_managers",
            "afmr_clusters",
            "afmr_program_goals",
        ]
        self._summary_metadata_keys = [
            "abbreviations",
            "afmr_short_url",
            "afmr_url",
            "afmr_github_url",
            "program_manager_count",
            "cluster_count",
            "proposal_count",
            "primary_investigator_count",
            "institution_count",
            "afmr_program_goals_count",
        ]
        self._program_manager_keys = [
            "full_name",
            "email",
        ]
        self._program_manager_metadata_keys = [
            "institution_count",
            "clusters",
            "proposal_count",
            "primary_investigator_count",
        ]
        self._key_fields = {
            self.KeyType.CLUSTER: self._cluster_keys,
            self.KeyType.CONTENT: self._content_keys,
            self.KeyType.SUMMARY: self._summary_keys,
            self.KeyType.PROGRAM_MANAGER: self._program_manager_keys,
        }
        self._metadata_key_fields = {
            self.KeyType.CLUSTER: self._cluster_metadata_keys,
            self.KeyType.CONTENT: self._content_metadata_keys,
            self.KeyType.SUMMARY: self._summary_metadata_keys,
            self.KeyType.PROGRAM_MANAGER: self._program_manager_metadata_keys,
        }

    def get_strict_keys(self) -> bool:
        return self._strict_keys

    def validate_key_type(self, key_type: KeyType) -> bool:
        return key_type in self._valid_key_types

    def get_metadata_key_fields(self, field_type: KeyType) -> List[str]:
        return self._metadata_key_fields[field_type]

    def get_key_fields(self, field_type: KeyType) -> List[str]:
        return self._key_fields[field_type]

    @classmethod
    def clear_instance(cls):
        if cls._instance is not None:
            cls._instance = None


class JsonDocumentLoading(DocumentLoadingBase):
    def __init__(self) -> None:
        super().__init__()
        self.settings = JsonDocumentSettings()

    def load(self, document_url: str) -> List[SourceDocument]:
        # log the document URL
        logger.debug(f"Loading document from {document_url}")
        self.document_url = document_url
        documents = self._load_json()

        source_documents: List[SourceDocument] = [
            SourceDocument(
                content=document.page_content,
                source=self.document_url,
            )
            for document in documents
        ]
        return source_documents

    def _get_json_document_type(self) -> JsonDocumentSettings.KeyType:
        if "_cluster" in self.document_url:
            return JsonDocumentSettings.KeyType.CLUSTER
        elif "_summary" in self.document_url:
            return JsonDocumentSettings.KeyType.SUMMARY
        elif "_pm" in self.document_url:
            return JsonDocumentSettings.KeyType.PROGRAM_MANAGER
        else:
            return JsonDocumentSettings.KeyType.CONTENT

    def _download_document(self) -> str:
        response = requests.get(self.document_url)
        file = BytesIO(response.content).getvalue()
        return file

    def _load_schema_from_dict(self, data_dict: dict) -> str:
        return_dict = {
            "content": {},
            "metadata": {},
        }
        keyType = self._get_json_document_type()
        keys = self.settings.get_key_fields(keyType)
        metadata_keys = self.settings.get_metadata_key_fields(keyType)

        for k in keys:
            value = data_dict.get(k)
            if value is None and self.settings.get_strict_keys():
                raise ValueError(
                    f"JSON file at path {self.document_url} must contain the field '{k}'"
                )
            return_dict["content"][k] = value

        for t in metadata_keys:
            value = data_dict.get(t)
            if value is None and self.settings.get_strict_keys():
                raise ValueError(
                    f"JSON file at path {self.document_url} must contain the field '{t}'"
                )
            return_dict["metadata"][t] = value
        return return_dict

    def _load_json(self) -> List[Document]:
        documents: List[Document] = []

        # Load JSON file from URL
        data = orjson.loads(self._download_document())
        # page_content = []

        if not isinstance(data, dict):
            raise ValueError(
                f"JSON file at path: {self.document_url} must be a dictionary."
            )
        else:
            data_dict = self._load_schema_from_dict(data)
            # page_content = data_dict.get("content")

        metadata = data_dict.get("metadata")
        documents.append(Document(page_content=str(data_dict), metadata=metadata))

        return documents
