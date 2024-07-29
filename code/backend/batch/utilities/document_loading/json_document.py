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
        CONTENT_PUBLICATIONS = "Content Publications"
        SUMMARY = "Project Summary"
        PROGRAM_MANAGER = "Program Managers"
        PUBLICATIONS = "Publications"

    def __init__(self) -> None:
        self._strict_keys = False
        self._valid_key_types = [x for x in self.KeyType]
        self._cluster_keys = [
            "name",
            "description",
            "research_questions",
            "research_approach",
            "key_takeaways",
            "research_trends",
            "executive_summary",
        ]
        self._cluster_metadata_keys = [
            "document_type",
            "afmr_url",
            # "afmr_proposals", # Proposal IDs were updated to be ADO work item IDs, which the current data does not have
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
            "publications",
        ]
        self._content_publications_keys = [
            "topic",
            "research_questions",
            "executive_summary",
        ]
        self._content_metadata_keys = [
            "proposal_id",
            "program_manager",
            "primary_investigator",
            "institution",
            "cluster",
            "source_url",
            "publications",
        ]
        self._content_publications_metadata_keys = [
            "publication_url",
            "publication_id",
            "authors",
            "publication_date",
        ]
        self._summary_keys = [
            "abstract",
            "summary",
            "program_managers",
            "goals",
            "research_questions",
            "research_approach",
            "key_takeaways",
            "research_trends",
        ]
        self._summary_metadata_keys = [
            "abbreviations",
            "afmr_short_url",
            "afmr_url",
            "afmr_github_url",
            "project_program_manager_count",
            "project_cluster_count",
            "project_proposal_count",
            "project_primary_investigator_count",
            "project_institution_count",
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
        self._publications_keys = [
            "topic",
            "summary",
            "research_questions",
            "research_approach",
            "research_takeaways",
            "research_conclusion",
            "executive_summary",
            # "body", # This is a large field and should be excluded (seems to be causing issues)
        ]
        self._publications_metadata_keys = [
            "source_url",
            "authors",
            "publication_date",
        ]
        self._key_fields = {
            self.KeyType.CLUSTER: self._cluster_keys,
            self.KeyType.CONTENT: self._content_keys,
            self.KeyType.CONTENT_PUBLICATIONS: self._content_publications_keys,
            self.KeyType.SUMMARY: self._summary_keys,
            self.KeyType.PROGRAM_MANAGER: self._program_manager_keys,
            self.KeyType.PUBLICATIONS: self._publications_keys,
        }
        self._metadata_key_fields = {
            self.KeyType.CLUSTER: self._cluster_metadata_keys,
            self.KeyType.CONTENT: self._content_metadata_keys,
            self.KeyType.CONTENT_PUBLICATIONS: self._content_publications_metadata_keys,
            self.KeyType.SUMMARY: self._summary_metadata_keys,
            self.KeyType.PROGRAM_MANAGER: self._program_manager_metadata_keys,
            self.KeyType.PUBLICATIONS: self._publications_metadata_keys,
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
        if "_cluster_info.json" in self.document_url:
            return JsonDocumentSettings.KeyType.CLUSTER
        elif "afmr_summary2.json" in self.document_url:
            return JsonDocumentSettings.KeyType.SUMMARY
        elif "_pm.json" in self.document_url:
            return JsonDocumentSettings.KeyType.PROGRAM_MANAGER
        elif "_pub.json" in self.document_url:
            return JsonDocumentSettings.KeyType.PUBLICATIONS
        else:
            return JsonDocumentSettings.KeyType.CONTENT

    def _download_document(self) -> str:
        response = requests.get(self.document_url)
        file = BytesIO(response.content).getvalue()
        return file

    def _get_content_publications(self, data_dict: dict, return_dict: dict):
        publications = data_dict.get("publications")
        if publications == []:
            return_dict["content"]["publications"] = []
            return
        if publications is None and self.settings.get_strict_keys():
            raise ValueError(
                f"JSON file at path {self.document_url} must contain the field 'publications'"
            )
        publication_data = []
        for publication in publications:
            publication_dict = {}
            keys = self.settings.get_key_fields(
                JsonDocumentSettings.KeyType.CONTENT_PUBLICATIONS
            )
            for k in keys:
                value = publication.get(k)
                if value is None and self.settings.get_strict_keys():
                    raise ValueError(
                        f"JSON file at path {self.document_url} must contain the field '{k}'"
                    )
                publication_dict[k] = value
            publication_data.append(publication_dict)
        return_dict["content"]["publications"] = publication_data

    def _get_content_publications_metadata(self, data_dict: dict, return_dict: dict):
        publications = data_dict.get("publications")
        if publications == []:
            return_dict["metadata"]["publications"] = []
            return
        if publications is None and self.settings.get_strict_keys():
            raise ValueError(
                f"JSON file at path {self.document_url} must contain the field 'publications'"
            )
        publication_data = []
        for publication in publications:
            publication_dict = {}
            keys = self.settings.get_metadata_key_fields(
                JsonDocumentSettings.KeyType.CONTENT_PUBLICATIONS
            )
            for k in keys:
                value = publication.get(k)
                if value is None and self.settings.get_strict_keys():
                    raise ValueError(
                        f"JSON file at path {self.document_url} must contain the field '{k}'"
                    )
                publication_dict[k] = value
            publication_data.append(publication_dict)
        return_dict["metadata"]["publications"] = publication_data

    def _load_schema_from_dict(self, data_dict: dict) -> str:
        return_dict = {
            "content": {},
            "metadata": {},
        }
        keyType = self._get_json_document_type()
        keys = self.settings.get_key_fields(keyType)
        metadata_keys = self.settings.get_metadata_key_fields(keyType)

        for k in keys:
            if k == "publications":
                self._get_content_publications(data_dict, return_dict)
                continue
            value = data_dict.get(k)
            if value is None and self.settings.get_strict_keys():
                raise ValueError(
                    f"JSON file at path {self.document_url} must contain the field '{k}'"
                )
            return_dict["content"][k] = value

        for t in metadata_keys:
            if t == "publications":
                self._get_content_publications(data_dict, return_dict)
                continue
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
