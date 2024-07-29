from typing import Optional, Type
import hashlib
import json
from urllib.parse import urlparse
from .source_document import SourceDocument

class SourceDocumentEnhanced(SourceDocument):
    def __init__(
        self,
        content: str,
        source: str,
        id: Optional[str] = None,
        title: Optional[str] = None,
        chunk: Optional[int] = None,
        offset: Optional[int] = None,
        page_number: Optional[int] = None,
        chunk_id: Optional[str] = None,
        enhanced_metadata: Optional[dict] = None,
    ):
        super().__init__(content, source, id, title, chunk, offset, page_number, chunk_id)
        self.enhanced_metadata = enhanced_metadata

    @classmethod
    def FromSourceDocument(cls, source_document: SourceDocument):
        return cls(
            source_document.content,
            source_document.source,
            source_document.id,
            source_document.title,
            source_document.chunk,
            source_document.offset,
            source_document.page_number,
            source_document.chunk_id,
        )

    @classmethod
    def ToSourceDocument(cls, source_document_enhanced: "SourceDocumentEnhanced"):
        return SourceDocument(
            source_document_enhanced.content,
            source_document_enhanced.source,
            source_document_enhanced.id,
            source_document_enhanced.title,
            source_document_enhanced.chunk,
            source_document_enhanced.offset,
            source_document_enhanced.page_number,
            source_document_enhanced.chunk_id,
        )

    def __str__(self):
        return f"SourceDocumentEnhanced(id={self.id}, title={self.title}, source={self.source}, chunk={self.chunk}, offset={self.offset}, page_number={self.page_number}, chunk_id={self.chunk_id}), enhanced_metadata={self.enhanced_metadata})"

    def __eq__(self, other):
        if isinstance(self, other.__class__):
            return (
                super().__eq__(other)
                and self.enhanced_metadata == other.enhanced_metadata
            )
        return False

    def to_json(self):
        return json.dumps(self, cls=SourceDocumentEnhancedEncoder)

    @classmethod
    def from_json(cls, json_string):
        return json.loads(json_string, cls=SourceDocumentEnhancedDecoder)

    @classmethod
    def from_dict(cls, dict_obj):
        return cls(
            dict_obj["content"],
            dict_obj["source"],
            dict_obj["id"],
            dict_obj["title"],
            dict_obj["chunk"],
            dict_obj["offset"],
            dict_obj["page_number"],
            dict_obj["chunk_id"],
            dict_obj["enhanced_metadata"]
        )

    @classmethod
    def from_metadata(
        cls: Type["SourceDocumentEnhanced"],
        content: str,
        metadata: dict,
        document_url: str | None,
        idx: int | None
    ) -> "SourceDocumentEnhanced":
        parsed_url = urlparse(document_url)
        file_url = parsed_url.scheme + "://" + parsed_url.netloc + parsed_url.path
        filename = parsed_url.path
        hash_key = hashlib.sha1(f"{file_url}_{idx}".encode("utf-8")).hexdigest()
        hash_key = f"doc_{hash_key}"
        sas_placeholder = (
            "_SAS_TOKEN_PLACEHOLDER_"
            if parsed_url.netloc
            and parsed_url.netloc.endswith(".blob.core.windows.net")
            else ""
        )
        return cls(
            id=metadata.get("id", hash_key),
            content=content,
            source=metadata.get("source", f"{file_url}{sas_placeholder}"),
            title=metadata.get("title", filename),
            chunk=metadata.get("chunk", idx),
            offset=metadata.get("offset"),
            page_number=metadata.get("page_number"),
            chunk_id=metadata.get("chunk_id"),
            # store the metadata in the enhanced_metadata field except for the id, content, source, title, chunk, offset, page_number, and chunk_id
            enhanced_metadata={k: v for k, v in metadata.items() if k not in ["id", "content", "source", "title", "chunk", "offset", "page_number", "chunk_id"]}
        )

    def get_filename(self, include_path=False):
        return super().get_filename(include_path)

    def get_markdown_url(self):
        return super().get_markdown_url()

class SourceDocumentEnhancedEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, SourceDocumentEnhanced):
            return {
                "id": obj.id,
                "content": obj.content,
                "source": obj.source,
                "title": obj.title,
                "chunk": obj.chunk,
                "offset": obj.offset,
                "page_number": obj.page_number,
                "chunk_id": obj.chunk_id,
                "enhanced_metadata": obj.enhanced_metadata
            }
        return super().default(obj)

class SourceDocumentEnhancedDecoder(json.JSONDecoder):
    def decode(self, s, **kwargs):
        obj = super().decode(s, **kwargs)
        return SourceDocumentEnhanced(
            id=obj["id"],
            content=obj["content"],
            source=obj["source"],
            title=obj["title"],
            chunk=obj["chunk"],
            offset=obj["offset"],
            page_number=obj["page_number"],
            chunk_id=obj["chunk_id"],
            enhanced_metadata=obj["enhanced_metadata"]
        )
