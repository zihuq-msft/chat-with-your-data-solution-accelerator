import logging
from azure.search.documents.indexes.models import (
    SplitSkill,
    InputFieldMappingEntry,
    OutputFieldMappingEntry,
    AzureOpenAIEmbeddingSkill,
    OcrSkill,
    MergeSkill,
    SearchIndexerIndexProjections,
    SearchIndexerIndexProjectionSelector,
    SearchIndexerIndexProjectionsParameters,
    IndexProjectionMode,
    SearchIndexerSkillset,
)
from azure.search.documents.indexes import SearchIndexerClient
from ..helpers.config.config_helper import IntegratedVectorizationConfig
from ..helpers.env_helper import EnvHelper
from azure.identity import DefaultAzureCredential
from azure.core.credentials import AzureKeyCredential

logger = logging.getLogger(__name__)


class AzureSearchSkillset:
    def __init__(
        self,
        env_helper: EnvHelper,
        integrated_vectorization_config: IntegratedVectorizationConfig,
    ):
        self.env_helper = env_helper
        self.indexer_client = SearchIndexerClient(
            self.env_helper.AZURE_SEARCH_SERVICE,
            (
                AzureKeyCredential(self.env_helper.AZURE_SEARCH_KEY)
                if self.env_helper.is_auth_type_keys()
                else DefaultAzureCredential()
            ),
        )
        self.integrated_vectorization_config = integrated_vectorization_config

    def create_skillset(self):
        skillset_name = f"{self.env_helper.AZURE_SEARCH_INDEX}-skillset"

        ocr_skill = OcrSkill(
            description="Extract text (plain and structured) from image",
            context="/document/normalized_images/*",
            inputs=[
                InputFieldMappingEntry(
                    name="image",
                    source="/document/normalized_images/*",
                )
            ],
            outputs=[
                OutputFieldMappingEntry(name="text", target_name="text"),
                OutputFieldMappingEntry(name="layoutText", target_name="layoutText"),
            ],
        )

        merge_skill = MergeSkill(
            description="Merge text from OCR and text from document",
            context="/document",
            inputs=[
                InputFieldMappingEntry(name="text", source="/document/content"),
                InputFieldMappingEntry(
                    name="itemsToInsert", source="/document/normalized_images/*/text"
                ),
                InputFieldMappingEntry(
                    name="offsets", source="/document/normalized_images/*/contentOffset"
                ),
            ],
            outputs=[
                OutputFieldMappingEntry(name="mergedText", target_name="merged_content")
            ],
        )

        split_skill = SplitSkill(
            description="Split skill to chunk documents",
            text_split_mode="pages",
            context="/document",
            maximum_page_length=self.integrated_vectorization_config.max_page_length,
            page_overlap_length=self.integrated_vectorization_config.page_overlap_length,
            inputs=[
                InputFieldMappingEntry(name="text", source="/document/merged_content"),
            ],
            outputs=[OutputFieldMappingEntry(name="textItems", target_name="pages")],
        )

        embedding_skill = AzureOpenAIEmbeddingSkill(
            description="Skill to generate embeddings via Azure OpenAI",
            context="/document/pages/*",
            resource_uri=self.env_helper.AZURE_OPENAI_ENDPOINT,
            deployment_id=self.env_helper.AZURE_OPENAI_EMBEDDING_MODEL,
            api_key=(
                self.env_helper.OPENAI_API_KEY
                if self.env_helper.is_auth_type_keys()
                else None
            ),
            inputs=[
                InputFieldMappingEntry(name="text", source="/document/pages/*"),
            ],
            outputs=[
                OutputFieldMappingEntry(name="embedding", target_name="content_vector")
            ],
        )

        index_projections = SearchIndexerIndexProjections(
            selectors=[
                SearchIndexerIndexProjectionSelector(
                    target_index_name=self.env_helper.AZURE_SEARCH_INDEX,
                    parent_key_field_name="id",
                    source_context="/document/pages/*",
                    mappings=[
                        InputFieldMappingEntry(
                            name="content", source="/document/pages/*"
                        ),
                        InputFieldMappingEntry(
                            name="content_vector",
                            source="/document/pages/*/content_vector",
                        ),
                        InputFieldMappingEntry(name="title", source="/document/title"),
                        InputFieldMappingEntry(
                            name="source", source="/document/metadata_storage_path"
                        ),
                    ],
                ),
            ],
            parameters=SearchIndexerIndexProjectionsParameters(
                projection_mode=IndexProjectionMode.SKIP_INDEXING_PARENT_DOCUMENTS
            ),
        )

        skillset = SearchIndexerSkillset(
            name=skillset_name,
            description="Skillset to chunk documents and generating embeddings",
            skills=[ocr_skill, merge_skill, split_skill, embedding_skill],
            index_projections=index_projections,
        )

        skillset_result = self.indexer_client.create_or_update_skillset(skillset)
        logger.info(f"{skillset.name} created")
        return skillset_result

    def create_researcher_skillset(self):
        skillset_name = f"{self.env_helper.AZURE_SEARCH_INDEX}-researcher-skillset"

        embedding_skill = AzureOpenAIEmbeddingSkill(
            description="Skill to generate embeddings via Azure OpenAI",
            context="/document",
            resource_uri=self.env_helper.AZURE_OPENAI_ENDPOINT,
            deployment_id=self.env_helper.AZURE_OPENAI_EMBEDDING_MODEL,
            api_key=(
                self.env_helper.OPENAI_API_KEY
                if self.env_helper.is_auth_type_keys()
                else None
            ),
            inputs=[
                InputFieldMappingEntry(name="text", source="/document/abstract"),
            ],
            outputs=[
                OutputFieldMappingEntry(name="embedding", target_name="abstract_vector")
            ],
        )

        index_projections = SearchIndexerIndexProjections(
            selectors=[
                SearchIndexerIndexProjectionSelector(
                    target_index_name=self.env_helper.AZURE_SEARCH_INDEX,
                    parent_key_field_name="id",
                    source_context="/document",
                    mappings=[
                        InputFieldMappingEntry(
                            name="source", source="/document/metadata_storage_path"
                        ),
                        InputFieldMappingEntry(
                            name="primary_investigator",
                            source="/document/primary_investigator",
                        ),
                        InputFieldMappingEntry(
                            name="program_manager", source="/document/program_manager"
                        ),
                        InputFieldMappingEntry(
                            name="institution", source="/document/institution"
                        ),
                        InputFieldMappingEntry(
                            name="cluster", source="/document/cluster"
                        ),
                        InputFieldMappingEntry(
                            name="abstract", source="/document/abstract"
                        ),
                        InputFieldMappingEntry(
                            name="Impact", source="/document/Impact"
                        ),
                        InputFieldMappingEntry(
                            name="Keywords", source="/document/Keywords"
                        ),
                        InputFieldMappingEntry(
                            name="abstract_vector",
                            source="/document/abstract_vector",
                        ),
                    ],
                ),
            ],
            parameters=SearchIndexerIndexProjectionsParameters(
                projection_mode=IndexProjectionMode.INCLUDE_INDEXING_PARENT_DOCUMENTS
            ),
        )

        skillset = SearchIndexerSkillset(
            name=skillset_name,
            description="Skillset to chunk documents and generating embeddings",
            skills=[embedding_skill],
            index_projections=index_projections,
        )

        skillset_result = self.indexer_client.create_or_update_skillset(skillset)
        logger.info(f"{skillset.name} created")
        return skillset_result
