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
    ConditionalSkill,
)
from azure.search.documents.indexes import SearchIndexerClient
from ..helpers.config.config_helper import IntegratedVectorizationConfig
from ..helpers.env_helper import EnvHelper
from azure.identity import DefaultAzureCredential
from azure.core.credentials import AzureKeyCredential
from urllib.parse import urlparse

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

        embedding_skill_topic = AzureOpenAIEmbeddingSkill(
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
                InputFieldMappingEntry(name="text", source="/document/topic"),
            ],
            outputs=[
                OutputFieldMappingEntry(name="embedding", target_name="topic_vector")
            ],
        )

        embedding_skill_abstract = AzureOpenAIEmbeddingSkill(
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

        embedding_skill_Impact = AzureOpenAIEmbeddingSkill(
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
                InputFieldMappingEntry(name="text", source="/document/Impact"),
            ],
            outputs=[
                OutputFieldMappingEntry(name="embedding", target_name="Impact_vector")
            ],
        )

        embedding_skill_Benchmark = AzureOpenAIEmbeddingSkill(
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
                InputFieldMappingEntry(name="text", source="/document/Benchmark"),
            ],
            outputs=[
                OutputFieldMappingEntry(name="embedding", target_name="Benchmark_vector")
            ],
        )

        embedding_skill_Outcomes = AzureOpenAIEmbeddingSkill(
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
                InputFieldMappingEntry(name="text", source="/document/Outcomes"),
            ],
            outputs=[
                OutputFieldMappingEntry(name="embedding", target_name="Outcomes_vector")
            ],
        )

        embedding_skill_Approach = AzureOpenAIEmbeddingSkill(
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
                InputFieldMappingEntry(name="text", source="/document/Approach"),
            ],
            outputs=[
                OutputFieldMappingEntry(name="embedding", target_name="Approach_vector")
            ],
        )

        embedding_skill_Novelty = AzureOpenAIEmbeddingSkill(
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
                InputFieldMappingEntry(name="text", source="/document/Novelty"),
            ],
            outputs=[
                OutputFieldMappingEntry(name="embedding", target_name="Novelty_vector")
            ],
        )

        embedding_skill_Domain = AzureOpenAIEmbeddingSkill(
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
                InputFieldMappingEntry(name="text", source="/document/Domain"),
            ],
            outputs=[
                OutputFieldMappingEntry(name="embedding", target_name="Domain_vector")
            ],
        )

        embedding_skill_Task = AzureOpenAIEmbeddingSkill(
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
                InputFieldMappingEntry(name="text", source="/document/Task"),
            ],
            outputs=[
                OutputFieldMappingEntry(name="embedding", target_name="Task_vector")
            ],
        )

        embedding_skill_Challenges = AzureOpenAIEmbeddingSkill(
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
                InputFieldMappingEntry(name="text", source="/document/Challenges"),
            ],
            outputs=[
                OutputFieldMappingEntry(name="embedding", target_name="Challenges_vector")
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
                            name="source",
                            source="/document/metadata_storage_path"
                        ),
                        InputFieldMappingEntry(
                            name="title",
                            source="/document/metadata_storage_name"
                        ),
                        # InputFieldMappingEntry(
                        #     name="metadata",
                        #     source="/document/metadata"
                        # ),
                        # InputFieldMappingEntry(
                        #     name="proposal_id",
                        #     source="/document/proposal_id"
                        # ),
                        InputFieldMappingEntry(
                            name="proposal_url",
                            source="/document/proposal_url"
                        ),
                        InputFieldMappingEntry(
                            name="ADO_ID",
                            source="/document/ADO_ID"
                        ),
                        InputFieldMappingEntry(
                            name="ADO_API_URL",
                            source="/document/ADO_API_URL"
                        ),
                        InputFieldMappingEntry(
                            name="primary_investigator",
                            source="/document/primary_investigator",
                        ),
                        InputFieldMappingEntry(
                            name="program_manager",
                            source="/document/program_manager"
                        ),
                        InputFieldMappingEntry(
                            name="institution",
                            source="/document/institution"
                        ),
                        InputFieldMappingEntry(
                            name="cluster",
                            source="/document/cluster"
                        ),
                        InputFieldMappingEntry(
                            name="topic",
                            source="/document/topic"
                        ),
                        InputFieldMappingEntry(
                            name="abstract",
                            source="/document/abstract"
                        ),
                        InputFieldMappingEntry(
                            name="Impact",
                            source="/document/Impact"
                        ),
                        InputFieldMappingEntry(
                            name="Benchmark",
                            source="/document/Benchmark"
                        ),
                        InputFieldMappingEntry(
                            name="Outcomes",
                            source="/document/Outcomes"
                        ),
                        InputFieldMappingEntry(
                            name="Approach",
                            source="/document/Approach"
                        ),
                        InputFieldMappingEntry(
                            name="Novelty",
                            source="/document/Novelty"
                        ),
                        InputFieldMappingEntry(
                            name="Domain",
                            source="/document/Domain"
                        ),
                        InputFieldMappingEntry(
                            name="Task",
                            source="/document/Task"
                        ),
                        InputFieldMappingEntry(
                            name="Challenges",
                            source="/document/Challenges"
                        ),
                        InputFieldMappingEntry(
                            name="has_publications",
                            source="/document/has_publications"
                        ),
                        InputFieldMappingEntry(
                            name="publication_urls",
                            source="/document/publication_urls"
                        ),
                        InputFieldMappingEntry(
                            name="Keywords",
                            source="/document/Keywords"
                        ),
                        InputFieldMappingEntry(
                            name="topic_vector",
                            source="/document/topic_vector",
                        ),
                        InputFieldMappingEntry(
                            name="abstract_vector",
                            source="/document/abstract_vector",
                        ),
                        InputFieldMappingEntry(
                            name="Impact_vector",
                            source="/document/Impact_vector",
                        ),
                        InputFieldMappingEntry(
                            name="Benchmark_vector",
                            source="/document/Benchmark_vector",
                        ),
                        InputFieldMappingEntry(
                            name="Outcomes_vector",
                            source="/document/Outcomes_vector",
                        ),
                        InputFieldMappingEntry(
                            name="Approach_vector",
                            source="/document/Approach_vector",
                        ),
                        InputFieldMappingEntry(
                            name="Novelty_vector",
                            source="/document/Novelty_vector",
                        ),
                        InputFieldMappingEntry(
                            name="Domain_vector",
                            source="/document/Domain_vector",
                        ),
                        InputFieldMappingEntry(
                            name="Task_vector",
                            source="/document/Task_vector",
                        ),
                        InputFieldMappingEntry(
                            name="Challenges_vector",
                            source="/document/Challenges_vector",
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
            skills=[
                embedding_skill_topic,
                embedding_skill_abstract,
                embedding_skill_Impact,
                embedding_skill_Benchmark,
                embedding_skill_Outcomes,
                embedding_skill_Approach,
                embedding_skill_Novelty,
                embedding_skill_Domain,
                embedding_skill_Task,
                embedding_skill_Challenges
            ],
            index_projections=index_projections,
        )

        skillset_result = self.indexer_client.create_or_update_skillset(skillset)
        logger.info(f"{skillset.name} created")
        return skillset_result
