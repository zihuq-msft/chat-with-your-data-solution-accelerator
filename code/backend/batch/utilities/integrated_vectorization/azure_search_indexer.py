import logging
from azure.search.documents.indexes.models import SearchIndexer, FieldMapping
from azure.search.documents.indexes import SearchIndexerClient
from ..helpers.env_helper import EnvHelper
from azure.identity import DefaultAzureCredential
from azure.core.credentials import AzureKeyCredential

logger = logging.getLogger(__name__)


class AzureSearchIndexer:
    def __init__(self, env_helper: EnvHelper):
        self.env_helper = env_helper
        self.indexer_client = SearchIndexerClient(
            self.env_helper.AZURE_SEARCH_SERVICE,
            (
                AzureKeyCredential(self.env_helper.AZURE_SEARCH_KEY)
                if self.env_helper.is_auth_type_keys()
                else DefaultAzureCredential()
            ),
        )

    def create_or_update_indexer(self, indexer_name: str, skillset_name: str):
        indexer = SearchIndexer(
            name=indexer_name,
            description="Indexer to index documents and generate embeddings",
            skillset_name=skillset_name,
            target_index_name=self.env_helper.AZURE_SEARCH_INDEX,
            data_source_name=self.env_helper.AZURE_SEARCH_DATASOURCE_NAME,
            parameters={
                "configuration": {
                    "dataToExtract": "contentAndMetadata",
                    "parsingMode": "default",
                    "imageAction": "generateNormalizedImages",
                }
            },
            field_mappings=[
                FieldMapping(
                    source_field_name="metadata_storage_path",
                    target_field_name="source",
                ),
                FieldMapping(
                    source_field_name="/document/normalized_images/*/text",
                    target_field_name="text",
                ),
                FieldMapping(
                    source_field_name="/document/normalized_images/*/layoutText",
                    target_field_name="layoutText",
                ),
            ],
        )
        indexer_result = self.indexer_client.create_or_update_indexer(indexer)
        # Run the indexer
        self.indexer_client.run_indexer(indexer_name)
        logger.info(
            f" {indexer_name} is created and running. If queries return no results, please wait a bit and try again."
        )
        return indexer_result

    def create_or_update_researcher_indexer(
        self, indexer_name: str, skillset_name: str
    ):
        indexer = SearchIndexer(
            name=indexer_name,
            description="Indexer to index documents and generate embeddings, specifically for Researcher AI Assistant",
            skillset_name=skillset_name,
            target_index_name=self.env_helper.AZURE_SEARCH_INDEX,
            data_source_name=self.env_helper.AZURE_SEARCH_DATASOURCE_NAME,
            parameters={
                "configuration": {
                    "dataToExtract": "contentAndMetadata",
                    "parsingMode": "json",
                    "allowSkillsetToReadFileData": "true",
                }
            },
            field_mappings=[
                # FieldMapping(
                #     source_field_name="proposal_id",
                #     target_field_name="proposal_id",
                # ),
                FieldMapping(
                    source_field_name="proposal_url",
                    target_field_name="proposal_url",
                ),
                FieldMapping(
                    source_field_name="ADO_ID",
                    target_field_name="ADO_ID",
                ),
                FieldMapping(
                    source_field_name="ADO_API_URL",
                    target_field_name="ADO_API_URL",
                ),
                FieldMapping(
                    source_field_name="has_publications",
                    target_field_name="has_publications",
                ),
                FieldMapping(
                    source_field_name="publication_urls",
                    target_field_name="publication_urls",
                ),
                FieldMapping(
                    source_field_name="metadata_storage_path",
                    target_field_name="source",
                ),
                FieldMapping(
                    source_field_name="metadata_storage_name",
                    target_field_name="title",
                ),
                FieldMapping(
                    source_field_name="primary_investigator",
                    target_field_name="primary_investigator",
                ),
                FieldMapping(
                    source_field_name="program_manager",
                    target_field_name="program_manager",
                ),
                FieldMapping(
                    source_field_name="institution",
                    target_field_name="institution",
                ),
                FieldMapping(
                    source_field_name="cluster",
                    target_field_name="cluster",
                ),
                FieldMapping(
                    source_field_name="topic",
                    target_field_name="topic",
                ),
                FieldMapping(
                    source_field_name="abstract",
                    target_field_name="abstract",
                ),
                FieldMapping(
                    source_field_name="Impact",
                    target_field_name="Impact",
                ),
                FieldMapping(
                    source_field_name="Benchmark",
                    target_field_name="Benchmark",
                ),
                FieldMapping(
                    source_field_name="Outcomes",
                    target_field_name="Outcomes",
                ),
                FieldMapping(
                    source_field_name="Approach",
                    target_field_name="Approach",
                ),
                FieldMapping(
                    source_field_name="Novelty",
                    target_field_name="Novelty",
                ),
                FieldMapping(
                    source_field_name="Domain",
                    target_field_name="Domain",
                ),
                FieldMapping(
                    source_field_name="Task",
                    target_field_name="Task",
                ),
                FieldMapping(
                    source_field_name="Challenges",
                    target_field_name="Challenges",
                ),
                FieldMapping(
                    source_field_name="Keywords",
                    target_field_name="Keywords",
                ),
                FieldMapping(
                    source_field_name="topic_vector",
                    target_field_name="topic_vector",
                ),
                FieldMapping(
                    source_field_name="abstract_vector",
                    target_field_name="abstract_vector",
                ),
                FieldMapping(
                    source_field_name="Impact_vector",
                    target_field_name="Impact_vector",
                ),
                FieldMapping(
                    source_field_name="Benchmark_vector",
                    target_field_name="Benchmark_vector",
                ),
                FieldMapping(
                    source_field_name="Outcomes_vector",
                    target_field_name="Outcomes_vector",
                ),
                FieldMapping(
                    source_field_name="Approach_vector",
                    target_field_name="Approach_vector",
                ),
                FieldMapping(
                    source_field_name="Novelty_vector",
                    target_field_name="Novelty_vector",
                ),
                FieldMapping(
                    source_field_name="Domain_vector",
                    target_field_name="Domain_vector",
                ),
                FieldMapping(
                    source_field_name="Task_vector",
                    target_field_name="Task_vector",
                ),
                FieldMapping(
                    source_field_name="Challenges_vector",
                    target_field_name="Challenges_vector",
                ),
            ],
        )
        indexer_result = self.indexer_client.create_or_update_indexer(indexer)
        # Run the indexer
        self.indexer_client.run_indexer(indexer_name)
        logger.info(
            f" {indexer_name} (Researcher AI Assitant) is created and running. If queries return no results, please wait a bit and try again."
        )
        return indexer_result

    def run_indexer(self, indexer_name: str):
        self.indexer_client.reset_indexer(indexer_name)
        self.indexer_client.run_indexer(indexer_name)
        logger.info(
            f" {indexer_name} is created and running. If queries return no results, please wait a bit and try again."
        )

    def indexer_exists(self, indexer_name: str):
        return indexer_name in [
            name for name in self.indexer_client.get_indexer_names()
        ]
