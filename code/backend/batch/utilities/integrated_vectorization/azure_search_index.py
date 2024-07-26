import logging
from azure.search.documents.indexes import SearchIndexClient
from azure.search.documents.indexes.models import (
    SearchField,
    SearchFieldDataType,
    SimpleField,
    SearchableField,
    VectorSearch,
    HnswAlgorithmConfiguration,
    HnswParameters,
    VectorSearchAlgorithmMetric,
    ExhaustiveKnnAlgorithmConfiguration,
    ExhaustiveKnnParameters,
    VectorSearchProfile,
    AzureOpenAIVectorizer,
    AzureOpenAIParameters,
    SemanticConfiguration,
    SemanticSearch,
    SemanticPrioritizedFields,
    SemanticField,
    SearchIndex,
)
from ..helpers.env_helper import EnvHelper
from azure.identity import DefaultAzureCredential
from azure.core.credentials import AzureKeyCredential
from ..helpers.llm_helper import LLMHelper
from ..helpers.config.config_helper import ConfigHelper
from ..helpers.config.assistant_strategy import AssistantStrategy

logger = logging.getLogger(__name__)


class AzureSearchIndex:
    _search_dimension: int | None = None

    def __init__(self, env_helper: EnvHelper, llm_helper: LLMHelper):
        self.env_helper = env_helper
        self.llm_helper = llm_helper
        self.index_client = SearchIndexClient(
            self.env_helper.AZURE_SEARCH_SERVICE,
            (
                AzureKeyCredential(self.env_helper.AZURE_SEARCH_KEY)
                if self.env_helper.is_auth_type_keys()
                else DefaultAzureCredential()
            ),
        )

    @property
    def search_dimensions(self) -> int:
        if AzureSearchIndex._search_dimension is None:
            AzureSearchIndex._search_dimension = len(
                self.llm_helper.get_embedding_model().embed_query("Text")
            )
        return AzureSearchIndex._search_dimension

    def create_or_update_index(self):
        config = ConfigHelper.get_active_config_or_default()
        if (
            config.prompts["ai_assistant_type"]
            == AssistantStrategy.RESEARCH_ASSISTANT.value
        ):
            return self.create_or_update_researcher_index()

        # Create a search index
        fields = [
            SimpleField(
                name="id",
                type=SearchFieldDataType.String,
                filterable=True,
            ),
            SearchableField(
                name="content",
                type=SearchFieldDataType.String,
                sortable=False,
                filterable=False,
                facetable=False,
            ),
            SearchField(
                name="content_vector",
                type=SearchFieldDataType.Collection(SearchFieldDataType.Single),
                vector_search_dimensions=self.search_dimensions,
                vector_search_profile_name="myHnswProfile",
            ),
            SearchableField(name="metadata", type=SearchFieldDataType.String),
            SearchableField(
                name="title",
                type=SearchFieldDataType.String,
                filterable=True,
                facetable=True,
            ),
            SearchableField(
                name="source", type=SearchFieldDataType.String, filterable=True
            ),
            SimpleField(
                name="chunk",
                type=SearchFieldDataType.Int32,
                filterable=True,
            ),
            SimpleField(name="offset", type=SearchFieldDataType.Int32, filterable=True),
            SearchField(
                name="chunk_id",
                type=SearchFieldDataType.String,
                key=True,
                sortable=True,
                filterable=True,
                facetable=True,
                analyzer_name="keyword",
            ),
        ]

        vector_search = self.get_vector_search_config()

        semantic_search = self.get_semantic_search_config()

        index = SearchIndex(
            name=self.env_helper.AZURE_SEARCH_INDEX,
            fields=fields,
            vector_search=vector_search,
            semantic_search=semantic_search,
        )
        result = self.index_client.create_or_update_index(index)
        logger.info(f"{result.name} index created successfully.")
        return result

    def get_vector_search_config(self):
        if self.env_helper.is_auth_type_keys():
            azure_open_ai_parameters = AzureOpenAIParameters(
                resource_uri=self.env_helper.AZURE_OPENAI_ENDPOINT,
                deployment_id=self.env_helper.AZURE_OPENAI_EMBEDDING_MODEL,
                api_key=self.env_helper.OPENAI_API_KEY,
            )
        else:
            azure_open_ai_parameters = AzureOpenAIParameters(
                resource_uri=self.env_helper.AZURE_OPENAI_ENDPOINT,
                deployment_id=self.env_helper.AZURE_OPENAI_EMBEDDING_MODEL,
            )

        return VectorSearch(
            algorithms=[
                HnswAlgorithmConfiguration(
                    name="myHnsw",
                    parameters=HnswParameters(
                        m=4,
                        ef_construction=400,
                        ef_search=500,
                        metric=VectorSearchAlgorithmMetric.COSINE,
                    ),
                ),
                ExhaustiveKnnAlgorithmConfiguration(
                    name="myExhaustiveKnn",
                    parameters=ExhaustiveKnnParameters(
                        metric=VectorSearchAlgorithmMetric.COSINE,
                    ),
                ),
            ],
            profiles=[
                VectorSearchProfile(
                    name="myHnswProfile",
                    algorithm_configuration_name="myHnsw",
                    vectorizer="myOpenAI",
                ),
                VectorSearchProfile(
                    name="myExhaustiveKnnProfile",
                    algorithm_configuration_name="myExhaustiveKnn",
                    vectorizer="myOpenAI",
                ),
            ],
            vectorizers=[
                AzureOpenAIVectorizer(
                    name="myOpenAI",
                    kind="azureOpenAI",
                    azure_open_ai_parameters=azure_open_ai_parameters,
                ),
            ],
        )

    def get_semantic_search_config(self):
        semantic_config = SemanticConfiguration(
            name=self.env_helper.AZURE_SEARCH_SEMANTIC_SEARCH_CONFIG,
            prioritized_fields=SemanticPrioritizedFields(
                content_fields=[SemanticField(field_name="content")]
            ),
        )

        return SemanticSearch(configurations=[semantic_config])

    def get_semantic_search_researcher_config(self):
        semantic_config = SemanticConfiguration(
            name=self.env_helper.AZURE_SEARCH_SEMANTIC_SEARCH_CONFIG,
            prioritized_fields=SemanticPrioritizedFields(
                title_field=SemanticField(field_name="title"),
                content_fields=[
                    SemanticField(field_name="topic"),
                    SemanticField(field_name="abstract"),
                    SemanticField(field_name="Impact"),
                    SemanticField(field_name="Benchmark"),
                    SemanticField(field_name="Outcomes"),
                    SemanticField(field_name="Approach"),
                    SemanticField(field_name="Novelty"),
                    SemanticField(field_name="Domain"),
                    SemanticField(field_name="Task"),
                    SemanticField(field_name="Challenges"),
                ],
                keywords_fields=[
                    SemanticField(field_name="cluster"),
                    SemanticField(field_name="program_manager"),
                    SemanticField(field_name="primary_investigator"),
                    SemanticField(field_name="institution"),
                    SemanticField(field_name="Keywords"),
                    # SemanticField(field_name="metadata"),
                    SemanticField(field_name="source"),
                ],
            ),
        )

        return SemanticSearch(configurations=[semantic_config])

    def create_or_update_researcher_index(self):
        fields = [
            SimpleField(
                name="id",
                type=SearchFieldDataType.String,
                filterable=True
            ),
            SearchField(
                name="primary_investigator",
                type=SearchFieldDataType.String,
                filterable=False,
                facetable=False,
                sortable=False,
                index_analyzer_name="keyword",
                search_analyzer_name="keyword",
            ),
            SearchField(
                name="proposal_id",
                type=SearchFieldDataType.String,
                key=True,
                filterable=True,
                sortable=True,
                facetable=True,
                analyzer_name="keyword",
            ),
            SimpleField(
                name="proposal_url",
                type=SearchFieldDataType.String,
            ),
            SearchField(
                name="ADO_ID",
                type=SearchFieldDataType.String,
                filterable=True,
                facetable=True,
                sortable=True,
                index_analyzer_name="keyword",
                search_analyzer_name="keyword",
            ),
            SimpleField(
                name="ADO_API_URL",
                type=SearchFieldDataType.String,
            ),
            SearchField(
                name="program_manager",
                type=SearchFieldDataType.String,
                filterable=True,
                facetable=True,
                sortable=True,
                index_analyzer_name="keyword",
                search_analyzer_name="keyword",
            ),
            SearchableField(
                name="institution",
                type=SearchFieldDataType.String,
                filterable=True,
                facetable=True,
                sortable=True,
                index_analyzer_name="keyword",
                search_analyzer_name="keyword",
            ),
            SearchField(
                name="cluster",
                type=SearchFieldDataType.String,
                filterable=True,
                facetable=True,
                sortable=True,
                index_analyzer_name="keyword",
                search_analyzer_name="keyword",
            ),
            SearchableField(
                name="topic",
                type=SearchFieldDataType.String,
            ),
            SearchableField(
                name="abstract",
                type=SearchFieldDataType.String,
            ),
            SearchableField(
                name="Impact",
                type=SearchFieldDataType.String,
            ),
            SearchableField(
                name="Benchmark",
                type=SearchFieldDataType.String,
            ),
            SearchableField(
                name="Outcomes",
                type=SearchFieldDataType.String,
            ),
            SearchableField(
                name="Approach",
                type=SearchFieldDataType.String,
            ),
            SearchableField(
                name="Novelty",
                type=SearchFieldDataType.String,
            ),
            SearchableField(
                name="Domain",
                type=SearchFieldDataType.String,
            ),
            SearchableField(
                name="Task",
                type=SearchFieldDataType.String,
            ),
            SearchableField(
                name="Challenges",
                type=SearchFieldDataType.String,
            ),
            SearchableField(
                name="has_publications",
                type=SearchFieldDataType.Boolean,
                filterable=True,
                facetable=True,
                sortable=True,
            ),
            SearchField(
                name="publication_urls",
                type=SearchFieldDataType.Collection(SearchFieldDataType.String),
            ),
            SearchField(
                name="Keywords",
                type=SearchFieldDataType.Collection(SearchFieldDataType.String),
            ),
            SearchField(
                name="topic_vector",
                type=SearchFieldDataType.Collection(SearchFieldDataType.Single),
                vector_search_dimensions=self.search_dimensions,
                vector_search_profile_name="myHnswProfile",
            ),
            SearchField(
                name="abstract_vector",
                type=SearchFieldDataType.Collection(SearchFieldDataType.Single),
                vector_search_dimensions=self.search_dimensions,
                vector_search_profile_name="myHnswProfile",
            ),
            SearchField(
                name="Impact_vector",
                type=SearchFieldDataType.Collection(SearchFieldDataType.Single),
                vector_search_dimensions=self.search_dimensions,
                vector_search_profile_name="myHnswProfile",
            ),
            SearchField(
                name="Benchmark_vector",
                type=SearchFieldDataType.Collection(SearchFieldDataType.Single),
                vector_search_dimensions=self.search_dimensions,
                vector_search_profile_name="myHnswProfile",
            ),
            SearchField(
                name="Outcomes_vector",
                type=SearchFieldDataType.Collection(SearchFieldDataType.Single),
                vector_search_dimensions=self.search_dimensions,
                vector_search_profile_name="myHnswProfile",
            ),
            SearchField(
                name="Approach_vector",
                type=SearchFieldDataType.Collection(SearchFieldDataType.Single),
                vector_search_dimensions=self.search_dimensions,
                vector_search_profile_name="myHnswProfile",
            ),
            SearchField(
                name="Novelty_vector",
                type=SearchFieldDataType.Collection(SearchFieldDataType.Single),
                vector_search_dimensions=self.search_dimensions,
                vector_search_profile_name="myHnswProfile",
            ),
            SearchField(
                name="Domain_vector",
                type=SearchFieldDataType.Collection(SearchFieldDataType.Single),
                vector_search_dimensions=self.search_dimensions,
                vector_search_profile_name="myHnswProfile",
            ),
            SearchField(
                name="Task_vector",
                type=SearchFieldDataType.Collection(SearchFieldDataType.Single),
                vector_search_dimensions=self.search_dimensions,
                vector_search_profile_name="myHnswProfile",
            ),
            SearchField(
                name="Challenges_vector",
                type=SearchFieldDataType.Collection(SearchFieldDataType.Single),
                vector_search_dimensions=self.search_dimensions,
                vector_search_profile_name="myHnswProfile",
            ),
            SearchableField(
                name="title",
                type=SearchFieldDataType.String,
                filterable=True,
                facetable=True,
            ),
            # SearchableField(
            #     name="metadata",
            #     type=SearchFieldDataType.String,
            # ),
            SearchableField(
                name="source",
                type=SearchFieldDataType.String,
                filterable=True
            ),
        ]

        vector_search = self.get_vector_search_config()

        semantic_search = self.get_semantic_search_researcher_config()

        index = SearchIndex(
            name=self.env_helper.AZURE_SEARCH_INDEX,
            fields=fields,
            vector_search=vector_search,
            semantic_search=semantic_search,
        )
        result = self.index_client.create_or_update_index(index)
        logger.info(f"{result.name} researcher index created successfully.")
        return result
