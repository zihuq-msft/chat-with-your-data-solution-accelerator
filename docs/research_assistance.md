# CWYD Research Assistant

## Overview
The CWYD Research Assistant is designed to help researchers and academics efficiently manage and interact with a large collection of research documents, including both unstructured and structured data types such as PDF, text, docx, JSON, and CSV files. It utilizes advanced natural language processing capabilities to provide accurate and contextually relevant responses to user queries about the documents.


## Research Assistant Infrastructure Configuration

The following is the CWYD infrastructure configuration that we suggest to optimize the performance and functionality of the Research Assistant:

- **Azure Semantic Search**: Utilize Azure Semantic Search to efficiently index and search research documents. This provides powerful search capabilities and integration with other Azure services.

- **Azure Cognitive Search Top K 15**: Set the Top K parameter to 15 to retrieve the top 15 most relevant documents. This configuration helps in providing precise and relevant search results for user queries.

- **Azure Search Integrated Vectorization**: Enable integrated vectorization in Azure Search to improve the semantic understanding and relevance of search results. This enhances the Research Assistant's ability to provide contextually accurate answers.

- **Azure OpenAI Models gpt-4o and text-embedding-3-large**: Leverage the Azure OpenAI models gpt-4o and text-embedding-3-large for advanced natural language processing capabilities. These models are well-suited for handling complex research queries and providing detailed and contextually appropriate responses.

- **Orchestration Strategy: Semantic Kernel**: Implement the Semantic Kernel orchestration strategy to effectively manage the integration and interaction between different components of the infrastructure. This strategy ensures seamless operation and optimal performance of the Research Assistant.

- **Conversation Flow Options**: Setting `CONVERSATION_FLOW` enables running advanced AI models like GPT-4o and text-embedding-3-large on your own enterprise data without needing to train or fine-tune models.

By following these infrastructure configurations, you can enhance the efficiency, accuracy, and overall performance of the CWYD Research Assistant, ensuring it meets the high demands and expectations of researchers and academics.


## Updating Configuration Fields
- See the `Set and Get Values` section of the [LOCAL_DEPLOYMENT.md](./LOCAL_DEPLOYMENT.md#set-and-get-values) document for setting up and using environment variables within the solution.

To apply the suggested configurations in your deployment, update the following fields accordingly:
- **Azure Semantic Search**: Set `AZURE_SEARCH_USE_SEMANTIC_SEARCH` to `true`
- **Azure Cognitive Search Top K 15**: Set `AZURE_SEARCH_TOP_K` to `15`.
- **Azure Search Integrated Vectorization**: Set `AZURE_SEARCH_USE_INTEGRATED_VECTORIZATION` to `true`.
- **Azure OpenAI API Version**: Set `AZURE_OPENAI_API_VERSION` to `2024-05-01-preview`.
- **Azure OpenAI Model**: Set `AZURE_OPENAI_MODEL`  to `research-assist-gpt-4o`.
- **Azure OpenAI Model Name**: Set `AZURE_OPENAI_MODEL_NAME` to `gpt-4o`. (could be different based on the name of the Azure OpenAI model deployment)
- **Azure OpenAI Model Name Version**: Set `AZURE_OPENAI_MODEL_VERSION` to `2024-05-13`.
- **Azure OpenAI Embeddings Model**: Set `AZURE_OPENAI_EMBEDDING_MODEL`  to `text-embedding-3-large`.
- **Azure OpenAI Embeddings Model Name**: Set `AZURE_OPENAI_EMBEDDING_MODEL_NAME` to `text-embedding-3-large`. (could be different based on the name of the Azure OpenAI model deployment)
- **Azure OpenAI Embeddings Model Name Version**: Set `AZURE_OPENAI_EMBEDDING_MODEL_VERSION` to `1`.
- **Azure OpenAI Max Tokens**: Set `AZURE_OPENAI_MAX_TOKENS` to `4096`.
- **Azure Search Dimensions**: Set `AZURE_SEARCH_DIMENSIONS` to `3072`.
- **Conversation Flow Options**: Set `CONVERSATION_FLOW` to `byod`
- **Orchestration Strategy**: Set `ORCHESTRATION_STRATEGY` to `semantic_kernel`.


## Admin Configuration
In the admin panel, there is a dropdown to select the CWYD Research Assistant. The options are:

- **Default**: CWYD default prompt.

<!-- TODO: Include screenshot similar to ![UnSelected](images/cwyd_admin_legal_unselected.png)  but for Research Assistant -->

- **Selected**: Research Assistant prompt.

<!-- TODO: Include screenshot similar to ![Checked](images/cwyd_admin_legal_selected.png)  but for Research Assistant -->

When the user selects "Research Assistant," the user prompt textbox will update to the Research Assistant prompt. When the user selects the default, the user prompt textbox will update to the default prompt. Note that if the user has a custom prompt in the user prompt textbox, selecting an option from the dropdown will overwrite the custom prompt with the default or research assistant prompt. Ensure to **Save the Configuration** after making this change.

## Research Assistant Prompt
The Research Assistant prompt configuration ensures that the AI responds accurately based on the given context, handling a variety of tasks such as summarizing research papers, extracting key insights, and providing relevant references. Below is the detailed prompt configuration:

```plaintext
## Summary Research Papers
Context:
{sources}
- You are a research assistant.
```

You can see the [Research Assistant Prompt](../code/backend/batch/utilities/helpers/config/default_research_assistant_prompt.txt) file for more details.

## Sample Research Data
We have added sample research data in the [Research Assistant sample Docs](../data/research_data) folder. This data can be used to test and demonstrate the Research Assistant's capabilities.

## Conclusion
This README provides an overview of the CWYD Research Assistant prompt, instructions for updating the prompt configuration, and examples of questions and answers. Ensure you follow the guidelines for updating the prompt to maintain consistency and accuracy in responses.
