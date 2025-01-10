In this repo, we'll show how to implement ACL in your RAG chatbot using Mosaic AI Vector Search.

All notebooks tested with DBR 15.4 ML. All notebooks tested with DBR 15.4 ML. For detailed implementation and additional insights, check out the Databricks community blog post: Mastering RAG Chatbot Security.

--------------------------------------------------------------------

## Repository Structure

### 1. Configuration
- **`config/rag_chain_config.yaml`**  
  This YAML file defines the configuration for the RAG chain, including vector search parameters, ACL rules, and metadata filtering settings.

### 2. Sample Data
- **`sample_data/source_data.csv`**  
  A CSV file containing sample text with the department data (e.g., Finance, HR). The department column will be used to demonstrate ACL and metadata filtering.

### 3. Code Examples
- **`00_data_preparation.py`**  
  Prepares and processes the sample data for vector search indexing, including cleaning and metadata tagging.
- **`01_vector_search_index_creation.py`**  
  Creates a vector search index using Mosaic AI Vector Search API, linking metadata and ACL rules for secure retrieval.
- **`02_single_turn_chatbot_with_acl.py`**  
  Implements a single-turn RAG chatbot that integrates ACL to ensure secure and filtered responses.
- **`03_deploy_chatbot.py`**  
  Demonstrates how to deploy the RAG chatbot, including endpoint setup and configuration for production.
- **`04_test_endpoint.py`**  
  Provides examples of testing the deployed chatbot endpoint, including sample queries and ACL compliance validation.

### 4. Licensing
- **`LICENSE`**  
  Specifies the licensing terms for this repository.
- **`NOTICE`**  
  Includes attribution and notices for third-party libraries used in this project.

--------------------------------------------------------------------

## Getting Started

1. **Prepare Data**: Run the data preparation script ```00_data_preparation.py``` to process the sample data. If you have a Delta Table in Databricks or have your own sample data, change the code to integrate.

2. **Create Vector Search Index**: Execute the script ```01_vector_search_index_creation.py``` to build the vector search index with ACL rules.

3. **Run the Chatbot**: Start the chatbot with ACL integration using script ```02_single_turn_chatbot_with_acl.py```. The example is shown with single turn. The structure can be applied to multi turn as well. 

4. **Deploy the Chatbot**: Deploy the chatbot using ```03_deploy_chatbot.py``` on Databricks as a Model Serving endpoint

5. **Test the Chatbot**: Test its endpoint for secure responses with ```04_test_endpoint.py```.
