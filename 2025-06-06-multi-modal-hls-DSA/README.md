# Multi Modal RAG on Databricks

## Databricks Blogpost
<TBD>

## Overview

This is a demonstration of doing multi-modal RAG on Databricks. We will be using sample summary of benefits PDFs to query alongside a text query and retrieve relevant information about a patient. 

### Key Features

- Multi-Modal Embeddings from the ColNomic Embedding Model
- Hosting the Embedding model on Databricks 
- Utilizing DSPy for programatic orchestraction 
- Utilization of existing Databricks tools like vector search, genie, Mosaic AI and more to create an end to end solution

## Prerequisites

- Python 3.11+
- A Databricks workspace (for AI model serving)

## FYA
This code will spin up a Vector Search Endpoint and a GPU Model Serving Endpoint for the Embedding Model. Please remember to destroy these resources if they are not needed or use existing resources to avoid costs. 

