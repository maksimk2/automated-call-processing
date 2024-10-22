# Anomaly detection using embeddings and GenAI

This repository contains the code accompanying the blog [Anomaly detection using embeddings and GenAI](https://community.databricks.com/t5/technical-blog/anomaly-detection-using-embeddings-and-genai/ba-p/95564). 
<br>In this blog, we explore how machine learning techniques, particularly leveraging embeddings and large language models (LLMs), can improve fraud detection by identifying outliers and patterns that are otherwise difficult to spot. 
<br>The code demonstrates the different approaches discussed and provides implementations for each technique.

[pipeline_emb.png](./data/pipeline_emb.png "pipeline_emb.png")

## Code

* [1_anomaly_detection_pca](https://github.com/databricks-solutions/databricks-blogposts/blob/main/genai-anomaly-detection/1_anomaly_detection_pca.py): Demonstrates the application of a classic machine learning dimensionality reduction technique, Principal Component Analysis (PCA), for detecting anomalies in embeddings.
* [2_hybrid_function-calling-anomaly-examples](https://github.com/databricks-solutions/databricks-blogposts/blob/main/genai-anomaly-detection/2_hybrid_function-calling-anomaly-examples.py): Demonstrates a hybrid approach that combines traditional anomaly detection models with LLMs and tools, offering a more comprehensive and interpretable solution for identifying anomalies. 
* [_resources](https://github.com/databricks-solutions/databricks-blogposts/tree/main/genai-anomaly-detection/_resources): Contains the code to create the catalog, schema and to ingest a synthetic dataset for demonstartion of the techniques discussed in the blog.
* [data](https://github.com/databricks-solutions/databricks-blogposts/tree/main/genai-anomaly-detection/data): Contains a synthetic dataset and an output example from notebook `1_anomaly_detection_pca` 

