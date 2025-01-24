# Enable Multi-Agents + Classic ML using Databricks and DSPy!
This repository contains code accompanying the blog [Enable Multi-Agents + Classic ML using Databricks and DSPy!] https://medium.com/@austinchoi/enable-multi-agents-classic-ml-using-databricks-and-dspy-918f78c16e3a

This is primarily an educational demonstration of how you can design Agents to work together to answer a wide variety of questions or figure out and complete tasks in the face of adversity. 

Through this demo, you should learn the following: 
1. You should be using LLMs + Traditional ML (The Vision Agent will demonstrate this) 
2. Tool calling is almost mandatory to ensure up to date and accurate information (The Pokemon Agent will demonstrate this) 
3. We can make RAG better with Agents (The Databricks Agent will demonstrate)

A chatbot is not the only use case for this code. In fact, it is likely other Use Case like ETL pipelines would benefit more from this solution. However, to demonstrate the interaction between agents, it is show through a Gradio Interface 

Below is a diagram of how the Agents work together to answer a user's question: 
![dspy_agent_diagram.png](./config/dspy_agent_diagram.png)

## Code 
[Multi-Agent Recreation](https://github.com/databricks-solutions/databricks-blogposts/blob/main/dspy-multi-agent-with-classic-ML/Multi_Agent_Recreation.py) Much of the set up occurs in the config file to set up your Databricks Environment

[config](https://github.com/databricks-solutions/databricks-blogposts/blob/main/dspy-multi-agent-with-classic-ML/config) This folder contains the notebooks and assets that will help recreate the demo one for one