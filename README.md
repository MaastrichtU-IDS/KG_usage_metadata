# Knowledge graph usage metadata: Insights from SPARQL log analysis



## Datasets

### RDF Knowledge Graphs (KGs)

Our analysis spans Wikidata SPARQL query logs from 2017 to 2018. To ensure alignment between schema and query logs, we downloaded the closest available Wikidata versions:

- **Wikidata 2017**: [Internet Archive](https://archive.org/download/wikibase-wikidatawiki-20170821)  
- **Wikidata 2018**: [Internet Archive](https://archive.org/download/wikibase-wikidatawiki-20180205)  

Both versions were hosted on Blazegraph on a local server to analyze SPARQL schema coverage changes over time.

For **Bio2RDF KG**, the analysis covers two periods of SPARQL query logs: one from 2013 and another from 2019. However, due to the unavailability of datasets from those years, we used the current **2024 public endpoint**:

- **Bio2RDF SPARQL Endpoint**: [https://Bio2RDF.org/sparql/](https://Bio2RDF.org/sparql/)  

### SPARQL Query Logs

The query logs were retrieved from multiple sources:

- **Linked SPARQL Queries Dataset (LSQ) 2.0**:  
  - **SPARQL Endpoint**: [https://lsq.data.dice-research.org/sparql](https://lsq.data.dice-research.org/sparql)  
  - This dataset contains queries from 24 datasets, including 23 Bio2RDF datasets and one Wikidata dataset.

- **Bio2RDF Query Logs**:  
  - **Dumontier Lab Repository**: [https://download.dumontierlab.com/Bio2RDF/logs/](https://download.dumontierlab.com/Bio2RDF/logs/)  

- **Wikidata Query Logs**:  
  - **International Center for Computational Logic (ICCL)**: [https://iccl.inf.tu-dresden.de/web/Wikidata_SPARQL_Logs/en](https://iccl.inf.tu-dresden.de/web/Wikidata_SPARQL_Logs/en)  
  - This dataset includes all queries and organic queries for **Interval 1** and **Interval 7**.

## Calculating Schema Coverage and Usage Analysis

To calculate **SPARQL Schema Coverage (SC)**, use the following steps:

1. **Extract all schema elements** by running the code in the **`KG-Schema-extractors`** folder.  
2. **Extract used schema elements from SPARQL query logs** by running the code in the **`Schema-coverage-method`** folder.  
3. **Compute SC (%)** using the formula:  
   \[
   SC (\%) = \left( \frac{USE}{TSE} \right) \times 100
   \]
   where:  
   - **TSE (Total Schema Elements):** All distinct types and predicates in the KG.  
   - **USE (Used Schema Elements):** The subset of schema elements found in user SPARQL queries.  

To perform the **usage pattern analysis** as proposed in the paper, run the code in the **`KG-Usage-analysis`** folder.


The **generated usage metadata** for **Bio2RDF** and **Wikidata** KGs can be found in the **`generated-usage-metadata`** folder.  

