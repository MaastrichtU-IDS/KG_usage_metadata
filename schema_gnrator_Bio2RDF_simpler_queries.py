import csv
import time
from SPARQLWrapper import SPARQLWrapper, JSON
from urllib.error import HTTPError

# Specific graph and type combinations
graph_type_pairs = [
    ("http://bio2rdf.org/clinicaltrials_resource:bio2rdf.dataset.clinicaltrials.R3", "http://bio2rdf.org/clinicaltrials_vocabulary:Resource"),
    ("http://bio2rdf.org/pharmgkb_resource:bio2rdf.dataset.pharmgkb.R3", "http://bio2rdf.org/ncbigene_vocabulary:Resource"),
    ("http://bio2rdf.org/pharmgkb_resource:bio2rdf.dataset.pharmgkb.R3", "http://bio2rdf.org/clinicaltrials_vocabulary:Resource"),
    ("http://bio2rdf.org/pharmgkb_resource:bio2rdf.dataset.pharmgkb.R3", "http://bio2rdf.org/ctd_vocabulary:Resource"),
    
    ("http://bio2rdf.org/pharmgkb_resource:bio2rdf.dataset.pharmgkb.R3","http://bio2rdf.org/kegg_vocabulary:Resource"),
    ("http://bio2rdf.org/pharmgkb_resource:bio2rdf.dataset.pharmgkb.R3","http://bio2rdf.org/dbsnp_vocabulary:Resource"),
    ("http://bio2rdf.org/pharmgkb_resource:bio2rdf.dataset.pharmgkb.R3","http://bio2rdf.org/pharmgkb_vocabulary:Variation"),
    ("http://bio2rdf.org/ctd_resource:bio2rdf.dataset.ctd.R3","http://bio2rdf.org/ctd_vocabulary:Resource"),
    ("http://bio2rdf.org/ctd_resource:bio2rdf.dataset.ctd.R3","http://bio2rdf.org/ctd_vocabulary:Gene-Disease-Association"),
    ("http://bio2rdf.org/ncbigene_resource:bio2rdf.dataset.ncbigene.R3","http://bio2rdf.org/ncbigene_vocabulary:Resource")
]

# Your SPARQL endpoint
sparql_endpoint = "https://bio2rdf.org/sparql"
sparql = SPARQLWrapper(sparql_endpoint)

# Configuration for retries
MAX_RETRIES = 5
RETRY_DELAY = 60
INITIAL_TIMEOUT = 60  # Initial timeout in seconds
TIMEOUT_INCREMENT = 60  # Increase timeout by 60 seconds on each retry

def execute_sparql_query(query):
    timeout = INITIAL_TIMEOUT
    for attempt in range(MAX_RETRIES):
        try:
            sparql.setQuery(query)
            sparql.setReturnFormat(JSON)
            sparql.setTimeout(timeout)
            return sparql.query().convert()
        except HTTPError as e:
            if e.code == 504:  # Gateway Time-out
                print(f"Timeout error on attempt {attempt + 1} for query: {query}")
                time.sleep(RETRY_DELAY)  # Delay before retrying
                timeout += TIMEOUT_INCREMENT  # Increase timeout for next attempt
            else:
                print(f"HTTP error: {e} on attempt {attempt + 1} for query: {query}")
                break  # Break the loop for non-timeout HTTP errors
        except Exception as e:
            print(f"An error occurred: {e} on attempt {attempt + 1} for query: {query}")
            break  # Break the loop for non-timeout errors
        if attempt == MAX_RETRIES - 1:
            print(f"Failed after {MAX_RETRIES} attempts. Moving to the next query.")
        # If we successfully increased the timeout but still failed, wait before retrying
        time.sleep(RETRY_DELAY)

with open('4faildgraph-types.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(["Class1", "Predicate", "Class2"])  # CSV Header

    for graph, stype in graph_type_pairs:
        # First query to fetch subjects
        query_subjects = f"""
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        SELECT DISTINCT ?s
        WHERE {{
          GRAPH <{graph}> {{
            ?s rdf:type <{stype}> .
          }}
        }}
        
        """
        subjects_results = execute_sparql_query(query_subjects)
        if subjects_results:
            for subject_result in subjects_results["results"]["bindings"]:
                subject = subject_result["s"]["value"]
                
                # Second query to fetch predicates and their object types for each subject
                query_details = f"""
                PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                SELECT DISTINCT ?p ?otype
                WHERE {{
                  GRAPH <{graph}> {{
                    <{subject}> ?p ?o .
                    ?o rdf:type ?otype .
                  }}
                }}
                """
                details_results = execute_sparql_query(query_details)
                if details_results:
                    for detail_result in details_results["results"]["bindings"]:
                        otype = detail_result["otype"]["value"]
                        predicate = detail_result["p"]["value"]
                        writer.writerow([stype, predicate, otype])

print("CSV file '17_March_kg_schema_optimized.csv' has been created with the optimized schema of the KG.")





    for graph, stype in graph_type_pairs:
        
        query_subjects = f"""
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        SELECT DISTINCT ?s
        WHERE {{
          GRAPH <{graph}> {{
            ?s rdf:type <{stype}> .}}}} """ 
              
        query_details = f"""
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            SELECT DISTINCT ?p ?otype
            WHERE {{
                GRAPH <{graph}> {{
                <{subject}> ?p ?o .
                ?o rdf:type ?otype .}}}} """ 
                 
                
