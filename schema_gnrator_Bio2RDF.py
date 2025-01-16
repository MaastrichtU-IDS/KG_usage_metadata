import csv
import time
from SPARQLWrapper import SPARQLWrapper, JSON
from urllib.error import HTTPError

# List of your graphs
graphs = ["http://bio2rdf.org/sgd_resource:bio2rdf.dataset.sgd.R3",
"http://bio2rdf.org/taxonomy_resource:bio2rdf.dataset.taxonomy.R3",
"http://bio2rdf.org/homologene_resource:bio2rdf.dataset.homologene.R3",
"http://bio2rdf.org/interpro_resource:bio2rdf.dataset.interpro.R3",
"http://bio2rdf.org/bioportal_resource:bio2rdf.dataset.bioportal.R3",
"http://bio2rdf.org/clinicaltrials_resource:bio2rdf.dataset.clinicaltrials.R3",
"http://bio2rdf.org/kegg_resource:bio2rdf.dataset.kegg.R3",
"http://bio2rdf.org/pharmgkb_resource:bio2rdf.dataset.pharmgkb.R3",
"http://bio2rdf.org/hgnc_resource:bio2rdf.dataset.hgnc.R3",
"http://bio2rdf.org/mesh_resource:bio2rdf.dataset.mesh.R3",
"http://bio2rdf.org/omim_resource:bio2rdf.dataset.omim.R3",
"http://bio2rdf.org/sider_resource:bio2rdf.dataset.sider.R3",
"http://bio2rdf.org/apo_resource:bio2rdf.dataset.apo.R3",
"http://bio2rdf.org/ctd_resource:bio2rdf.dataset.ctd.R3",
"http://bio2rdf.org/go_resource:bio2rdf.dataset.go.R3",
"http://bio2rdf.org/hp_resource:bio2rdf.dataset.hp.R3",
"http://bio2rdf.org/drugbank_resource:bio2rdf.dataset.drugbank.R3",
"http://bio2rdf.org/mgi_resource:bio2rdf.dataset.mgi.R3",
"http://bio2rdf.org/goa_resource:bio2rdf.dataset.goa.R3",
"http://bio2rdf.org/ndc_resource:bio2rdf.dataset.ndc.R3",
"http://bio2rdf.org/wormbase_resource:bio2rdf.dataset.wormbase.R3",
"http://bio2rdf.org/lsr_resource:bio2rdf.dataset.lsr.R3",
"http://bio2rdf.org/affymetrix_resource:bio2rdf.dataset.affymetrix.R3",
"http://bio2rdf.org/ncbigene_resource:bio2rdf.dataset.ncbigene.R3",
"http://bio2rdf.org/eco_resource:bio2rdf.dataset.eco.R3",
"http://bio2rdf.org/irefindex_resource:bio2rdf.dataset.irefindex.R3"]

# Your SPARQL endpoint
sparql_endpoint = "https://bio2rdf.org/sparql"

# Initialize SPARQL wrapper
sparql = SPARQLWrapper(sparql_endpoint)

# Configuration for retries
MAX_RETRIES = 10
RETRY_DELAY = 60
INITIAL_TIMEOUT = 60  # Initial timeout in seconds
TIMEOUT_INCREMENT = 55  # Increase timeout by 60 seconds on each retry

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
                time.sleep(RETRY_DELAY)
                timeout += TIMEOUT_INCREMENT
            else:
                print(f"HTTP error: {e} on attempt {attempt + 1} for query: {query}")
                break
        except Exception as e:
            print(f"An error occurred: {e} on attempt {attempt + 1} for query: {query}")
            break
        if attempt == MAX_RETRIES - 1:
            print(f"Failed after {MAX_RETRIES} attempts. Moving to the next graph/query.")
        time.sleep(RETRY_DELAY)

properties = ['rdf:type', 'rdfs:subClassOf']
object_properties = ['rdf:type', 'rdfs:subClassOf']
all_classes = set()

def fetch_subject_types(graph, property):
    query = f"""
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    SELECT DISTINCT ?stype
    WHERE {{
      GRAPH <{graph}> {{
        ?s {property} ?stype .
      }}
    }}
    """
    results = execute_sparql_query(query)
    if results:
        return [result["stype"]["value"] for result in results["results"]["bindings"]]
    else:
        return []

with open('schema.csv', mode='w', newline='') as file, \
     open('all_classes.csv', mode='w', newline='') as class_file:
    writer = csv.writer(file)
    class_writer = csv.writer(class_file)
    
    writer.writerow(["Class1", "Predicate", "Class2"])  # CSV Header for schema relationships
    class_writer.writerow(["All Classes"])  # CSV Header for all classes

    for graph in graphs:
        for subject_prop in properties:
            subject_types = fetch_subject_types(graph, subject_prop)
            for stype in subject_types:
                if "_vocabulary" in stype:
                    class_writer.writerow([stype])  # Write subject types to the all_classes.csv
                    for object_prop in object_properties:
                        query = f"""
                        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                        SELECT DISTINCT ?stype ?p ?otype
                        WHERE {{
                          GRAPH <{graph}> {{
                            ?s {subject_prop} <{stype}> .
                            ?s ?p ?o .
                            ?o {object_prop} ?otype .
                          }}
                        }}
                        """
                        results = execute_sparql_query(query)
                        if results:
                            for result in results["results"]["bindings"]:
                                otype = result["otype"]["value"]
                                predicate = result["p"]["value"]
                                writer.writerow([stype, predicate, otype])

print("CSV files '16_March_kg_schema_optimized.csv' and 'all_classes.csv' have been created.")
# nohup python bioSchema.py > OutputLog.txt 2>&1 &



                        
                        
                          
                                    
                                        
                                       


                        

                        
                        
                
