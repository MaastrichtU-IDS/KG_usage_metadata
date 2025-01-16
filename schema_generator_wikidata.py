import csv
import time
from SPARQLWrapper import SPARQLWrapper, JSON

def main():
    sparql = SPARQLWrapper("https://maryam.blazegraph.137.120.31.148.nip.io/bigdata/namespace/kb/sparql")
    
    MAX_RETRIES = 3
    INITIAL_TIMEOUT = 60  # Starting with a 60 seconds timeout
    TIMEOUT_INCREMENT = 60  # Timeout increases by 60 seconds after each timeout error
    RETRY_DELAY = 45  # Delay before retrying

    unique_types = set()  # Set to store unique type_ids

    def execute_sparql_query(query):
        timeout = INITIAL_TIMEOUT  # Initial timeout setting
        for attempt in range(MAX_RETRIES):
            try:
                sparql.setQuery(query)
                sparql.setReturnFormat(JSON)
                sparql.setTimeout(timeout)  # Set the timeout for the query
                return sparql.query().convert()
            except Exception as e:
                print(f"An error occurred: {e} on attempt {attempt + 1} for query: {query}")
                timeout += TIMEOUT_INCREMENT  # Increase the timeout for the next attempt
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY)  # Delay before retrying
                else:
                    print(f"Failed after {MAX_RETRIES} attempts. Moving to the next query.")
        return None

    def fetch_types_with_property(property):
        query_template = """
        PREFIX wdt: <http://www.wikidata.org/prop/direct/>
        SELECT DISTINCT ?type WHERE {{
            ?s {property} ?type .
        }} 
        """
        return execute_sparql_query(query_template.format(property=property))

    def fetch_relationships_for_type(type_id, property):
        results = []
        for prop in ["wdt:P31", "wdt:P279"]:
            query_template = f"""
            PREFIX wdt: <http://www.wikidata.org/prop/direct/>
            PREFIX wd: <http://www.wikidata.org/entity/>
            SELECT DISTINCT ?p ?objectType WHERE {{
                ?s {property} wd:{type_id} .
                ?s ?p ?o .
                ?o {prop} ?objectType .
            }} 
            """
            partial_results = execute_sparql_query(query_template)
            if partial_results:
                results.extend(partial_results["results"]["bindings"])
        return {"results": {"bindings": results}}

    csv_filename = 'wikidata_schema.csv'
    with open(csv_filename, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(["Class1", "Predicate", "Class2"])  # CSV Header

        for property in ["wdt:P31", "wdt:P279"]:
            types_results = fetch_types_with_property(property)
            if types_results:
                for result in types_results["results"]["bindings"]:
                    type_id = result["type"]["value"].split('/')[-1]  # Extracting QID
                    unique_types.add(type_id)  # Adding to set of unique types
                    relationships_results = fetch_relationships_for_type(type_id, property)
                    if relationships_results:
                        for relationship in relationships_results["results"]["bindings"]:
                            class1 = type_id
                            predicate = relationship["p"]["value"]
                            class2 = relationship["objectType"]["value"].split('/')[-1]  # Extracting QID
                            writer.writerow([class1, predicate, class2])

    # Writing the unique type_ids to a new CSV file
    unique_types_filename = 'unique_type_ids.csv'
    with open(unique_types_filename, mode='w', newline='', encoding='utf-8') as type_file:
        type_writer = csv.writer(type_file)
        type_writer.writerow(['Type ID'])
        for type_id in unique_types:
            type_writer.writerow([type_id])

    print(f"CSV file '{csv_filename}' has been created with the schema information.")
    print(f"CSV file '{unique_types_filename}' has been created with all unique type IDs.")

if __name__ == "__main__":
    main()
