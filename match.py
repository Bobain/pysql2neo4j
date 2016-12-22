from pysql2neo4j.graph import GraphProc
import time


def run_cyper_script(graph, file_path):
    with open(file_path, 'r') as cypher_file:
        statements = cypher_file.read()

    for s in statements.split(";"):
        print("Running cypher statement : \n\t%s" % s)
        start = time.time()
        graph.cypher_exec(s)
        print("\n\t it took: %g " % (time.time() - start))

if __name__ == '__main__':
    graph = GraphProc()
    run_cyper_script(graph, './match_scripts/cypher_scripts/match.cql')
