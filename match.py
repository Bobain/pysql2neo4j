from pysql2neo4j.graph import GraphProc
import time


def run_cyper_script(graph, file_path):
    with open(file_path, 'r') as cypher_file:
        statements = cypher_file.read()

    for s in statements.split(";"):
        if len(s) > 4:
            print("Running cypher statement : \n\t%s" % s)
            start = time.time()
            graph.cypher_exec(s)
            print("\n\t it took: %g " % (time.time() - start))

if __name__ == '__main__':
    graph = GraphProc()
    run_cyper_script(graph, './match_scripts/cypher_scripts/match.cql')

    req = """
            MATCH 
                    (u1:Users)-[r1:BACKED]->(p1:Projects)<-[r2:BACKED]-(u2:Users)
            WHERE ID(u1) = %d and %d < ID(u2)
            WITH u1, u2, count(p1) as nb_common_projects
            CREATE UNIQUE (u1)-[r:BACKED_PROJECTS_MATCH {nb_projects: nb_common_projects}]-(u2)
        """
    n = graph.graphDb.evaluate('MATCH (u:Users) RETURN COUNT(*)')
    print('Running a query to create relationships for all %d users' % n)
    n_chunk_print = 10000
    for i in xrange(1, n+1):
        s = req % (i, i)
        if i % n_chunk_print == 1:
            print("Running cypher statement : \n\t%s" % s)
            start = time.time()
        graph.cypher_exec(s)
        if i % n_chunk_print == 0:
            print("\n\t it took: %g " % (time.time() - start))
