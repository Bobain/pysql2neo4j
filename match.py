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
            WHERE ID(u1) < ID(u2) AND ID(u1) >= %d and ID(u1) < %d
            WITH u1, u2, count(p1) as nb_common_projects
            CREATE UNIQUE (u1)-[r:BACKED_PROJECTS_MATCH {nb_projects: nb_common_projects}]-(u2)
        """
    n = 10
    for i in xrange(0, graph.graphDb.evaluate('MATCH (u:Users) RETURN COUNT(*)'), n):
        s = req % (i, i+n)
        print("Running cypher statement : \n\t%s" % s)
        start = time.time()
        graph.cypher_exec(s)
        print("\n\t it took: %g " % (time.time() - start))
