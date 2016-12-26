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
            WHERE u1.user_id = %d and ID(u1) < ID(u2)
            WITH u1, u2, count(p1) as nb_common_projects
            CREATE UNIQUE (u1)-[r:BACKED_PROJECTS_MATCH {nb_projects: nb_common_projects}]-(u2)
        """
    n = graph.graphDb.evaluate('MATCH (u:Users) RETURN max(u.user_id)')
    print('Running a query to create relationships for all %d users' % n)
    all_start = time.time()
    n_chunk_print = 10000
    for i in xrange(1, n+1):
        s = req % i
        if i % n_chunk_print == 1:
            print("Running cypher statement : \n\t%s" % s)
            start = time.time()
        graph.cypher_exec(s)
        if i % n_chunk_print == 0:
            print("\n\t it took: %g " % (time.time() - start))
    print('Creating all match relationships took: %g' % (time.time() - all_start))

    print('Answer to original question: How many match for three common projects?')
    graph.graphDb.evaluate('MATCH (u1)-[r:BACKED_PROJECTS_MATCH]-(u2) WHERE ID(u1) < ID(u2) and r.nb_projects > 2 RETURN count(*)')
    print('Answer to original question: How many different people are involved in these match up?')
    graph.graphDb.evaluate('MATCH (u1)-[r:BACKED_PROJECTS_MATCH]-(u2) WHERE ID(u1) < ID(u2) and r.nb_projects > 2 RETURN 1 + count(DISTINCT u1)')

     
    print('Here is the number of match for a given number of common projects:')
    nb_match = 1
    nb_projects = 2
    while nb_match > 0:
        nb_match = graph.graphDb.evaluate('MATCH (u1)-[r:BACKED_PROJECTS_MATCH]-(u2) WHERE ID(u1) < ID(u2) and r.nb_projects = %d RETURN count(*)' % nb_projects)
        print('%d,%d' % (nb_projects, nb_match))
        nb_projects += 1

    nb_prefect_match = graph.graphDb.evaluate('MATCH (u1)-[r:BACKED_PROJECTS_MATCH]-(u2) WHERE ID(u1) < ID(u2) and r.nb_projects > 2 and u1.nb_projects_contributed = u2.nb_projects_contributed RETURN count(*)')
    print('Number of perfect match up with at least three common projects : %d' % nb_prefect_match)

    max_nb_common_projects = graph.graphDb.evaluate('MATCH (u1)-[r:BACKED_PROJECTS_MATCH]-(u2) WHERE ID(u1) < ID(u2) and r.nb_projects > 2 and u1.nb_projects_contributed = r.nb_projects and u1.nb_projects_contributed = u2.nb_projects_contributed RETURN count(*)')
    print('Real max number of commen projects backed is : %d' % max_nb_common_projects)
    # graph.graphDb.evaluate('MATCH (u1)-[r:BACKED_PROJECTS_MATCH]-(u2) WHERE ID(u1) < ID(u2) and r.nb_projects = %d RETURN [u1, u2]' % max_nb_common_projects)


    max_nb_common_projects4perfectmatch = graph.graphDb.evaluate('MATCH (u1)-[r:BACKED_PROJECTS_MATCH]-(u2) WHERE ID(u1) < ID(u2) and r.nb_projects > 2 and u1.nb_projects_contributed = u2.nb_projects_contributed RETURN max(r.nb_projects)')
    print('Max number of common projects backed for perfect match is : %d' % max_nb_common_projects4perfectmatch)
    print('Here is the number of match for a given number of common projects for people who did backed exactly the same projects:')    
    nb_projects = 2
    while nb_projects < max_nb_common_projects4perfectmatch:
        nb_match = graph.graphDb.evaluate('MATCH (u1)-[r:BACKED_PROJECTS_MATCH]-(u2) WHERE ID(u1) < ID(u2) and r.nb_projects = %d and u1.nb_projects_contributed = %d and u2.nb_projects_contributed = %d RETURN count(*)' % tuple([nb_projects]*3))
        print('%d,%d' % (nb_projects, nb_match))
        nb_projects += 1




    staff_users_id = [835650, 1257099,  650923, 1297847, 1270691, 1266970, 1284297,     722, 1247141,  614086,   78178, 1120114, 1217633, 1074735, 1200264, 1181275,     588,  898449, 1000059, 1066252,  944915,  988807,  542821,  194341,   51108,  943675,   45987,  589577,  437899,   75549,  371637,  571109,  614692,   76841,   52547,  293231,    5499,    3048,  604063,  570981,       7,       2,  360191,   13887,   73663]
    for sid in staff_users_id:
        nb_projects_backed = graph.graphDb.evaluate('MATCH (u1) WHERE u1.user_id = %d RETURN u1.nb_projects_contributed' % sid)
        if not nb_projects_backed is None and nb_projects_backed > 1:
            name = graph.graphDb.evaluate('MATCH (u1) WHERE u1.user_id = %d RETURN coalesce(u1.screenname, u1.username, u1.name_)' % sid)
            if len(name.replace(' ', '')) == 0:
                print('No name: %d' % sid)
            nb_match = graph.graphDb.evaluate('MATCH (u1)-[r:BACKED_PROJECTS_MATCH]-(u2) WHERE u1.user_id = %d and r.nb_projects > 2  RETURN count(*)' % sid)
            nb_light_match = graph.graphDb.evaluate('MATCH (u1)-[r:BACKED_PROJECTS_MATCH]-(u2) WHERE u1.user_id = %d and r.nb_projects > 1  RETURN count(*)' % sid)
            nb_perfect_match = graph.graphDb.evaluate('MATCH (u1)-[r:BACKED_PROJECTS_MATCH]-(u2) WHERE u1.user_id = %d and r.nb_projects = %d and u1.nb_projects_contributed = %d and u2.nb_projects_contributed = %d RETURN count(*)' % (sid, nb_projects_backed, nb_projects_backed, nb_projects_backed))
            match_exemple = graph.graphDb.evaluate('MATCH (u1)-[r:BACKED_PROJECTS_MATCH]-(u2) WHERE u1.user_id = %d and r.nb_projects > 1 RETURN coalesce(u2.screenname, u2.username, u2.name_) order by r.nb_projects / sqrt(u1.nb_projects_contributed * u2.nb_projects_contributed) DESC LIMIT 1' % sid)
            print('%s,%d,%d,%d,%s,%d' % (name, nb_match, nb_light_match, nb_perfect_match, match_exemple, nb_projects_backed))

