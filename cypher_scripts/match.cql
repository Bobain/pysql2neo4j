MATCH (u:Users)-[r1:USERS_CONTRIBUTIONS]-(c1:Contributions)-[r2:PROJECTS_CONTRIBUTIONS]-(p:Projects)
CREATE UNIQUE (u)-[r:BACKED]->(p);

MATCH
        (u1:Users)-[r1:BACKED]->(p1:Projects)<-[r2:BACKED]-(u2:Users)
WITH u1, u2, count(p1) as nb_common_projects
WHERE ID(u1) < ID(u2)
CREATE UNIQUE (u1)-[r:BACKED_PROJECTS_MATCH {nb_projects: nb_common_projects}]->(u2);
