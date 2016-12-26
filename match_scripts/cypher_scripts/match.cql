MATCH (u:Users) WHERE NOT exists(u.nb_projects_contributed) OR u.nb_projects_contributed < 2 DETACH DELETE u;

MATCH (u:Users)-[r1:USERS_CONTRIBUTIONS]-(c1:Contributions)-[r2:PROJECTS_CONTRIBUTIONS]-(p:Projects)
WITH u, p, sum(c1.contribution) as sum_cont
CREATE (u)-[r:BACKED {contribution: sum_cont}]->(p);
