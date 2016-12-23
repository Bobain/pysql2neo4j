MATCH (u:Users) WHERE NOT exists(u.nb_projects_contributed) OR u.nb_projects_contributed < 2 DETACH DELETE u;

MATCH (u:Users)-[r1:USERS_CONTRIBUTIONS]-(c1:Contributions)-[r2:PROJECTS_CONTRIBUTIONS]-(p:Projects)
CREATE UNIQUE (u)-[r:BACKED]->(p);

MATCH (c:Contributions) DETACH DELETE c;