MATCH (u:Users)-[r1:USERS_CONTRIBUTIONS]-(c1:Contributions)-[r2:PROJECTS_CONTRIBUTIONS]-(p:Projects)
CREATE UNIQUE (u)-[r:BACKED]->(p);

CREATE INDEX ON :Users(nb_projects_contributed);
