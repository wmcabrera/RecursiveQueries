insert into colrq_timetrack values('complete1m4_distopt','n','n','n','n',4,'n',TIMESTAMP 'now')

DROP table IF EXISTS E CASCADE

CREATE TABLE E AS SELECT i AS i,j AS j, 1 AS p, v AS v FROM complete1m

DROP table IF EXISTS R1 CASCADE

CREATE TABLE R1 AS SELECT  1 AS d, i AS i, j AS j, p AS p, v AS v FROM E

DROP table IF EXISTS R2 CASCADE

CREATE TABLE R2 AS SELECT 2 AS d, R1.i AS i, E.j AS j, R1.p*E.p AS p,R1.v+E.v AS v  FROM R1 JOIN E ON R1.j=E.i WHERE R1.i!=R1.j 

SELECT COUNT(*) FROM R2

DROP table IF EXISTS R3 CASCADE

CREATE TABLE R3 AS SELECT 3 AS d, R2.i AS i, E.j AS j, R2.p*E.p AS p,R2.v+E.v AS v  FROM R2 JOIN E ON R2.j=E.i WHERE R2.i!=R2.j 

