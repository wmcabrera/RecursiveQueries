insert into colrq_timetrack values('wiki_vote4_groupopt','n','n','y','y',4,'n',TIMESTAMP 'now')

DROP table IF EXISTS E CASCADE

CREATE TABLE E AS SELECT i AS i,j AS j, 1 AS p, v AS v FROM wiki_vote

DROP table IF EXISTS R1 CASCADE

CREATE TABLE R1 AS SELECT 1 AS d, i AS i, j AS j, sum(p) AS p, max(v) AS v FROM E GROUP BY d,i,j

DROP table IF EXISTS R2 CASCADE

CREATE TABLE R2 AS SELECT  2 AS d, R1.i AS i, E.j AS j, sum(R1.p*E.p) AS p, max(R1.v*E.v) AS v FROM R1 JOIN E ON R1.j=E.i WHERE R1.i!=R1.j GROUP BY R1.d,R1.i,E.j

SELECT COUNT(*) FROM R2

DROP table IF EXISTS R3 CASCADE

CREATE TABLE R3 AS SELECT  3 AS d, R2.i AS i, E.j AS j, sum(R2.p*E.p) AS p, max(R2.v*E.v) AS v FROM R2 JOIN E ON R2.j=E.i WHERE R2.i!=R2.j GROUP BY R2.d,R2.i,E.j

SELECT COUNT(*) FROM R3

DROP table IF EXISTS R4 CASCADE

CREATE TABLE R4 AS SELECT  4 AS d, R3.i AS i, E.j AS j, sum(R3.p*E.p) AS p, max(R3.v*E.v) AS v FROM R3 JOIN E ON R3.j=E.i WHERE R3.i!=R3.j GROUP BY R3.d,R3.i,E.j

SELECT COUNT(*) FROM R4

DROP table IF EXISTS reqa CASCADE

CREATE TABLE reqa AS SELECT   * FROM R1 UNION ALL SELECT * FROM R2 UNION ALL SELECT * FROM R3 UNION ALL SELECT * FROM R4

SELECT COUNT(*) FROM reqa

DROP table IF EXISTS E CASCADE

DROP table IF EXISTS R1 CASCADE

DROP table IF EXISTS R2 CASCADE

DROP table IF EXISTS R3 CASCADE

DROP table IF EXISTS R4 CASCADE

DROP table IF EXISTS def CASCADE

select d,i,j,sum(p) as p,max(v) as v into def from REQA group by d,i,j

update colrq_timetrack set endtime=timestamp 'now' where endtime is null

