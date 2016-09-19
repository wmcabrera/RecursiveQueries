insert into colrq_timetrack values('treeclique10k4_selectopt','n','n','y','n',4,'n',TIMESTAMP 'now')

DROP table IF EXISTS E CASCADE

CREATE TABLE E AS SELECT i AS i,j AS j, 1 AS p, v AS v FROM treeclique1m

DROP table IF EXISTS R1 CASCADE

CREATE TABLE R1 AS SELECT  1 AS d, i AS i, j AS j, p AS p, v AS v FROM E WHERE i=1

DROP table IF EXISTS R2 CASCADE

CREATE TABLE R2 AS SELECT 2 AS d, R1.i AS i, E.j AS j, R1.p*E.p AS p,R1.v+E.v AS v  FROM R1 JOIN E ON R1.j=E.i WHERE R1.i!=R1.j AND R1.i=1

SELECT COUNT(*) FROM R2

DROP table IF EXISTS R3 CASCADE

CREATE TABLE R3 AS SELECT 3 AS d, R2.i AS i, E.j AS j, R2.p*E.p AS p,R2.v+E.v AS v  FROM R2 JOIN E ON R2.j=E.i WHERE R2.i!=R2.j AND R2.i=1

SELECT COUNT(*) FROM R3

DROP table IF EXISTS R4 CASCADE

CREATE TABLE R4 AS SELECT 4 AS d, R3.i AS i, E.j AS j, R3.p*E.p AS p,R3.v+E.v AS v  FROM R3 JOIN E ON R3.j=E.i WHERE R3.i!=R3.j AND R3.i=1

SELECT COUNT(*) FROM R4

DROP table IF EXISTS R5 CASCADE

CREATE TABLE R5 AS SELECT 5 AS d, R4.i AS i, E.j AS j, R4.p*E.p AS p,R4.v+E.v AS v  FROM R4 JOIN E ON R4.j=E.i WHERE R4.i!=R4.j AND R4.i=1

SELECT COUNT(*) FROM R5

DROP table IF EXISTS R6 CASCADE

CREATE TABLE R6 AS SELECT 6 AS d, R5.i AS i, E.j AS j, R5.p*E.p AS p,R5.v+E.v AS v  FROM R5 JOIN E ON R5.j=E.i WHERE R5.i!=R5.j AND R5.i=1

SELECT COUNT(*) FROM R6

DROP table IF EXISTS reqa CASCADE

CREATE TABLE reqa AS SELECT   * FROM R1 UNION ALL SELECT * FROM R2 UNION ALL SELECT * FROM R3 UNION ALL SELECT * FROM R4 UNION ALL SELECT * FROM R5 UNION ALL SELECT * FROM R6

SELECT COUNT(*) FROM reqa

DROP table IF EXISTS E CASCADE

DROP table IF EXISTS R1 CASCADE

DROP table IF EXISTS R2 CASCADE

DROP table IF EXISTS R3 CASCADE

DROP table IF EXISTS R4 CASCADE

DROP table IF EXISTS R5 CASCADE

DROP table IF EXISTS R6 CASCADE

DROP table IF EXISTS def CASCADE

select * into def from REQA where i=1

update colrq_timetrack set endtime=timestamp 'now' where endtime is null

