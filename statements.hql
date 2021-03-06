DROP TABLE IF EXISTS dept;

CREATE TABLE IF NOT EXISTS dept (
  deptno int,
  dname string,
  loc string
);

INSERT INTO TABLE dept VALUES (10,'ACCOUNTING','NEW YORK');
INSERT INTO TABLE dept VALUES (20,'RESEARCH','DALLAS');
INSERT INTO TABLE dept VALUES (30,'SALES','CHICAGO');
INSERT INTO TABLE dept VALUES (40,'OPERATIONS','BOSTON');

DROP TABLE IF EXISTS emp;

CREATE TABLE IF NOT EXISTS emp (
  empno int,
  ename string,
  job string,
  mgr string,
  hiredate string,
  sal int,
  comm string,
  deptno int
);


INSERT INTO TABLE emp VALUES (7839,'KING','PRESIDENT','','1981-11-17',5000,'',10);
INSERT INTO TABLE emp VALUES (7698,'BLAKE','MANAGER','7839','1981-01-05',2850,'',30);
INSERT INTO TABLE emp VALUES (7782,'CLARK','MANAGER','7839','1981-09-06',2450,'',10);
INSERT INTO TABLE emp VALUES (7566,'JONES','MANAGER','7839','1981-04-02',2975,'',20);
INSERT INTO TABLE emp VALUES (7788,'SCOTT','ANALYST','7566','1987-07-13',3000,'',20);
INSERT INTO TABLE emp VALUES (7902,'FORD','ANALYST','7566','1981-12-03',3000,'',20);
INSERT INTO TABLE emp VALUES (7369,'SMITH','CLERK','7902','1980-12-17',800,'',20);
INSERT INTO TABLE emp VALUES (7499,'ALLEN','SALESMAN','7698','1981-02-20',1600,'300',30);
INSERT INTO TABLE emp VALUES (7521,'WARD','SALESMAN','7698','1981-02-22',1250,'500',30);
INSERT INTO TABLE emp VALUES (7654,'MARTIN','SALESMAN','7698','1981-09-28',1250,'1400',30);
INSERT INTO TABLE emp VALUES (7844,'TURNER','SALESMAN','7698','1981-09-08',1500,'0',30);
INSERT INTO TABLE emp VALUES (7876,'ADAMS','CLERK','7788','1987-07-13',1100,'',20);
INSERT INTO TABLE emp VALUES (7900,'JAMES','CLERK','7698','1981-03-12',950,'',30);
INSERT INTO TABLE emp VALUES (7934,'MILLER','CLERK','7782','1982-01-23',1300,'',10);

