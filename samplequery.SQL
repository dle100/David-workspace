SELECT student.sid, sname, enroll.cnum, enroll.secnum FROM student JOIN enroll ON enroll.sid=student.sid
SELECT student.sid, sname, enroll.cnum, enroll.secnum FROM student JOIN enroll ON enroll.sid=student.sid WHERE enroll.cnum = "COSC 304"
SELECT student.sid, sname, enroll.cnum, enroll.secnum FROM student JOIN enroll ON enroll.sid=student.sid WHERE enroll.cnum = "DATA 301"

SELECT student.sid, sname, enroll.cnum, enroll.secnum AVG(enroll.grade) FROM student JOIN enroll ON enroll.sid=student.sid GROUP BY student.sid

-- Part 4
SELECT student.sid, AVG(student.gpa) as avgGPA FROM student WHERE student.sid = "45671234"
SELECT student.sid, enroll.grade FROM student JOIN enroll ON enroll.sid=student.sid WHERE student.sid = "45671234"

INSERT INTO student (sid, sname, sex, birthdate,) VALUES ()


UPDATE student SET sname = "Scott Brown", sex = "M", birthdate = None, gpa = "4.0" WHERE sid = "00567454";

enrollDB.updateStudent("99999999",  "Wang Wong", "F", "1995-11-08", 3.23);

UPDATE student SET sname = "Scott Brown", sex = "M", birthdate = null, gpa = "4.0" WHERE sid = "00567454";
UPDATE student SET sname = "Wang Wong", sex = "F", birthdate = "1995-11-08", gpa = "3.23" WHERE sid = "99999999"

enrollDB.newEnroll("98123434", "COSC 304", "001", 2.51)

INSERT INTO enroll (sid, cnum, secnum, grade) VALUES ("98123434", "COSC 304", "001", 2.51)

SELECT AVG(grade) FROM enroll WHERE sid = 98123434;
SELECT AVG(grade) FROM enroll;
SELECT sid, AVG(grade) as gpa FROM enroll;
SELECT sid FROM enroll GROUP BY sid;

-- Query 2:
SELECT student.sid, sname, COUNT(enroll.cnum) as numcourses, AVG(enroll.grade) AS gpa FROM student LEFT JOIN enroll ON student.sid=enroll.sid WHERE (student.birthdate > "1992-03-15") GROUP BY student.sid HAVING ((gpa > 3.1) OR COUNT(enroll.cnum) =0) ORDER BY gpa DESC, sid ASC LIMIT 5;


-- Finding Sheldon
SELECT student.sid, sname, COUNT(enroll.cnum) as numcourses, AVG(enroll.grade) AS gpa FROM student LEFT JOIN enroll ON student.sid=enroll.sid WHERE (student.birthdate > "1992-03-15") GROUP BY student.sid HAVING ((gpa > 3.1) OR COUNT(enroll.cnum) =0) ORDER BY gpa DESC, sid ASC LIMIT 5;

SELECT * from student;

--Query 3
-- numsections, numstudents, avggrade, numprofs
-- Numsections:
SELECT enroll.cnum, count(DISTINCT enroll.secnum) as numsections FROM section JOIN enroll on enroll.cnum=section.cnum  GROUP BY enroll.cnum;

-- Numstudents and avggrade:
SELECT enroll.cnum, COUNT(DISTINCT enroll.secnum) AS numsections, COUNT(enroll.sid) AS numstudents, AVG(enroll.grade) AS avggrade FROM enroll GROUP BY enroll.cnum;

-- Numprofs
-- First, each prof for each section:
SELECT DISTINCT enroll.secnum, enroll.cnum, section.pname FROM enroll JOIN section on section.secnum= enroll.secnum;
-- SELECT enroll.cnum, DISTINCT enroll.secnum, COUNT(enroll.sid) AS numstudents, AVG(enroll.grade) AS avggrade FROM enroll GROUP BY enroll.cnum;
SELECT DISTINCT enroll.secnum, enroll.cnum, section.pname FROM enroll JOIN section on section.secnum= enroll.secnum GROUP BY enroll.secnum



SELECT enroll.cnum, COUNT(DISTINCT enroll.secnum) AS numsections, COUNT(enroll.sid) AS numstudents, AVG(enroll.grade) AS avggrade, section.pname FROM enroll JOIN course ON course.cnum=enroll.cnum JOIN section on section.secnum=section.secnum GROUP BY enroll.cnum;

SELECT enroll.cnum, COUNT(DISTINCT enroll.secnum) AS numsections, COUNT(enroll.sid) AS numstudents, AVG(enroll.grade) AS avggrade, count(section.pname) FROM enroll JOIN section GROUP BY enroll.cnum;
SELECT enroll.cnum, COUNT(DISTINCT enroll.secnum) AS numsections, COUNT(enroll.sid) AS numstudents, AVG(enroll.grade) AS avggrade, count(DISTINCT section.pname) FROM enroll JOIN section on section.secnum = enroll.secnum GROUP BY enroll.cnum

SELECT enroll.cnum, COUNT(DISTINCT enroll.secnum) AS numsections, COUNT(enroll.sid) AS numstudents, AVG(enroll.grade) AS avggrade, count(DISTINCT section.cnum) FROM enroll JOIN section on section.secnum = enroll.secnum GROUP BY enroll.cnum ;





-- Query 4 --
-- Return the students who received a higher grade than their course section average in at least two courses. 
-- Order by number of courses higher than the average and only show top 5.

SELECT student.sid, sname, count(DISTINCT sid) FROM student JOIN enroll on student.sid=enroll.sid GROUP by student.sid;
SELECT enroll.sid, sname, avg(enroll.grade) FROM student JOIN enroll on student.sid=enroll.sid GROUP by enroll.sid;
--Table: Base Grades
SELECT enroll.sid, enroll.cnum, sname, enroll.grade FROM student JOIN enroll on student.sid=enroll.sid;
(SELECT enroll.cnum, avg(enroll.grade) as avggrade FROM enroll GROUP BY cnum);
--sort by section
(SELECT cnum, secnum, avg(enroll.grade) FROM enroll GROUP BY secnum);


(SELECT enroll.cnum, enroll.secnum, avg(enroll.grade) as avggrade FROM enroll GROUP BY secnum);
-- Merged:
SELECT enroll.sid, enroll.cnum, sname, enroll.grade, e1.avggrade FROM student JOIN enroll on student.sid=enroll.sid JOIN (SELECT enroll.cnum, avg(enroll.grade) AS avggrade FROM enroll GROUP BY cnum) as e1 ON enroll.cnum=e1.cnum;


SELECT sid, sname FROM student JOIN enroll on student.sid=enroll.sid WHERE enroll.grade > 
SELECT sid, sname FROM student JOIN enroll on student.sid=enroll.sid WHERE count(enroll.grade > avg(enroll.grade))



SELECT e1.sid, student.sname, e1.grade, e2.avggrade FROM enroll as e1 JOIN student ON student.sid=e1.sid JOIN (SELECT AVG(grade) as avggrade, enroll.cnum FROM enroll GROUP BY cnum) as e2 ON e1.cnum=e2.cnum WHERE e1.grade > e2.avggrade

(SELECT AVG(grade), enroll.cnum FROM enroll GROUP BY cnum)

e2 WHERE e1.grade > AVG(e2.grade)