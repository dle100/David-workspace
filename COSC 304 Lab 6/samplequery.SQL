SELECT student.sid, sname, enroll.cnum, enroll.secnum FROM student JOIN enroll ON enroll.sid=student.sid;
SELECT student.sid, sname, enroll.cnum, enroll.secnum FROM student JOIN enroll ON enroll.sid=student.sid WHERE enroll.cnum = "COSC 304";
SELECT student.sid, sname, enroll.cnum, enroll.secnum FROM student JOIN enroll ON enroll.sid=student.sid WHERE enroll.cnum = "DATA 301";

SELECT student.sid, sname, enroll.cnum, enroll.secnum AVG(enroll.grade) FROM student JOIN enroll ON enroll.sid=student.sid GROUP BY student.sid;

-- Part 4: computeGPA
SELECT student.gpa FROM student WHERE student.sid = "45671234";
SELECT AVG(grade) FROM enroll WHERE sid = "45671234";

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
"""For each student return their id and name, number of course sections registered in (called numcourses), and gpa (average of grades). 
        Return only students born after March 15, 1992. A student is also only in the result if their gpa is above 3.1 or registered in 0 courses.
        Order by GPA descending then student name ascending and show only the top 5."""
SELECT student.sid, sname, COUNT(enroll.cnum) as numcourses, AVG(enroll.grade) AS gpa FROM student LEFT JOIN enroll ON student.sid=enroll.sid WHERE (student.birthdate > "1992-03-15") GROUP BY student.sid HAVING ((gpa > 3.1) OR COUNT(enroll.cnum) =0) ORDER BY gpa DESC, sid ASC LIMIT 5;


-- Finding Sheldon
SELECT student.sid, sname, COUNT(enroll.cnum) as numcourses, AVG(enroll.grade) AS gpa FROM student LEFT JOIN enroll ON student.sid=enroll.sid WHERE (student.birthdate > "1992-03-15") GROUP BY student.sid HAVING ((gpa > 3.1) OR COUNT(enroll.cnum) = 0 OR COUNT(enroll.cnum) IS NULL) ORDER BY gpa DESC, sid ASC LIMIT 5;;
SELECT student.sid, sname, COUNT(enroll.cnum) as numcourses, AVG(enroll.grade) AS gpa FROM student LEFT JOIN enroll ON student.sid=enroll.sid WHERE (student.birthdate > "1992-03-15") GROUP BY student.sid HAVING ((gpa > 3.1) OR COUNT(enroll.cnum) = 0 OR COUNT(enroll.cnum) IS NULL) ORDER BY gpa DESC, sname ASC LIMIT 5;



--Query 3


SELECT enroll.cnum, count(DISTINCT enroll.secnum) as numsections FROM section JOIN enroll on enroll.cnum=section.cnum  GROUP BY enroll.cnum;
-- We want cnum, numsections, numstudents, avggrade, numprofs


-- cnum, Numsections:
SELECT enroll.cnum, count(DISTINCT enroll.secnum) as numsections FROM section JOIN enroll on enroll.cnum=section.cnum  GROUP BY enroll.cnum;

-- cnum, Numsections, Numstudents and Avggrade:
SELECT enroll.cnum, COUNT(DISTINCT enroll.secnum) AS numsections, COUNT(enroll.sid) AS numstudents, AVG(enroll.grade) AS avggrade FROM enroll GROUP BY cnum;

-- Minimized:
SELECT cnum, CAST(COUNT(enroll.sid) AS int), AVG(enroll.grade) AS avggrade FROM enroll GROUP BY cnum;


-- Numprofs:
SELECT DISTINCT section.cnum, count(section.secnum), COUNT(DISTINCT section.pname) as profs FROM section GROUP BY section.cnum HAVING profs > 0 ORDER BY cnum asc;



-- All Categories
SELECT DISTINCT section.cnum, count(section.secnum) as numsections, numstudents, avggrade, COUNT(DISTINCT section.pname) as profs FROM section LEFT JOIN (SELECT enroll.cnum, COUNT(enroll.sid) AS numstudents, AVG(enroll.grade) AS avggrade FROM enroll GROUP BY cnum) as enr ON enr.cnum = section.cnum GROUP BY section.cnum HAVING profs > 0 ORDER BY cnum asc;


-- Applying Filters, only COSC and CHEM.
-- =============== FINAL QUERY =================
SELECT DISTINCT section.cnum, count(section.secnum) as numsections, numstudents, avggrade, COUNT(DISTINCT section.pname) as profs FROM section LEFT JOIN (SELECT enroll.cnum, COUNT(enroll.sid) AS numstudents, AVG(enroll.grade) AS avggrade FROM enroll GROUP BY cnum) as marks ON marks.cnum = section.cnum WHERE section.cnum LIKE '%CHEM%' OR section.cnum LIKE '%COSC%' GROUP BY section.cnum HAVING profs > 0 ORDER BY cnum asc;

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

-- Finally a comparator appears
SELECT enroll.sid, enroll.cnum, sname, (enroll.grade > e1.avggrade) FROM student JOIN enroll on student.sid=enroll.sid JOIN (SELECT enroll.cnum, avg(enroll.grade) AS avggrade FROM enroll GROUP BY cnum) as e1 ON enroll.cnum=e1.cnum;
SELECT sid, sname FROM student JOIN enroll on student.sid=enroll.sid WHERE enroll.grade > 
SELECT sid, sname FROM student JOIN enroll on student.sid=enroll.sid WHERE count(enroll.grade > avg(enroll.grade))



SELECT e1.sid, student.sname, e1.grade, e2.avggrade FROM enroll as e1 JOIN student ON student.sid=e1.sid JOIN (SELECT AVG(grade) as avggrade, enroll.cnum FROM enroll GROUP BY cnum) as e2 ON e1.cnum=e2.cnum WHERE e1.grade > e2.avggrade

(SELECT AVG(grade), enroll.cnum FROM enroll GROUP BY cnum)

e2 WHERE e1.grade > AVG(e2.grade);
-- FINAL Query 4
SELECT enroll.sid, sname, SUM (enroll.grade > e1.avggrade) as numhigher FROM student JOIN enroll on student.sid=enroll.sid JOIN (SELECT enroll.cnum, avg(enroll.grade) AS avggrade FROM enroll GROUP BY enroll.cnum) as e1 ON enroll.cnum=e1.cnum GROUP BY enroll.sid HAVING numhigher > 1 ORDER BY numhigher DESC LIMIT 5;


------ Refactoring
SELECT enroll.sid, sname, SUM (enroll.grade > e1.avggrade) as numhigher 
FROM student 
JOIN enroll on student.sid=enroll.sid 

JOIN 

(SELECT enroll.secnum,  avg(enroll.grade) AS avggrade 
FROM enroll GROUP BY enroll.secnum) 
as e1 
ON enroll.secnum=e1.secnum GROUP BY enroll.sid HAVING numhigher > 1 ORDER BY numhigher DESC LIMIT 5;




-- Trying a Join.
SELECT enroll.sid, sname, SUM(enroll.grade > e1.avggrade) as numhigher 
FROM student, enroll

JOIN 

(SELECT enroll.cnum, enroll.secnum, avg(enroll.grade) AS avggrade 
FROM enroll GROUP BY enroll.secnum, enroll.cnum) 
as e1 ON enroll.cnum=e1.cnum AND enroll.secnum=e1.secnum

WHERE student.sid=enroll.sid 
GROUP BY enroll.sid HAVING numhigher > 1 
ORDER BY numhigher DESC LIMIT 5;

--trying a join part 2
SELECT enroll.sid, sname, SUM(enroll.grade > e1.avggrade) as numhigher 
FROM student, enroll, 
(SELECT enroll.cnum, enroll.secnum, avg(enroll.grade) AS avggrade 
FROM enroll GROUP BY enroll.secnum, enroll.cnum) 
as e1

WHERE student.sid=enroll.sid AND enroll.cnum=e1.cnum AND enroll.secnum=e1.secnum
GROUP BY enroll.sid HAVING numhigher > 1 
ORDER BY numhigher DESC LIMIT 5;

-- Final Answer:
SELECT enroll.sid, sname, SUM(enroll.grade > e1.avggrade) as numhigher FROM student, enroll, (SELECT enroll.cnum, enroll.secnum, avg(enroll.grade) AS avggrade FROM enroll GROUP BY enroll.secnum, enroll.cnum) as e1 WHERE student.sid=enroll.sid AND enroll.cnum=e1.cnum AND enroll.secnum=e1.secnum GROUP BY enroll.sid HAVING numhigher > 1 ORDER BY numhigher DESC LIMIT 5;