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