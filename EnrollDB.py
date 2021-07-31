# Install pre-req for the code:
# pip install mysql-connector-python

# Lab 6 Question 1:
import mysql.connector
import unittest

# EnrollDB is the Application


class EnrollDB:
    """Application for an enrollment database on MySQL"""

    def connect(self):
        """Makes a connection to the database and returns connection to caller"""
        try:
            # TODO: Fill in your connection information
            print("Connecting to database.")
            self.cnx = mysql.connector.connect(
                user='avarma', password='39738166', host='cosc304.ok.ubc.ca', database='db_avarma')
            return self.cnx
        except mysql.connector.Error as err:
            print(err)

    def init(self):
        """Creates and initializes the database"""
        fileName = "university.ddl"
        print("Loading data")
        try:
            cursor = self.cnx.cursor()
            with open(fileName, "r") as infile:
                st = infile.read()
                commands = st.split(";")
                for line in commands:
                    # print(line.strip("\n"))
                    line = line.strip()
                    if line == "":  # Skip blank lines
                        continue
                    cursor.execute(line)
            cursor.close()
            self.cnx.commit()
            print("Database load complete")
        except mysql.connector.Error as err:
            print(err)
            self.cnx.rollback()

    def close(self):
        try:
            print("Closing database connection.")
            self.cnx.close()
        except mysql.connector.Error as err:
            print(err)

    # Part 1: List all students in the database

    def listAllStudents(self):
        # """ Returns a String with all the students in the database.
        #     Format:
        #         sid, sname, sex, birthdate, gpa
        #         00005465, Joe Smith, M, 1997-05-01, 3.20
        #     Return:
        #         String containing all student information"""

        query = "SELECT sid, sname, sex, birthdate, gpa FROM student"

        print("Executing list all students.")

        # TODO: Execute query and build output string
        try:
            cursor = self.cnx.cursor()
            output = "sid, sname, sex, birthdate, gpa \n"
            cursor.execute(query)

            for (sid, sname, sex, birthdate, gpa) in cursor:
                output = output + str(sid) + ", " + sname + ", " + \
                    sex + ", " + str(birthdate) + ", " + str(gpa) + "\n"
            cursor.close()
            return output
        except mysql.connector.Error as err:
            print(err)

    # Part 2: List all professors in a department
    def listDeptProfessors(self, deptName):

        # """Returns a String with all the professors in a given department name.
        #    Format:
        #             Professor Name, Department Name
        #             Art Funk, Computer Science
        #    Returns:
        #             String containing professor information"""
        try:
            output = "Professor Name, Deparment Name \n"
            if (deptName != "None"):
                query = f'SELECT pname, dname FROM prof WHERE dname = "{deptName}"'
            else:
                query = 'SELECT pname, dname FROM prof WHERE dname = "Null"'
            print("Looking for all professors in " + deptName + "... \n")

            cursor = self.cnx.cursor()
            cursor.execute(query)

            for (pname, dname) in cursor:
                output = output + pname + ", " + dname + "\n"
            cursor.close()
            return output
        except mysql.connector.Error as err:
            print(err)

    # Part 3
    def listCourseStudents(self, courseNum):
        # """Returns a String with all students in a given course number (all sections).
        #     Format:
        #         Student Id, Student Name, Course Number, Section Number
        #         00005465, Joe Smith, COSC 304, 001
        #     Return:
        #          String containing students"""

        try:
            output = "Student Id, Student Name, Course Number, Section Number \n"
            query = f'SELECT student.sid, sname, enroll.cnum, enroll.secnum FROM student JOIN enroll ON enroll.sid=student.sid WHERE enroll.cnum = "{courseNum}"'
            cursor = self.cnx.cursor()
            cursor.execute(query)
            for (sid, sname, cnum, secnum) in cursor:
                output = output + f'{sid}, {sname}, {cnum}, {secnum}, \n'
            cursor.close()
            return output
        except mysql.connector.Error as err:
            print(err)

    # Part 4
    def computeGPA(self, studentId):
        # """Returns a cursor with a row containing the computed GPA (named as gpa) for a given student id."""
        query = f'SELECT student.sid, student.gpa FROM student WHERE student.sid = "{studentId}"'
        try:
            cursor = self.cnx.cursor()
            cursor.execute(query)
            print(cursor.rowcount, "record(s) affected")
            return cursor
        except mysql.connector.Error as err:
            print(err)

    # Part 5
    def addStudent(self, studentId, studentName, sex, birthDate):
        query = f'INSERT INTO student (student.sid, sname, sex, birthdate) VALUES ("{studentId}",  "{studentName}", "{sex}", "{birthDate}")'
        try:
            cursor = self.cnx.cursor()
            cursor.execute(query)
            self.cnx.commit()
            print(cursor.rowcount, "record(s) affected")
            return
        except mysql.connector.Error as err:
            print(err)

    # Part 6: Delete Students
    def deleteStudent(self, studentId):
        #         """Deletes a student from the databases."""
        query = f'DELETE FROM student WHERE sid ="{studentId}"'

        cursor = self.cnx.cursor()
        cursor.execute(query)
        self.cnx.commit()
        print(cursor.rowcount, "record(s) affected")
        cursor.close()
        return

    # Part 7: Update Student
    def updateStudent(self, studentId, studentName, sex, birthDate, gpa):
        # Updates a student in the databases.

        if(str(birthDate) == "None"):
            query = f'UPDATE student SET sname = "{studentName}", sex = "{sex}", birthdate = null, gpa = "{gpa}" WHERE sid = "{studentId}"'
        else:
            query = f'UPDATE student SET sname = "{studentName}", sex = "{sex}", birthdate = "{birthDate}", gpa = "{gpa}" WHERE sid = "{studentId}"'

        print(query)
        try:
            cursor = self.cnx.cursor()
            cursor.execute(query)
            self.cnx.commit()
            print(cursor.rowcount, "record(s) affected")
            cursor.close()
            return
        except mysql.connector.Error as err:
            print(err)

    # Part 8: New Enrolment
    def newEnroll(self, studentId, courseNum, sectionNum, grade):
        query = f'INSERT INTO enroll (sid, cnum, secnum, grade) VALUES ("{studentId}", "{courseNum}", "{sectionNum}", "{grade}")'
        print(query)
        try:
            cursor = self.cnx.cursor()
            cursor.execute(query)
            self.cnx.commit()
            print(cursor.rowcount, "record(s) affected")
            cursor.close()
            return
        except mysql.connector.Error as err:
            print(err)

    def updateStudentGPA(self, studentId):
        # """ Updates a student's GPA based on courses taken."""
        query = f'SELECT sid, AVG(grade) FROM enroll WHERE sid = "{studentId}"'

        print(f'Updating the gpa for student number: {studentId}')
        try:
            cursor = self.cnx.cursor()
            cursor.execute(query)
            output = ""

            for (gpa) in cursor:
                print("new GPA is: " + str(gpa[1]))
                query = f'UPDATE student SET gpa = "{str(gpa[1])}" WHERE sid = "{studentId}"'
            try:
                cursor.execute(query)
                self.cnx.commit()
                print(cursor.rowcount, "record(s) affected")
                cursor.close()
            except mysql.connector.Error as err:
                print(err)
        except mysql.connector.Error as err:
            print(err)
        return


    def updateStudentMark(self, studentId, courseNum, sectionNum, grade):
        # Updates a student's mark in an enrolled course section and updates their grade.
        # TODO: Execute statement. Make sure to commit
        query = f'UPDATE enroll SET grade = "{grade}" WHERE sid = "{studentId}" AND cnum = "{courseNum}" AND secnum = "{sectionNum}"'
        try:
            cursor = self.cnx.cursor()
            cursor.execute(query)
            self.cnx.commit()
            print(cursor.rowcount, "record(s) affected")
            cursor.close()
            self.updateStudentGPA(studentId)
        except mysql.connector.Error as err:
            print(err)
        return


    def removeStudentFromSection(self, studentId, courseNum, sectionNum):
        # Removes a student from a course and updates their GPA.
        query = f'DELETE FROM enroll WHERE sid = "{studentId}" AND cnum = "{courseNum}" AND secnum = "{sectionNum}"'
        print(
            f'Attempting to remove {courseNum}, {sectionNum} for student number: {studentId}')

        try:
            cursor = self.cnx.cursor()
            cursor.execute(query)
            self.cnx.commit()
            print(cursor.rowcount, "record(s) affected")
            cursor.close()
            self.updateStudentGPA(studentId)
        except mysql.connector.Error as err:
            print(err)
        return

    def query1(self):
        #Return the list of students (id and name) that have not registered in any course section. Hint: Left join can be used instead of a subquery.
        
        query = 'SELECT student.sid, sname FROM student LEFT JOIN enroll ON student.sid=enroll.sid WHERE NOT student.sid IN (SELECT sid FROM enroll GROUP BY sid)'
        
        try:
            cursor = self.cnx.cursor()
            cursor.execute(query)
            return cursor
        except mysql.connector.Error as err:
            print(err)


    def query2(self):
#         """For each student return their id and name, number of course sections registered in (called numcourses), and gpa (average of grades).
#         Return only students born after March 15, 1992. A student is also only in the result if their gpa is above 3.1 or registered in 0 courses.
#         Order by GPA descending then student name ascending and show only the top 5."""
        query = 'SELECT student.sid, sname, COUNT(enroll.cnum) as numcourses, AVG(enroll.grade) AS gpa FROM student LEFT JOIN enroll ON student.sid=enroll.sid WHERE (student.birthdate > "1992-03-15") GROUP BY student.sid HAVING ((gpa > 3.1) OR COUNT(enroll.cnum) =0) ORDER BY gpa DESC, sid ASC LIMIT 5;'
        try:
            cursor = self.cnx.cursor()
            cursor.execute(query)
            return cursor
        except mysql.connector.Error as err:
            print(err)

    def query3(self):
#         """For each course, return the number of sections (numsections), total number of students enrolled (numstudents), average grade (avggrade), and number of distinct professors who taught the course (numprofs).
#             Only show courses in Chemistry or Computer Science department. Make sure to show courses even if they have no students. Do not show a course if there are no professors teaching that course.
#             Format:
#             cnum, numsections, numstudents, avggrade, numprof"""

        query = 'SELECT enroll.cnum, COUNT(DISTINCT enroll.secnum) AS numsections, COUNT(enroll.sid) AS numstudents, AVG(enroll.grade) AS avggrade FROM enroll GROUP BY enroll.cnum;'
        
        try:
            cursor = self.cnx.cursor()
            cursor.execute(query)
            return cursor
        except mysql.connector.Error as err:
            print(err)

    def query4(self):
#         """Return the students who received a higher grade than their course section average in at least two courses. Order by number of courses higher than the average and only show top 5.
#             Format:
#             EmployeeId, EmployeeName, orderCount"""

        query = 'SELECT student.sid, sname FROM student LEFT JOIN enroll ON student.sid=enroll.sid WHERE NOT student.sid IN (SELECT sid FROM enroll GROUP BY sid)'
        
        # "Total columns: 3"
        # 				+"\nsid, sname, numhigher"
        # 				+"\n00324534, Tony Tenson, 10"
        # 				+"\n00112233, Trisha Cavanugh, 9"
        # 				+"\n00612354, Elizabeth Guillum, 8"
        # 				+"\n55980348, Brian Brooks, 7"
        # 				+"\n99234353, Jamie Stokes, 7"
        # 				+"\nTotal results: 5"

        try:
            cursor = self.cnx.cursor()
            cursor.execute(query)
            return cursor
        except mysql.connector.Error as err:
            print(err)

     # Do NOT change anything below here
    def resultSetToString(self, cursor, maxrows):
        output = ""
        cols = cursor.column_names
        output += "Total columns: "+str(len(cols))+"\n"
        output += str(cols[0])
        for i in range(1, len(cols)):
            output += ", "+str(cols[i])
        for row in cursor:
            output += "\n"+str(row[0])
            for i in range(1, len(cols)):
                output += ", "+str(row[i])
        output += "\nTotal results: "+str(cursor.rowcount)
        return output
#################################################################


# Main execution for testing
enrollDB = EnrollDB()
enrollDB.connect()
# Prevent rebuilding on each try of the code.
# enrollDB.init()

# # #Question 1 Part 1:
# # print("\n ---------------------- Question 1 Part 1: ----------------------")
# # results = (enrollDB.listAllStudents())
# # print(results)

# # #Question 1 Part 2:
# # print("\n ---------------------- Question 1 Part 2: ----------------------")
# # print("Executing list professors in a department: Computer Science")
# # print(enrollDB.listDeptProfessors("Computer Science"))
# # print("Executing list professors in a department: none")
# # print(enrollDB.listDeptProfessors("none"))

# # # Question 1 Part 3:
# # print("\n ---------------------- Question 1 Part 3: ----------------------")
# # print("Executing list students in course: COSC 304")
# # output = enrollDB.listCourseStudents("COSC 304")
# # print(output)

# # print("Executing list students in course: DATA 301")
# # output = enrollDB.listCourseStudents("DATA 301")
# # print(output)

# # # Question 1 Part 4:
# # print("\n ---------------------- Question 1 Part 4: ----------------------")
# # print("Executing compute GPA for student: 45671234")
# # print(enrollDB.resultSetToString(enrollDB.computeGPA("45671234"), 10))

# # print("Executing compute GPA for student: 00000000")
# # enrollDB.resultSetToString(enrollDB.computeGPA("45671234"), 10)

# # # Question 1 Part 5
# # print("\n ---------------------- Question 1 Part 5: ----------------------")
# # print("Adding student 55555555:")
# # enrollDB.addStudent("55555555",  "Stacy Smith", "F", "1998-01-01")
# # print("Adding student 11223344:")
# # enrollDB.addStudent("11223344",  "Jim Jones", "M",  "1997-12-31")

# # # Question 1 Part 6
# # print("\n ---------------------- Question 1 Part 6: ----------------------")
# # print("Test delete student:")
# # print("Deleting student 99999999:")
# # enrollDB.deleteStudent("99999999")
# # # Non-existing student
# # print("Deleting student 00000000:")
# # enrollDB.deleteStudent("00000000")


# # # Question 1 Part 7
# # print("\n ---------------------- Question 1 Part 7: ----------------------")
# # print("Updating student 99999999:")
# # enrollDB.updateStudent("99999999",  "Wang Wong", "F", "1995-11-08", 3.23)
# # print("Updating student 00567454:")
# # enrollDB.updateStudent("00567454",  "Scott Brown", "M",  None, 4.00)
# # print(enrollDB.listAllStudents())

# # Question 1 part 8
# print("\n ---------------------- Question 1 Part 8: ----------------------")
# print("Test new enrollment in COSC 304 for 98123434:")
# # Add an enroll with a student
# enrollDB.newEnroll("98123434", "COSC 304", "001", 2.51)

# Question 1 part 9
# print("\n ---------------------- Question 1 Part 9: ----------------------")
# enrollDB.init()
# print("Test update student GPA for student:")
#enrollDB.newEnroll("98123434", "COSC 304", "001", 3.97)
#enrollDB.updateStudentGPA("98123434")

# Question 1 part 10
# print("\n ---------------------- Question 1 Part 10: ----------------------")
# print("Test update student mark for student 98123434 to 3.55:")
# enrollDB.updateStudentMark("98123434", "COSC 304", "001", 3.55)

# Question 1 part 11
# print("\n ---------------------- Question 1 Part 11: ----------------------")
# enrollDB.init()
# enrollDB.removeStudentFromSection("00546343", "CHEM 113", "002")

# Queries
# Re-initialize all data

#enrollDB.init()
print("\n ---------------------- Question 1 Query 1: ----------------------")
print(enrollDB.resultSetToString(enrollDB.query1(), 100))

print("\n ---------------------- Question 1 Query 2: ----------------------")
print(enrollDB.resultSetToString(enrollDB.query2(), 100))

print("\n ---------------------- Question 1 Query 3: ----------------------")
print(enrollDB.resultSetToString(enrollDB.query3(), 100))


print(enrollDB.resultSetToString(enrollDB.query4(), 100))

enrollDB.close()
