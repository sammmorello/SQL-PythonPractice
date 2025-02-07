import psycopg2
from psycopg2 import sql

conn = psycopg2.connect(
    database="pythonPractice",
    user="postgres",
    password="pumpkinpostgre",
    host="localhost",
    port="5432"
)
conn = None
cur = None

try:

    conn = psycopg2.connect(
        database="pythonPractice",
        user="postgres",
        password="pumpkinpostgre",
        host="localhost",
        port="5432"
)

    cur = conn.cursor()

#create database table names employee

# Drop the employee table if it exists
   # cur.execute('DROP TABLE IF EXISTS employee CASCADE')

    create_employee_table = ''' CREATE TABLE IF NOT EXISTS employee (
                            employee_id int PRIMARY KEY,
                            name varchar(40) NOT NULL,
                            salary int,
                            dept_id varchar(30)
                            )'''
 
    cur.execute(create_employee_table) #should create table

    create_jobtitle_table = ''' CREATE TABLE IF NOT EXISTS job_title (
                            job_id SERIAL PRIMARY KEY,
                            job_name VARCHAR(50) NOT NULL UNIQUE,
                            min_salary INT,
                            max_salary INT
                            )'''
    
    cur.execute(create_jobtitle_table) #should create table

    create_employeereview_table = ''' CREATE TABLE IF NOT EXISTS employeereview (
                            review_id SERIAL PRIMARY KEY,
                            employee_id INT REFERENCES employee(id),
                            review_date DATE NOT NULL,
                            reviewer_name VARCHAR(50),
                            performance_score INT,
                            comments TEXT
                            )'''
    
    cur.execute(create_employeereview_table) #should create table

    create_employee_overview_table = '''CREATE TABLE combined_data AS
    SELECT 
        e.employee_id,
        e.name AS employee_name,
        e.salary,
        j.job_name,
        r.performance_score,
        r.comments
    FROM 
        employee e
    LEFT JOIN 
        employeereview r ON e.employee_id = r.employee_id
    LEFT JOIN 
        job_title j ON CAST(e.dept_id AS INTEGER) = j.job_id
    '''
    
    cur.execute(create_employee_overview_table)
    conn.commit()
    
    
    conn.commit() # commit changes

    #always make sure database connection + cursor is closed when we exit program
    cur.close() #close cursor
    conn.close() #close connection

except Exception as error:
        print(error)

finally:
    if cur is not None:
        cur.close() #close cursor
    if conn is not None:
        conn.close() #close connection 