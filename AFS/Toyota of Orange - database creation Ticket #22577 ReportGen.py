#!/usr/bin/env python
# coding: utf-8

# In[1]:


import openpyxl
from openpyxl.styles import Font
import os
import pymysql
from datetime import datetime
from shutil import copyfile
 


# In[2]:


# Database connection parameters

DB_HOST = "n4k-auto02"
DB_PORT = 3306
DB_USER = "toyota_orange"
DB_PASSWORD = "Superm@n3"
DB_SCHEMA = "toyota_orange"

path = "J:/AFS Clients/Toyota of Orange 401(k) Plan and Trust - 1227/Database/report/"

batch_id = datetime.now().strftime("%Y-%m-%d_%H.%M.%S")


# In[22]:


template_path = "J:\AFS Clients\Toyota of Orange 401(k) Plan and Trust - 1227\Database\AutomationOnly/template.xlsx"
if not os.path.exists(template_path ):
            raise FileNotFoundError(f"Template file not found at: {template_path }")

target_file_path = os.path.join(path,f"Toyota_Orange_Report_{batch_id}.xlsx")
copyfile(template_path, target_file_path)

target_workbook = openpyxl.load_workbook(target_file_path)


# In[4]:


conn = pymysql.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_SCHEMA
     )


# In[23]:


def appendToSheet(targetSheetName, tableName, query):

    target_sheet = target_workbook[targetSheetName]

    columns = """
        SHOW FIELDS FROM %s
    """ % (tableName)

    cursor = conn.cursor()

    cursor.execute(columns)
    dataField = cursor.fetchall()


    headingList=[]
    
    for row in dataField:
       headingList.append(row[0])

    target_sheet.append(headingList)
    
    cursor.execute(query)
    data = cursor.fetchall()
 
            # Append data from database to target sheet
    for row in data:
        target_sheet.append(row)
 
            # Save changes to the workbook
    target_workbook.save(target_file_path)

    print(f"[{datetime.now()}]Data appended successfully")
    


# In[24]:


def appendToReportSheet(targetSheetName, query):

    target_sheet = target_workbook[targetSheetName]

    cursor = conn.cursor()

    headingList=['SSN', 'Loc_CD', 'Last Name', 'First Name', 'Hire Date', 'Term Date']

    target_sheet.append(headingList)
    
    cursor.execute(query)
    data = cursor.fetchall()
 
            # Append data from database to target sheet
    for row in data:
        target_sheet.append(row)
 
            # Save changes to the workbook
    target_workbook.save(target_file_path)

    print(f"[{datetime.now()}] Data appended successfully")


# In[25]:


query1 = """
       SELECT id as ID,loc_cd,ssn,last_name,first_name,birth_date,hire_date,term_date,sec_125,create_time,batch_nm FROM census_plus125 where ssn not in (select ssn from census_payment_detail) order by create_time desc
    """
appendToSheet('DQ_125_No_PayDet', 'census_plus125', query1)

query2 = """
        SELECT * FROM census_payment_detail where ssn not in (select ssn from census_plus125) order by create_time desc
    """
appendToSheet('DQ_PayDet_No_125', 'census_payment_detail', query2)

query3 = """
        SELECT * FROM census_payment_detail where ssn not in (select ssn from census_address) order by create_time desc
    """
appendToSheet('DQ_PayDet_No_Address', 'census_payment_detail', query3)

query4 = """
        SELECT id,loc_cd,ssn,address1,address2,city,state_cd,zip,create_time,batch_nm FROM census_address where ssn not in (select ssn from census_plus125) order by create_time desc
    """
appendToSheet('DQ_Address_No_125', 'census_address', query4)

query5 = """
        SELECT id,loc_cd,ssn,address1,address2,city,state_cd,zip,create_time,batch_nm FROM census_address where ssn not in (select ssn from census_payment_detail) order by create_time desc
    """
appendToSheet('DQ_Address_No_PayDet', 'census_address', query5)

query6 = """
        SELECT id,loc_cd,ssn,last_name,first_name,birth_date,hire_date,term_date,sec_125,create_time,batch_nm FROM census_plus125 where ssn not in (select ssn from census_address) order by create_time desc
    """
appendToSheet('DQ_125_No_Address', 'census_plus125', query6)

query7 = """
        SELECT * FROM census_address  where loc_cd not in (SELECT location_cd FROM location) ORDER BY create_time desc,`loc_cd` asc 
    """

appendToSheet('Unknown_Addr_Loc_CD', 'census_address', query7)

query8 = """
        SELECT * FROM census_plus125 where loc_cd not in (SELECT location_cd FROM location) ORDER BY create_time desc,`loc_cd` asc
    """

appendToSheet('Plus125_unknown_Location_Code', 'census_plus125', query8)

query9 = """
        SELECT * FROM census_payment_detail where loc_cd not in (SELECT location_cd FROM location) ORDER BY create_time desc,`loc_cd` asc
    """

appendToSheet('PaymentDetail_unknown_Loc_Code', 'census_payment_detail', query9)

operationalQuery = """
SELECT distinct SSN, Loc_CD, Last_Name, First_Name, Hire_Date, Term_Date
FROM emp_at_mult_locations_vw
where SSN in (select ssn from census_125_mult_locations_vw )
order by SSN, hire_date, loc_cd
"""
appendToReportSheet('Emp_at_Mult_Locations', operationalQuery)


# In[ ]:




