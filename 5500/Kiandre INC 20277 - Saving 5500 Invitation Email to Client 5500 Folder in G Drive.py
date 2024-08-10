#!/usr/bin/env python
# coding: utf-8

import sys
import contextlib
from pathlib import Path
from scheduleLogger import schedule_logger
from errorEmail import email_error_to_automation
from datetime import datetime as datetime2

try:
    schedule_logger('Running...', pyfile= __file__)
    
    
    path_fstdout__ = Path(r'Y:\Automation\Scheduled\stdout') / f'{Path(__file__).stem}.txt'
    path_fstderr__ = Path(r'Y:\Automation\Scheduled\stderr') / f'{Path(__file__).stem}.txt'
    
    with open(path_fstdout__,'a') as fstdout__, \
         open(path_fstderr__,'a') as fstderr__:
                       
        with contextlib.redirect_stdout(fstdout__), \
             contextlib.redirect_stderr(fstderr__):
            
            print('\n\n')
            print('+' * 80)
            print(f'{datetime2.now():%F %T}', end='\n\n')
            
            print('\n\n')
            print('+' * 80, file=sys.stderr)
            print(f'{datetime2.now():%F %T}', file=sys.stderr, end='\n\n')
            

            
            # In[1]:
            
            
            """
            I initially tried to save emails using win32com and it worked fine on my laptop. However, it doesn't work on the automation
            server. Everytime win32com tries to access a com object, outlook will prompt you to allow access for each email or
            grant it access for up to 10 minutes. The only way this can be disabled is if you can go into Outlook's Trust Center
            and grant programmatic access. This is grayed out and IT wont do it for you. Furthermore, Trust Center
            cant detect antivirus software on a server operating system (even though it exists) so this programmatic block is strictly
            enforced by Outlook. I had to move on and use the O365 library. You'll see remnants of the win32com code and I kept it in 
            place in case I want to use it in the future. For now, I can only save emails as an .eml file rather than a .msg file 
            because thats the limitation of O365. 
            
            - Andrew 07/24/23
            """
            
            
            
            import sys
            sys.path.insert(0, "Y:\Automation\Team Scripts\Andrew Kim\my modules")
            
            import os
            import re
            #import win32com
            #from win32com.client import Dispatch
            from glob import glob
            from dateutil.relativedelta import relativedelta
            #from pprint import pprint
            from pathlib import Path
            import datetime
            
            import pensionpro as pp
            
            import pandas as pd
            import numpy as np
            from fuzzywuzzy import fuzz, process
            
            from O365 import Account, FileSystemTokenBackend
            from O365.message import Message
            
            from IPython.display import display
            
            pd.set_option('display.max_rows',None)
            pd.set_option('display.max_columns',None)
            pd.set_option('display.max_colwidth',None)
            
            
            # In[3]:
            
            
            # Custom Functions
            
            def get_client_folder(planid,testing_year):
                
                g_drive = os.listdir('G:/')
                
                for folder in g_drive:
                    
                    if folder.split()[0] == str(planid):
                        target_folder =f'G:/{folder}/{testing_year}/5500'
                        if not os.path.exists(target_folder):
                            print(f"{target_folder} doesn't exist. Creating the folder.")
                            os.makedirs(target_folder)                
                        return target_folder
                    
                return False
            
            
            def dataframe_logger(dataframe, destination_path, trim_at = 100):
                """
                Requires os and pandas. Saves a dataframe as an excel file for error logging. If its found at the destination,
                it will concact onto it. It will trim the newly concatenated dataframe if there are more than 100 rows.
                
                """
                
                if os.path.exists(destination_path):
                    existing_excel_dataframe = pd.read_excel(destination_path)
                    
                    new_dataframe = pd.concat([dataframe,existing_excel_dataframe], ignore_index=True) # Old df tacked onto new df.
                    
                    # Trim old records so excel file doesn't become huge.
                    counter = 0
                    while len(new_dataframe) > int(trim_at): 
                        new_dataframe.drop(new_dataframe.index[-1], inplace=True)
                        counter += 1
                    if counter:
                        print(f"{counter} record(s) deleted.")
                        
                    new_dataframe.to_excel(destination_path, index = False)
                    print(f"Log file found. Concatenated to {destination_path}")
                    
                else:
                    dataframe.to_excel(destination_path, index = False)
                    print(f"New log created at {destination_path}")        
                    
                    
            def return_save_path_with_unique_name(EMAIL_SUBJECT,DESTINATION_FOLDER):
                
                CLEANED_EMAIL_SUBJECT = re.sub(r'[<>:"/\\|?*]', '', EMAIL_SUBJECT)  
                SAVE_PATH = os.path.join(DESTINATION_FOLDER, f"{CLEANED_EMAIL_SUBJECT}.eml")
            
                counter = 1
                while os.path.exists(SAVE_PATH):
                    # Do not overwrite emails if one is found at its destination. Just append a number to the new copy.
                    # Append a suffix (e.g., _1, _2, etc.) to the file name
                    NEW_FILE_NAME = f"{os.path.splitext(f'{CLEANED_EMAIL_SUBJECT}.eml')[0]}_{counter}{os.path.splitext(f'{CLEANED_EMAIL_SUBJECT}.eml')[1]}"
                    SAVE_PATH = os.path.join(DESTINATION_FOLDER, NEW_FILE_NAME)
                    counter += 1
                
                return SAVE_PATH
                #Y:\5500\2022\Emails
                
            def CLEAN_PLAN_NAME(input_str):
                """
                Once you grab the email subject and find the plan name, clean the plan name 
                to the same standards as that of the ALL_PLANS df.
                """
                #input_str = re.sub(r'401\(k\).*|401k.*', '', input_str, flags=re.IGNORECASE)
                input_str = re.sub(r'401\s?\(k\)[^\n]*?plan|401\s?k[^\n]*?plan', '', input_str, flags=re.IGNORECASE)
                input_str = re.sub(r'401\s?\(k\)|401\s?k', '', input_str, flags=re.IGNORECASE)
                input_str = re.sub(r'401\s?\(a\)[^\n]*?plan|401\s?a[^\n]*?plan', '', input_str, flags=re.IGNORECASE)
                input_str = re.sub(r'401\s?\(a\)|401\s?a', '', input_str, flags=re.IGNORECASE)
                input_str = input_str.strip()
                return input_str    
            
            
            # In[ ]:
            
            
            
            
            
            # In[4]:
            
            
            # General setup
            
            # Regex pattern to search for the date in the email.
            REGEX_PATTERN = r"(?i)(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s+\d{4}"
            
            
            
            credentials = "fe12d3a6-9b60-4764-ab55-868fd4533247", "qtw8Q~HFdlP1RR4yES4e8paQOglCiieXHR8gvbOZ"
            token_backend = FileSystemTokenBackend(token_filename='automation_oauth_token.txt')
            account = Account(credentials,
                              token_backend=token_backend,
                              scopes = ['basic','message_all','mailbox'],
                             )
            
            if not account.is_authenticated:
                account.authenticate()
            print(account.is_authenticated)
            
            
            # In[5]:
            
            
            # Log at end of script.
            TROUBLESHOOTING_DF = pd.DataFrame(columns=["Found Plan Name", "Planid", "Found Directory", "Run Time"], dtype='object')
            
            # Grabbing all plans in pension pro and using fuzzywuzzy turned out to be a better way
            # to search for plans instead of searching each plan in Pension Pro. 
            # Not all plan names found in the email will have proper spelling and may even have
            # random punctuation such as 'INC' and 'INC.'
            ALL_PLANS = pd.DataFrame(pp.get_plans(get_all=True, select='Name,InternalPlanId'))
            ALL_PLANS["Name"] = ALL_PLANS["Name"].str.lower()
            ALL_PLANS = ALL_PLANS.drop(ALL_PLANS.loc[ALL_PLANS['Name'] == "employees retirement plan"].index)
            ALL_PLANS['Name'] = ALL_PLANS['Name'].str.replace(r'401\s?\(k\)[^\n]*?plan|401\s?k[^\n]*?plan', '', regex=True)
            ALL_PLANS['Name'] = ALL_PLANS['Name'].str.replace(r'401\s?\(k\)|401\s?k', '', regex=True)   
            ALL_PLANS['Name'] = ALL_PLANS['Name'].str.replace(r'401\s?\(a\)[^\n]*?plan|401\s?a[^\n]*?plan', '', regex=True)
            ALL_PLANS['Name'] = ALL_PLANS['Name'].str.replace(r'401\s?\(a\)|401\s?a', '', regex=True)  
            ALL_PLANS.reset_index(inplace=True)
            
            
            # In[6]:
            
            
            # win32com: Find the Inbox folder within Automation Team.
            
            # outlook = win32com.client.Dispatch("Outlook.Application")
            # namespace = outlook.GetNamespace("MAPI")
            # num_data_files = namespace.Folders.Count
            
            # # Iterate over the data files and print their properties
            # for i in range(1, num_data_files + 1):
            #     data_file = namespace.Folders.Item(i)
            #     print(f"Data File Index: {i}")
            #     print(f"Data File Name: {data_file.Name}")
            #     print("")
            #     if data_file.Name == "Automation Team":
            #         index = i
            # data_file = namespace.Folders.Item(index)
            
            
            
            # inbox_folder = None
            # for folder in data_file.Folders:
            #     if folder.Name == "Inbox":
            #         inbox_folder = folder
            #         break
            
            
            # In[7]:
            
            
            ALL_PLANS.head(30)
            
            
            # In[14]:
            
            
            emails = account.mailbox().inbox_folder().get_messages(limit=999) #o365 account. Not win32com
            for email in emails:
                try:
                    print(EMAIL_SUBJECT := email.subject)
                except UnicodeEncodeError:
                    continue
                EMAIL_BODY = email.get_body_text()
                
                
                
                if EMAIL_SUBJECT[0:31] == "ACTION REQUIRED: Sign Form 5500":
                
                
                    # Search for the date using regex
                    DATE_FROM_REGEX = re.search(REGEX_PATTERN, EMAIL_BODY)
                    if DATE_FROM_REGEX:
                        FOUND_DATE_STRING = DATE_FROM_REGEX.group()
                        print(FOUND_DATE_STRING)
                        DATETIME_OBJECT = datetime.datetime.strptime(FOUND_DATE_STRING, "%B %d, %Y")
                        DATETIME_OBJECT = DATETIME_OBJECT - relativedelta(days=364) 
                        PLAN_YEAR = DATETIME_OBJECT.year
                    else:
                        print("Date could not found. Cannot determine the plan year. Saving to Y:\\5500\\2022\\Emails")
                        SAVE_PATH = return_save_path_with_unique_name(EMAIL_SUBJECT,r'Y:\5500\2022\Emails')
                        email.save_as_eml(to_path=SAVE_PATH)
                        email.delete()  
                        continue
                        
                        
                        
                    # Get the plan name from the email Subject.
                    #print(PLAN_NAME_FROM_SUBJECT_REGEX := re.findall(r'for\s+(.*?PLAN)', EMAIL_SUBJECT, re.IGNORECASE))
                    print(PLAN_NAME_FROM_SUBJECT_REGEX := re.findall(r'for\s+(.*?)(?=\(0)', EMAIL_SUBJECT, re.IGNORECASE))
                    if PLAN_NAME_FROM_SUBJECT_REGEX:
                        PLAN_NAME_FROM_SUBJECT_REGEX = PLAN_NAME_FROM_SUBJECT_REGEX[0].strip()
                        PLAN_NAME_FROM_SUBJECT_REGEX = CLEAN_PLAN_NAME(PLAN_NAME_FROM_SUBJECT_REGEX).lower()
                        print(f"Found Plan Name: {PLAN_NAME_FROM_SUBJECT_REGEX}")
                    else:
                        print("Regex couldn't find a plan name. Saving to Y:\\5500\\2022\\Emails")
                        SAVE_PATH = return_save_path_with_unique_name(EMAIL_SUBJECT,r'Y:\5500\2022\Emails')
                        email.save_as_eml(to_path=SAVE_PATH)
                        email.delete()    
                        continue        
            
                        
                        
                    
            
                    
                    #CLOSE_MATCHES = process.extract(PLAN_NAME_FROM_SUBJECT_REGEX.lower(), ALL_PLANS["Name"].astype(str).str.lower().to_list(), limit=3, scorer=fuzz.token_set_ratio)
                    CLOSE_MATCHES = process.extract(PLAN_NAME_FROM_SUBJECT_REGEX, ALL_PLANS["Name"].astype(str).str.lower().to_list(), limit=3, scorer=fuzz.WRatio)
                    CLOSE_MATCHES = [match[0] for match in CLOSE_MATCHES if match[1] >= 80]
                    if CLOSE_MATCHES:
                        print("Matches Found:")
                        #print(ALL_PLANS.loc[ALL_PLANS["Name"]==CLOSE_MATCHES[0].lower(),["Name","InternalPlanId"]].iloc[0]) 
                        print(CLOSE_MATCHES)
                        PLANID = ALL_PLANS.loc[ALL_PLANS["Name"]==CLOSE_MATCHES[0].lower(),"InternalPlanId"].iloc[0] 
                        print(DESTINATION_FOLDER := get_client_folder(PLANID,PLAN_YEAR))
                        
                        if not DESTINATION_FOLDER:
                            print("Cannot find a valid plan directory. Saving to Y:\\5500\\2022\\Emails\n")
                            SAVE_PATH = return_save_path_with_unique_name(EMAIL_SUBJECT,r'Y:\5500\2022\Emails')
                            email.save_as_eml(to_path=SAVE_PATH)
                            email.delete()    
                            continue    
                            
                    else:
                        print("Couldn't find the plan using the ALL_PLANS df. Saving to Y:\\5500\\2022\\Emails \n")
                        SAVE_PATH = return_save_path_with_unique_name(EMAIL_SUBJECT,r'Y:\5500\2022\Emails')
                        email.save_as_eml(to_path=SAVE_PATH)
                        email.delete()    
                        continue            
                    
                    
                    
                    # Save email
                    # Get rid of illegal characters or it wont save the file in windows.
            #         CLEANED_EMAIL_SUBJECT = re.sub(r'[<>:"/\\|?*]', '', EMAIL_SUBJECT)  
            #         SAVE_PATH = os.path.join(DESTINATION_FOLDER, f"{CLEANED_EMAIL_SUBJECT}.eml")
            
            #         counter = 1
            #         while os.path.exists(SAVE_PATH):
            #             # Do not overwrite emails if one is found at its destination. Just append a number to the new copy.
            #             # Append a suffix (e.g., _1, _2, etc.) to the file name
            #             NEW_FILE_NAME = f"{os.path.splitext(f'{CLEANED_EMAIL_SUBJECT}.eml')[0]}_{counter}{os.path.splitext(f'{CLEANED_EMAIL_SUBJECT}.eml')[1]}"
            #             SAVE_PATH = os.path.join(DESTINATION_FOLDER, NEW_FILE_NAME)
            #             counter += 1
                        
                    SAVE_PATH = return_save_path_with_unique_name(EMAIL_SUBJECT,DESTINATION_FOLDER)
                    email.save_as_eml(to_path=SAVE_PATH)
                    print(f"Email saved to {SAVE_PATH}")
                    email.delete()           
                        
                        
                        
                    new_row = {
                    "Found Plan Name": PLAN_NAME_FROM_SUBJECT_REGEX,
                    "Planid": PLANID,
                    "Found Directory": DESTINATION_FOLDER,
                    "Run Time": datetime.datetime.now().strftime('%m/%d/%y %H:%M:%S')
                    }
            
                    # Create a new DataFrame from the new_row dictionary
                    new_df = pd.DataFrame([new_row])
            
                    # Concatenate the existing_df and new_df
                    TROUBLESHOOTING_DF = pd.concat([TROUBLESHOOTING_DF, new_df], ignore_index=True)
                    print('\n\n')  
            
            
            # Save emails with win32com. I moved onto using OAuth and o365 library to do this. 
            # IT refuses to lower security standards so that win32com can access com objects so this is not viable on anything
            # other than your own laptop.
            
            
            
            #EMAILS_TO_DELETE = [] # Store unique email entry ids here for deletion for win32com.
            # for item in inbox_folder.Items:
            
                
                
                
            
            #     if item.Subject[0:31] == "ACTION REQUIRED: Sign Form 5500":  # Check if the item is an email and contain target string.
                    
            #         print(EMAIL_SUBJECT := item.Subject)
            #         EMAIL_BODY = item.Body
            
            
            #         # Search for the date using regex
            #         DATE_FROM_REGEX = re.search(REGEX_PATTERN, EMAIL_BODY)
            #         if DATE_FROM_REGEX:
            #             FOUND_DATE_STRING = DATE_FROM_REGEX.group()
            #             print(FOUND_DATE_STRING)
            #             DATETIME_OBJECT = datetime.datetime.strptime(FOUND_DATE_STRING, "%B %d, %Y")
            #             DATETIME_OBJECT = DATETIME_OBJECT - relativedelta(days=364) 
            #             PLAN_YEAR = DATETIME_OBJECT.year
            #         else:
            #             print("Date could not found. Cannot determine the plan year. Skipping.")
            #             continue
            
                        
                        
            #         # Get the plan name from the email Subject.
            #         #print(PLAN_NAME_FROM_SUBJECT_REGEX := re.findall(r'for\s+(.*?PLAN)', EMAIL_SUBJECT, re.IGNORECASE))
            #         print(PLAN_NAME_FROM_SUBJECT_REGEX := re.findall(r'for\s+(.*?)(?=\(0)', EMAIL_SUBJECT, re.IGNORECASE))
            #         if PLAN_NAME_FROM_SUBJECT_REGEX:
            #             PLAN_NAME_FROM_SUBJECT_REGEX = PLAN_NAME_FROM_SUBJECT_REGEX[0].strip()
            #         else:
            #             print("Regex couldn't find a plan name. Skipping.")
            #             continue 
                        
            
            #         # Search Pension Pro using plan name.     
            # #        DESTINATION_FOLDER = ""
            # #         PLAN_INFO = pp.get_plans(filters=f"contains(Name, '{PLAN_NAME_FROM_SUBJECT_REGEX}')")
            # #         if PLAN_INFO["Values"]:
            # #             PLAN_INFO = PLAN_INFO["Values"][0]
            # #             PLANID = PLAN_INFO['InternalPlanId']
            # #             print(DESTINATION_FOLDER := get_client_folder(PLANID,PLAN_YEAR))
            # #             if not DESTINATION_FOLDER:
            # #                 print("Cannot find a valid plan directory.\n")
            # #             pprint(PLAN_INFO)
            # #         else:
            # #             print("Couldn't find the plan in Pension Pro using a literal search.\n")
            
            
            
                   
            
            #         # I download all plans from pension pro as DF and use difflib's fuzzy matching.
            #         # I disabled the literal string search in pension pro. Disabled difflib and used fuzzywuzzy lib.
            #         #if not DESTINATION_FOLDER:
            #         CLOSE_MATCHES = process.extract(PLAN_NAME_FROM_SUBJECT_REGEX.lower(), ALL_PLANS["Name"].astype(str).str.lower().to_list(), limit=3, scorer=fuzz.token_set_ratio)
            #         if CLOSE_MATCHES:
            #             CLOSE_MATCHES = [match[0] for match in CLOSE_MATCHES if match[1] >= 80]
            #             print("Match Found:")
            #             print(ALL_PLANS.loc[ALL_PLANS["Name"]==CLOSE_MATCHES[0].lower(),["Name","InternalPlanId"]].iloc[0]) 
            #             PLANID = ALL_PLANS.loc[ALL_PLANS["Name"]==CLOSE_MATCHES[0].lower(),"InternalPlanId"].iloc[0] 
            #             print(DESTINATION_FOLDER := get_client_folder(PLANID,PLAN_YEAR))
            #             if not DESTINATION_FOLDER:
            #                 print("Cannot find a valid plan directory. Skipping.\n")
            #                 continue    
            #         else:
            #             print("Couldn't find the plan using the ALL_PLANS df. Skipping.\n")
            #             continue    
                            
                            
                            
            #         # Save email
            #         # Get rid of illegal characters or it wont save the .msg file.
            #         CLEANED_EMAIL_SUBJECT = re.sub(r'[<>:"/\\|?*]', '', EMAIL_SUBJECT)  
            #         SAVE_PATH = os.path.join(DESTINATION_FOLDER, f"{CLEANED_EMAIL_SUBJECT}.msg")
            
            #         counter = 1
            #         while os.path.exists(SAVE_PATH):
            #             # Do not overwrite emails if one is found at its destination. Just append a number to the new copy.
            #             # Append a suffix (e.g., _1, _2, etc.) to the file name
            #             NEW_FILE_NAME = f"{os.path.splitext(f'{CLEANED_EMAIL_SUBJECT}.msg')[0]}_{counter}{os.path.splitext(f'{CLEANED_EMAIL_SUBJECT}.msg')[1]}"
            #             SAVE_PATH = os.path.join(DESTINATION_FOLDER, NEW_FILE_NAME)
            #             counter += 1
            
            
            #         item.SaveAs(SAVE_PATH)  # Save the email as .msg file (3 = olMSG)
            #         print(f"Email saved to {SAVE_PATH}")
            #         EMAILS_TO_DELETE.append(item.EntryID) # I cant delete items from the same list im iterating through. I have to save the reference elsewhere.
            
            #         new_row = {
            #         "Found Plan Name": PLAN_NAME_FROM_SUBJECT_REGEX,
            #         "Planid": PLANID,
            #         "Found Directory": DESTINATION_FOLDER,
            #         "Run Time": datetime.datetime.now().strftime('%m/%d/%y %H:%M:%S')
            #         }
            
            #         # Create a new DataFrame from the new_row dictionary
            #         new_df = pd.DataFrame([new_row])
            
            #         # Concatenate the existing_df and new_df
            #         TROUBLESHOOTING_DF = pd.concat([TROUBLESHOOTING_DF, new_df], ignore_index=True)
            #         print('\n')    
                    
            
            
            # In[7]:
            
            
            # Delete emails with win32com.
            # counter = 0        
            # for i in EMAILS_TO_DELETE:
            #     try:
            #         email_to_delete = namespace.GetItemFromID(i)
            #         print(email_to_delete.Subject)
            #         email_to_delete.Delete()
            #     except:
            #         counter += 1
            #         print(f"{counter} errors")
            #         continue
            
            
            # In[13]:
            
            
            
            dataframe_logger(TROUBLESHOOTING_DF, '20277.xlsx', trim_at = 5000)        
            raise SystemExit('Done')
            
            
            # In[ ]:
            
            
            TROUBLESHOOTING_DF
            

    schedule_logger('Finished', pyfile= __file__)

except SystemExit:
    schedule_logger('Finished', pyfile= __file__)
    raise
    
except Exception as e:
    email_error_to_automation(e, __file__)
    schedule_logger('Error', pyfile= __file__)

            