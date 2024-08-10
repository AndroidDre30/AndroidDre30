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
            

            
            # In[17]:
            
            
            import sys
            sys.path.insert(0, "Y:\Automation\Team Scripts\Andrew Kim\my modules")
            import datetime
            import os
            import re
            from glob import glob
            import pickle
            import requests 
            import shutil
            from pathlib import Path
            import socket
            import time
            
            import pandas as pd
            import numpy as np
            from pypdf import PdfReader
            from O365 import Account, FileSystemTokenBackend
            from O365.message import Message
            from pdf2image import convert_from_path
            import pytesseract
            
            import pensionpro_v1 as pp
            import pensionpro as pp1
            
            from IPython.display import display, HTML
            pd.set_option('display.max_rows',None)
            pd.set_option('display.max_columns',None)
            
            
            # # Resources
            
            # In[18]:
            
            
            hostname = socket.gethostname()
            
            if hostname == 'N4K-REM-59WTPN3':
                # Directories
                downloads_directory = './downloads'
                # nova_census_oauth = r"C:\Users\akim\Documents\WPy64-31050\notebooks\novacensus_oauth_token.txt"
                # nova_link_oauth = r"C:\Users\akim\Documents\WPy64-31050\notebooks\novalink_oauth_token.txt"
                # automation_oauth = r"C:\Users\akim\Documents\WPy64-31050\notebooks\automation_oauth_token.txt"
                nova_census_oauth = r"novacensus_oauth_token.txt"
                nova_link_oauth = r"novalink_oauth_token.txt"
                automation_oauth = r"automation_oauth_token.txt"
            
                # OCR stuff
                os.environ["PATH"] += (';' + os.path.join(os.getcwd(), 'poppler bin'))
                pytesseract.pytesseract.tesseract_cmd = r"C:\Users\akim\AppData\Local\Programs\Tesseract-OCR\tesseract.exe"
            
                
            elif hostname == 'N4K-AUTO01':
                
                # Directories
                downloads_directory = r'C:\Users\Public\WPy64-39100\notebooks\scheduler\21853_downloads'
                # nova_census_oauth = r"C:\Users\akim\Documents\WPy64-31050\notebooks\novacensus_oauth_token.txt"
                # nova_link_oauth = r"C:\Users\akim\Documents\WPy64-31050\notebooks\novalink_oauth_token.txt"
                # automation_oauth = r"C:\Users\akim\Documents\WPy64-31050\notebooks\automation_oauth_token.txt"
                nova_census_oauth = r"novacensus_oauth_token.txt"
                nova_link_oauth = r"novalink_oauth_token.txt"
                automation_oauth = r"automation_oauth_token.txt"
                
                
                # OCR stuff
                os.environ["PATH"] += ";C:\\Users\\Public\\WPy64-39100\\notebooks\\scheduler\\poppler bin"
                pytesseract.pytesseract.tesseract_cmd = r"C:\Program Files\Tesseract-OCR\tesseract.exe"
                
            
                
                
            # Oauth Stuff
            novacensus_client_id = '80cf8a9d-cf23-4511-8bf3-6e9783241671'
            novacensus_secret_value = 'q7Y8Q~JkwQ4L4iBdpBJmzyb8M~mSE1ChPPdPPaWt'
            novalink_client_id = '3fcc1be8-f7aa-433b-87e0-ac6dd1297622'
            novalink_secret_value = '1Tb8Q~JH5QqLbI4vHApgdd6XMQt9PaPyLYhOVabD'
            automation_client_id = "fe12d3a6-9b60-4764-ab55-868fd4533247"
            automation_secret_value = "qtw8Q~HFdlP1RR4yES4e8paQOglCiieXHR8gvbOZ"
            
            novalink_payroll_access_setup_template_id = pp.get_project_template_by_name("Novalink Payroll Access Setup")[0]["Id"]
            
            
            # The program is broken down into multiple stages rather than a single massive loop. 
            # So each stage will have to reference this dataframe.
            
            main_reference_df = pd.DataFrame({'run_time':[''],
                                            'email_received':[''],
                                            'time_email_received':[''],
                                              'pdf_planid':[''],
                                             'found_email_attachments':[''],
                                            'payroll_super_user':[''],
                                              'payroll_super_user_email':[''],
                                              'regular_payroll_user':[''],
                                              'regular_payroll_user_email':[''],
                                             'email_destination':[''],
                                              'incomplete_data':[''],
                                             'previously_invited?':[''],
                                             'previously_processed?':[''],
                                              'contact_added':[''],
                                              'files_moved':['']
                                             })
            
            
            
            # Commented out. I fugured the better way to tell if a plan was procesed was by adding a note instead of saving to pickle.
            # Note will be added to plan by add_note_for_ea_addendum_received()
            
            # This dataframe is a saved record of every valid planid that was processed with this program. 
            # While you are iterating through the emails, the plan id matches up with the ones recorded in this dataframe,
            # a warning should be emailed to novalink@nova401k.com. 
            
            # if os.path.exists('processed_plans_history.pkl'):
            #     with open('processed_history.pkl', 'rb') as file:
            #         processed_plans_history = pickle.load(file)
            # else:
            #     processed_plans_history = pd.DataFrame(columns = ['planid','processed_on'])
            # processed_plans_history    
            
            
            # In[ ]:
            
            
            
            
            
            # In[19]:
            
            
            # Grab all plan ids in one call.
            # This is for checking if the plan id in the pdf is valid. Also, if a new contact is found and added to pension pro,
            # you also need to specify the company name into the new contact. Referring to this df is a faster way to do that
            # instead of making a bunch of calls.
            
            ALL_PLANS = pd.DataFrame(pp.get_plans(get_all=True, select='Client,InternalPlanId',expand="Client.CompanyName"))
            ALL_PLANS["CompanyNameId"] = ALL_PLANS["Client"].apply(lambda x: x["CompanyName"]["Id"]) # Used for adding new contacts and defining their Company field.
            
            
            # # Functions and Tools
            
            # In[4]:
            
            
            def generate_authentication_link(tpa_id):
                """
                UNCOMMENT THE URL_KEY BEFORE USE!
                """
                url_key = "bgs872jw77" # Production
                #url_key = "kr7fe09ic8" # Test
                
                url = f"https://{url_key}.execute-api.us-east-1.amazonaws.com/getInvitationLink?plan_id={tpa_id}"
                r = requests.get(url)
                description = r.text
                
                return description 
                
            
            
            def create_novalink_folder(planid):
                
                g_drive = os.listdir('G:/')
                
                for folder in g_drive:
                    
                    if folder.split()[0] == str(planid):
                        target_folder =f'G:/{folder}/Novalink'
                        if not os.path.exists(target_folder):
                            print(f"{target_folder} doesn't exist. Creating the folder.")
                            os.makedirs(target_folder)                
                        return target_folder
                    
                return False
            
            
            def move_file_with_increment(source, destination):
                """
                Move a file from source to destination.
                If a file with the same name exists at the destination,
                append a number to its name.
            
                :param source: str - The path to the source file.
                :param destination: str - The directory to move the file to.
                """
                # Check if the source file exists
                if not os.path.isfile(source):
                    print(f"The source file does not exist: {source}")
                    return
                
                # Get the file name and check if it exists in the destination directory
                file_name = os.path.basename(source)
                base_name, extension = os.path.splitext(file_name)
                incremented_file_name = file_name
                i = 1
                
                # Check if a file with the same name exists in the destination directory
                while os.path.isfile(os.path.join(destination, incremented_file_name)):
                    # If it does, append a number to the file name
                    incremented_file_name = f"{base_name}{i}{extension}"
                    i += 1
                
                # Move the file to the destination with the new name
                destination_file_path = os.path.join(destination, incremented_file_name)
                shutil.move(source, destination_file_path)
                print(f"File moved to: {destination_file_path}")
            
            class grab_contacts_from_ea_addendum:
            
                def __init__(self, raw_pdf_text):
                    self.raw_pdf_text = raw_pdf_text.split('\n')
                    
                    # Once you grab all the text from a pdf using tesseract, grab all text after the 
                    # phrase 'additional required information'. This is the important information from page 2. 
                    # Then, weed out all the strings that contain anything from the filter_list. This is not important.
                    self.filter_list = [
                        "“Super User” for Payroll System ",
                        "\"Super User\" for Payroll System ",
                        'for payroll system',
                        'first name last name email address',
                        'Regular Payroll Contact',
                        'Reference ID (found in the email)',
                        'Reference ID (found inthe email)',
                        'Page 2 of 2',
                        ]
                    self.super_user_first = ""
                    self.super_user_last = ""
                    self.super_user_email = ""
                    self.regular_payroll_first = ""
                    self.regular_payroll_last = ""
                    self.regular_payroll_email = ""
                    self.planid = ""
                    
                    
                def erase_these_words(self,string_to_check, reference_list):
                    """
                    Immediately return True if any string from reference_list is found in string_to_check.
                    Used to weed out useless strings in define_contact_info()
                    """
            
                    reference_list = [i.lower() for i in reference_list]
                    string_to_check = string_to_check.lower()
            
                    for list_value in reference_list:
                        string_to_check = string_to_check.replace(list_value,'')
                    string_to_check = string_to_check.strip()
                    return string_to_check
            
            
                def define_contact_info(self):
                    target_info_list = [] # Temporary staging list. 
                    
                    bool_flag = False
            
                    for i in self.raw_pdf_text:
                        
                        # Ignore all text until you reach the signature portion of the pdf. 
                        # Start appending all signature information to list. 
                        if 'additional required information' in i.lower():
                            bool_flag = True
                            continue
                        if bool_flag == True:
                            target_info_list.append(i)
                            
                            
                    # I need to write getters and setters for this. This is awful.    
                    try:
                        target_info_list = [self.erase_these_words(i, self.filter_list) for i in target_info_list]
                        target_info_list = [i for i in target_info_list if i]
            
                        self.super_user_first = target_info_list[0].split()[0].strip().title()
                        self.super_user_last = target_info_list[0].split()[1].strip().title()
                        self.super_user_email = target_info_list[0].split()[2:]
                        # Sometimes, the .com is separated from the rest of the email address and turns into another list item.
                        # No idea why that happens. connect them back if this occurs.
                        self.super_user_email = ''.join(self.super_user_email).strip()  
                        if self.super_user_email.endswith('.c'):  # in case emails get cut off since there is no room.
                            self.super_user_email = self.super_user_email + 'om'
                        if self.super_user_email.endswith('.co'):  # in case emails get cut off since there is no room.
                            self.super_user_email = self.super_user_email + 'm'
                            
                        self.regular_payroll_first = target_info_list[1].split()[0].strip().title()
                        self.regular_payroll_last = target_info_list[1].split()[1].strip().title()
                        self.regular_payroll_email = target_info_list[1].split()[2:]
                        self.regular_payroll_email = ''.join(self.regular_payroll_email).strip() 
                        if self.regular_payroll_email.endswith('.c'): 
                            self.regular_payroll_email = self.regular_payroll_email + 'om'
                        if self.regular_payroll_email.endswith('.co'): 
                            self.regular_payroll_email = self.regular_payroll_email + 'm'
                            
                        self.planid = target_info_list[2].split()[0].strip()
                        if self.planid.lower() == 'reference':
                            self.planid = target_info_list[2].split()[-1].strip()
                    
                    except:
                        return 1 # Information parsing has failed.
                    
                    # Check for invalid names. Return 2 so the program can mark it down on the data frame as invalid. 
                    contact_names = [self.super_user_first,self.super_user_last,self.regular_payroll_first,self.regular_payroll_last]
                    for i in contact_names:
                        # Invalid data. Cannot abbreviate names. 
                        if len(i) == 1:
                            return 2 
                        elif (len(i) == 2) and (i[-1] == "."):
                            return 2
                    
                    # Basic checks to see if email is valid. Must have "@" or a dot. (.com, .net, etc)
                    for i in [self.super_user_email,self.regular_payroll_email]:
                        if ("@" not in i) or ("." not in i):
                            return 2
                    
                    return 0
                
            def save_eml_with_unique_name(EMAIL_SUBJECT,DESTINATION_FOLDER):
                """
                Cleans up an email subject and saves it as a unique file name. Returns a file path of its destination.
                
                Intended to be used only with O365 library.
                """
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
            
            
            
            def check_for_previous_invitation(planid):
                """
                Requires pandas as pd.
                Requires get_interactions_by_planid
                Requires get_interaction_by_interaction_id
                
                Goes through the plan Interactions tab. Finds any Novalink invitation and returns True if found. 
                Else, returns False. 
                """
                plan_interactions = pd.DataFrame(pp.get_interactions_by_planid(planid))
                if len(plan_interactions) == 0:
                    return False
                
                plan_interactions = plan_interactions[plan_interactions["Title"].str.contains('Valuable Services Coming to Nova for')]
                if len(plan_interactions) == 0:
                    return False
                
                for i in plan_interactions.index[:]:
                    interaction_id = plan_interactions.at[i,"Id"]
                    interaction_body = pp.get_interaction_by_interaction_id(interaction_id)["Details"]
                    if ("Novalink" in interaction_body) and (planid in interaction_body):
                        return True
                
                return False   
            
            
            
            def close_project(project_id):
                """
                Written by Andrew.
                Closes all tasks of a project.
                """
                # Get all active task groups with their tasks. 
                tasks_of_active_task_groups = [i["Tasks"] for i in pp.get_task_groups_by_projectid(project_id,expand="Tasks",filters="DateCompleted eq null")]
                
                
                # Each task is a dictionary. A task group is a list with dictionaries. Break these dictionaries out of its parent list
                # by extending each list into a single list.    
                combined_tasks = []
                for tasks in tasks_of_active_task_groups: 
                    combined_tasks.extend(tasks)
                
                # Filter tasks based on having no completion date. Override in sequence. 
                all_active_tasks = [i for i in combined_tasks if not i["DateCompleted"]]
                for task in all_active_tasks:
                    pp.override_task(task["Id"])
            
                # Keep a record of all completed tasks just in case you need to undo it.
                
                # If you ever need to uncomplete these, be sure to do a try/except.
                # Uncompleting a task in a task group will uncomplete everything down stream of a task group. 
                # This means you may try to uncomplete a task that was already uncompleted. 
                return [i["Id"] for i in all_active_tasks] 
            
            
            def dataframe_logger(dataframe, destination_path, trim_at = 1000):
                """
                Requires os and pandas. 
                
                Saves a dataframe as an excel file for error logging. If its found at the destination,
                it will concact onto it. It will trim the newly concatenated dataframe if there are more than 500 rows.
                
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
            
                    
            def dataframe_loader(file_path, date_column_name: str, start_date, end_date = datetime.datetime.now(), datetime_format = None):
                """
                Requirements: 
                os, datetime, and pandas (as pd).
                
                Purpose: 
                Returns a dataframe from a saved file and the rows for the specified date range. Used for undoing a bunch of mistakes 
                for a given timeframe. 
                
                ==================================Parameters==============================================================================
                
                file_path : Loads up a dataframe you saved as a file somewhere. Only use 3 file types cus I dont care. 
                
                date_column_name : Case Insensitive. This assumes that the saved dataframe also has a column with a list of dates that the 
                programmer has previously added. The programmer should have added this to the df and it should correspond with when they 
                ran a specific action (such as a project launch). You need to point out what the name of that column is. 
                
                start_date :
                end_date: Specify a range you want to look at. The start_date should be the oldest date. The end_date should be the
                youngest date. It assumes end_date is current time if you dont specify it. Its ok to write it as 'mm/dd/yy' format or 
                'mm/dd/yy hh:mm:ss' format. Or whatever format if you have a lot of faith with to_datetime(). 
                
                datetime_format : You can leave this as None. Manually specify how pd.to_datetime() should read the strings under the
                date columns. Used when the datetime column was written in a really messed up way. The pd.to_datetime() method is pretty 
                decent at figuring out how it should read the date strings but some past programmers can be very 'creative'. 
                
                """
                
                if isinstance(date_column_name,str) == False:
                    raise ValueError("date_column_name must be a string.")
                    
                file_type = os.path.splitext(file_path)[1]
                if file_type == '.xlsx':
                    df = pd.read_excel(file_path)
                elif file_type == '.pkl':
                    df = pd.read_pickle(file_path)
                elif file_type == '.csv':
                    df = pd.read_csv(file_paht)
                else:
                    raise ValueError("File path is not xlsx, pkl, or csv.") # I expect to only use these file types. 
                df = df.astype(object)
                
                # Check if inputted date column name actually exists in df. 
                # Use the lower cased names to find the index postion.
                # Use that index position to return the properly cased column name from the actual df. 
                date_column_name = date_column_name.lower()
                lower_cased_columns = [i.lower() for i in df.columns]
                if date_column_name not in lower_cased_columns:
                    raise ValueError("Your specified date column doesn't exist in the dataframe.")
                index_postion = lower_cased_columns.index(date_column_name)
                date_column_name = df.columns[index_postion]
                
                
                start_date = pd.to_datetime(start_date)
                end_date = pd.to_datetime(end_date)
                    
                    
                df[date_column_name] = pd.to_datetime(df[date_column_name], format = datetime_format)
                df = df[(df[date_column_name] > start_date) & (df[date_column_name] < end_date)]
                df.reset_index(inplace=True)
                df.drop(["index"], axis=1, inplace = True)
                df = df.astype(object)
            
                
                return df
            
            
            def add_note_for_ea_addendum_received(planid):
                """
                Requires datetime and pension pro. 
                
                Mark a plan as having their EA Addendum document as processed. I decided this is better than having a persistent 
                pickle file and saving all processed plans. I defined a note category id and my own contact id
                in case these ever need to be found with power tools. 
                """
                
                payload = {
                "PlanId" : pp.get_sysplanid(planid),
                "NoteText": f"EA Addendum processed on {datetime.datetime.now().strftime('%m/%d/%Y %H:%M:%S')}\n\nIf you need to send another invitation email, delete this note and roll the task back to the Authentication Email task.",
                "NoteCategoryId": 3199, # 3199 note category for 'Plan Document'
                "CreatedByContactId":1377745 # Perhaps change this to a neutral automation account later? 
                }
                
                results = pp.add_note(payload)
                
                return results
            
            
            def check_plan_if_ea_addendum_processed(planid):
                plan_notes = pp.get_notes_by_planid(planid)
                for i in plan_notes:
                    if i["NoteText"][0:25] == "EA Addendum processed on ":
                        return True
                return False
            
            
            # # OAuth Authentication for the 2 emails
            
            # In[5]:
            
            
            # Nova Census stuff
            token_backend = FileSystemTokenBackend(token_filename=nova_census_oauth)
            novacensus_account = Account((novacensus_client_id,novacensus_secret_value),
                              token_backend=token_backend,
                              scopes = ['basic','message_all','mailbox'],
                             )
            
            if not novacensus_account.is_authenticated:
                novacensus_account.authenticate()
            print(novacensus_account.is_authenticated)
            
            
            # In[6]:
            
            
            # Automation email account stuff
            token_backend = FileSystemTokenBackend(token_filename=automation_oauth)
            automation_account = Account((automation_client_id,automation_secret_value),
                              token_backend=token_backend,
                              scopes = ['basic','message_all','mailbox'],
                             )
            
            if not automation_account.is_authenticated:
                automation_account.authenticate()
            print(automation_account.is_authenticated)
            
            
            # In[7]:
            
            
            # Nova Link Stuff
            
            # If a TPA id can be determined from the novacensus emails, you need to use the reference numbers in the novalink emails
            # and determine the plan id from there. 
            
            token_backend = FileSystemTokenBackend(token_filename=nova_link_oauth)
            novalink_account = Account((novalink_client_id,novalink_secret_value),
                              token_backend=token_backend,
                              scopes = ['basic','message_all','mailbox'],
                             )
            
            if not novalink_account.is_authenticated:
                novalink_account.authenticate()
            print(novalink_account.is_authenticated)
            
            
            # # Stage 1
            # ## Download PDF's from emails and collect their info one by one. 
            # ### All information recorded should go to the main reference df since this program is too gigantic to put into one big mega loop.
            
            # In[8]:
            
            
            #raise SystemExit() # Remove when you're done testing
            inbox_emails_to_erase = [] # Store email ID's here. Clean up the inbox after looping. 
            
            novacensus_mailbox = novacensus_account.mailbox().inbox_folder()
            novacensus_emails = novacensus_mailbox.get_messages(limit=999,download_attachments=True)
            
            index = 0
            for email in novacensus_emails:
            
                if email.has_attachments == False:
                    continue
                if "EA Addendum.pdf" not in email.subject:
                    continue
                    
                # Populate the reference df
                main_reference_df.at[index, 'run_time'] = datetime.datetime.now().strftime('%m/%d/%y %H:%M:%S')
                main_reference_df.at[index,'email_received'] = email.subject                  # Do i really need this?
                main_reference_df.at[index,'time_email_received'] = email.received.strftime('%m/%d/%y %H:%M:%S')
            
                
                
                # Download the attachments.
                downloaded_files = []
                for attachment in email.attachments:
            
                    if ".pdf" in attachment.name:
                        print(f"Attachment Name: {attachment.name}")
                        attachment.save(downloads_directory)           # You can't rename with the 'save' method. Save it first then rename. 
                        downloaded_files.append(attachment.name)
                # Turn list into single string separated by  '/'. You can turn this back into a list later with split('/')
                main_reference_df.at[index, 'found_email_attachments'] = '/'.join(downloaded_files)   
                                
                    
                    
                #=======================================================================================================
                #     Grab signature information from pdf.
                
                #     You cant just grab the text from a pdf using a straight forward solution. The raw text from the pdf is 
                #     jumbled because some text was filled out using text boxes from docusign. Docusign wont save the metadata so you 
                #     cant query those data directly from pdf either. I also tried turning a pdf into excel to force the text 
                #     to align properly. This only works when manually done in Adobe Pro. Free libraries wont do this properly.
                #     OCR technology is the only solution I know.
                #=======================================================================================================
                
                # Convert pdf to image using poppler.
                image = convert_from_path(f"{downloads_directory}/EA Addendum.pdf")[-1] # Only grab 2nd page. 
                
                # Use tesseract to turn image into text.
                raw_text = pytesseract.image_to_string(image,config='--oem 1 --psm 4')        
                
                # Create class instance for grabbing contact info and plan id from pdf. 
                # Call method to filter the text and define contact information in self attributes. 
                addendum_contact = grab_contacts_from_ea_addendum(raw_text)
                # Will return 0 if its good. 1 if the parsing outright failed. 2 if the names or emails are invalid. 
                addendum_contact_error_code = addendum_contact.define_contact_info()  
                
                
                #=======================================================================================================
                #     If the pdf information gathering portion messed up OR...
                #     If the Plan Id grabbed from the PDF doesn't exist in Pension Pro, mark it as invalid. Download the email
                #     so you can send it with a warning to novalink. Stop all processing. Delete downloaded attachments. 
                #=======================================================================================================
                if (addendum_contact_error_code == 1) or (addendum_contact.planid not in ALL_PLANS["InternalPlanId"].values):
                    main_reference_df.at[index, 'pdf_planid'] = f"{addendum_contact.planid} - invalid"
                    # Return a unique path. If it exists, append a number to it. 
                    eml_download_path = Path(save_eml_with_unique_name(email.subject,f"{downloads_directory}")).absolute()
                    email.save_as_eml(eml_download_path)
                    main_reference_df.at[index, 'email_destination'] = str(eml_download_path)
                    
                    for file in downloaded_files:
                        os.remove(f"{downloads_directory}/{file}")
                        
                    inbox_emails_to_erase.append(email.object_id) 
                    
                    index += 1
                    continue
                    
                print(f"PDF info parsed successfully for plan id {addendum_contact.planid}")
                
                #=======================================================================================================
                #    Check if the user put in abbreviated names or invalid emails. 
                #=======================================================================================================
                if (addendum_contact_error_code == 2):
                    main_reference_df.at[index, 'incomplete_data'] = "incomplete"
                    eml_download_path = Path(save_eml_with_unique_name(email.subject,f"{downloads_directory}")).absolute()
                    email.save_as_eml(eml_download_path)
                    main_reference_df.at[index, 'email_destination'] = str(eml_download_path)
                    
                    for file in downloaded_files:
                        os.remove(f"{downloads_directory}/{file}")
                    
                    inbox_emails_to_erase.append(email.object_id) 
                    
                    index += 1
                    continue
                if (addendum_contact_error_code == 0):
                    main_reference_df.at[index, 'incomplete_data'] = ""
                    
                print("Names and emails parsed:")
                print(addendum_contact.super_user_first)
                print(addendum_contact.super_user_last)
                print(addendum_contact.super_user_email)
                print(addendum_contact.regular_payroll_first)
                print(addendum_contact.regular_payroll_last)
                print(addendum_contact.regular_payroll_email)
                print(addendum_contact.planid)    
                
                #=======================================================================================================
                #     After you grab the plan id from the pdf, you can now rename them with the planid prepended. 
                #     You cant move them to the plan directory at this stage. The program doesn't even know if the pdf is messed up 
                #     so it must be done at a later stage. 
                #=======================================================================================================
                
                for file in downloaded_files:
                    os.rename(f"{downloads_directory}/{file}", f"{downloads_directory}/{addendum_contact.planid}_{file}")
                    
                #=======================================================================================================
                #     Check if a plan was previously invited. If a pdf was returned to us and no previous invite was sent,
                #     we have a problem. If we blindly process for the Plan ID on the pdf, we will end up opting another
                #     client into Novalink. novalink@nova401k.com should be warned. 
                #=======================================================================================================
                
                print(f"Checking {addendum_contact.planid} interactions tab for previous invitation. ")              
                previously_invited = check_for_previous_invitation(addendum_contact.planid)
                              
                if previously_invited == True:
                    main_reference_df.at[index, 'previously_invited?'] = True
                    print("Previously invited.")
                    
                else:  # Else, mark this as previously received in the "main_reference_df". A warning must be sent out to novalink.     
                    main_reference_df.at[index, 'previously_invited?'] = False
                    print("NOT INVITED! Client could have inputted the wrong plan id and is accidentally launching novalink on the behalf of other plans. Or someone simply forgot to add the invitation to the Interactions tab.")
                    
                
                #=======================================================================================================
                #     If records show that we have previously processed this pdf before and we got it again this time, this is bad. 
                #     Mark it in the main reference df and send warning to novalink. 
                #=======================================================================================================
                
                
                # I previously checked for this using a persistent pickle file of all processed plans. Now I'm checking using plan notes.
                # All successful plans had this note previously added when they ran through this script.
                previously_processed_flag = check_plan_if_ea_addendum_processed(addendum_contact.planid)  
                
                if previously_processed_flag:
                    main_reference_df.at[index, 'previously_processed?'] = True # not good. We should not be processing a form for a processed plan. 
                else:
                    main_reference_df.at[index, 'previously_processed?'] = False
                    
                    
                    
                # Clean the email subject of invalid characters and download it to the client folder. Use the subject as title.
                if (previously_invited == True) and (previously_processed_flag == False):
                    eml_download_path = Path(save_eml_with_unique_name(email.subject,create_novalink_folder(addendum_contact.planid))).absolute()
                    email.save_as_eml(eml_download_path)
                    main_reference_df.at[index, 'email_destination'] = str(eml_download_path)
                    print(f"Email saved to {str(eml_download_path)}")
                else:
                    # If a plan was not invited or we received a duplicate doc, save it under the downloads directory and dont move it anywhere.
                    # Its going to be attached to the warning email so a human can examine what happened.
                    eml_download_path = Path(save_eml_with_unique_name(email.subject,f"{downloads_directory}")).absolute()
                    email.save_as_eml(eml_download_path)
                    main_reference_df.at[index, 'email_destination'] = str(eml_download_path)
                    print(f"Email saved to {str(eml_download_path)} and will be sent out with the error email.")
                
                # Populate the reference df for use in the next phase. 
                main_reference_df.at[index, 'pdf_planid'] = addendum_contact.planid 
                main_reference_df.at[index, 'payroll_super_user'] = f"{addendum_contact.super_user_first} {addendum_contact.super_user_last}"
                main_reference_df.at[index, 'regular_payroll_user'] = f"{addendum_contact.regular_payroll_first} {addendum_contact.regular_payroll_last}"
                main_reference_df.at[index, 'payroll_super_user_email'] = addendum_contact.super_user_email
                main_reference_df.at[index, 'regular_payroll_user_email'] = addendum_contact.regular_payroll_email 
                
                
                
                inbox_emails_to_erase.append(email.object_id) 
                
                
                index += 1
                
            
            
            # In[9]:
            
            
            main_reference_df
            
            
            # In[10]:
            
            
            if main_reference_df.at[0,'run_time'] == '':
                print('No emails to process.')
                raise SystemExit()
            
            
            # In[11]:
            
            
            #main_reference_df.at[6,'email_destination'] #="C:\\Users\\Public\\WPy64-39100\\notebooks\\scheduler\\21853_downloads\\Completed Complete with DocuSign EA Addendum.pdf.msg"
            
            
            # # Stage 2
            # ## Send emails for all problem plans. Get these out of the way before proceeding with the good plans
            # 
            
            # In[12]:
            
            
            #raise SystemExit() # Remove when you're done testing
            
            
            # Emails to send if no invitations are found.
            for i in main_reference_df[main_reference_df['previously_invited?'] == False].index[:]:
                
                planid = main_reference_df.at[i,'pdf_planid']
                eml_path = main_reference_df.at[i,'email_destination']
                
                if planid not in ALL_PLANS["InternalPlanId"].values:
                    # If a planid has never been invited and it doesn't even exist in the system, skip this email. Instead,send the email
                    # that states that the planid doesn't exist in the system (next block)
                    continue 
                    
                print(f"No previous invite found for {planid}. Sending email.")    
                message_object = automation_account.mailbox().new_message()                  
                message_object.to.add(['novalink@nova401k.com'])              
                message_object.subject = f'Plan ID {planid}: PDF With No Matching Invite'
                message_object.body = "The Plan ID in the PDF doesn't match with any previously known invite email. This means the plan was never invited in the first place. Please make sure the Plan ID is correct in the reference form."
                message_object.attachments.add(eml_path)
                message_object.send()
                os.remove(eml_path)
            
                
                
            # Emails to send if plan id doesn't exist in Pension Pro.
            for i in main_reference_df[~main_reference_df['pdf_planid'].isin(ALL_PLANS["InternalPlanId"])].index[:]:
                
                planid = main_reference_df.at[i,'pdf_planid']
                eml_path = main_reference_df.at[i,'email_destination']
                
                if str(planid) == 'nan':
                    continue 
                    
                if planid not in ALL_PLANS["InternalPlanId"].values:
                    print(f"{planid} doesn't exist in Pension Pro. Sending email.")
                    message_object = automation_account.mailbox().new_message()                  
                    message_object.to.add(['novalink@nova401k.com'])              
                    message_object.subject = 'Novalink Agreement – TPA ID Mismatch'
                    message_object.body = "The reference ID from the Novalink Service Agreement is not a match to any TPA ID in Pension Pro. Please reply back with the correct TPA ID."
                    message_object.attachments.add(eml_path)
                    message_object.send()
                    os.remove(eml_path)
                    
            
                
            # Emails to send if we already processed a pdf for this client and they sent another one. 
            for i in main_reference_df[main_reference_df['previously_processed?'] == True].index[:]:
                
                planid = main_reference_df.at[i,'pdf_planid']
                eml_path = main_reference_df.at[i,'email_destination']
                
                message_object = automation_account.mailbox().new_message()                  
                message_object.to.add(['novalink@nova401k.com'])              
                message_object.subject = 'Novalink Agreement – Multiple Agreements'
                message_object.body = "Multiple signed agreements were received for the same client (see attached agreements). Please reply back confirming the correct agreement."
                message_object.attachments.add(eml_path)
                message_object.send()    
                os.remove(eml_path)
            
                
            for i in main_reference_df[main_reference_df['incomplete_data'] == "incomplete"].index[:]:
            
                planid = main_reference_df.at[i,'pdf_planid']
                eml_path = main_reference_df.at[i,'email_destination']
                
                message_object = novacensus_account.mailbox().new_message()                  
                message_object.to.add(['novalink@nova401k.com'])              
                message_object.subject = 'EA Addendum – Incorrect Fields'
                message_object.body = "There were one or more fields completed with incorrect information. Contact client to get correct information and re-submit addendum."
                message_object.attachments.add(eml_path)
                message_object.send()    
                os.remove(eml_path)
            
            
            # # Stage 3
            # ## For all valid pdf submissions, add the contacts to the plan
            
            # In[13]:
            
            
            #raise SystemExit() # Remove when you're done testing
            
            # Filters for all good submissions
            filt1 = main_reference_df['previously_invited?'] == True
            filt2 = main_reference_df['pdf_planid'].isin(ALL_PLANS["InternalPlanId"])
            filt3 = main_reference_df['previously_processed?'] == False
            filt4 = main_reference_df['incomplete_data'] != "incomplete"
            filt5 = main_reference_df['contact_added'] != True
            
            for i in main_reference_df[filt1 & filt2 & filt3 & filt4 & filt5].index[:]:
                
                planid = main_reference_df.at[i, "pdf_planid"]
                super_user_first = main_reference_df.at[i, 'payroll_super_user'].split()[0]
                super_user_last = main_reference_df.at[i, 'payroll_super_user'].split()[-1]
                super_user_email = main_reference_df.at[i, 'payroll_super_user_email']
                regular_payroll_first = main_reference_df.at[i, 'regular_payroll_user'].split()[0]
                regular_payroll_last = main_reference_df.at[i, 'regular_payroll_user'].split()[-1]
                regular_payroll_email = main_reference_df.at[i, 'regular_payroll_user_email']
                company_name_id = ALL_PLANS.loc[ALL_PLANS["InternalPlanId"] == planid, "CompanyNameId"].iloc[0]
                
                
                
                
                # If the contact is found in Pension Pro, add him/her as Novalink Payroll Super User.
                # Else, add the person in Pension Pro first and then add them as contact.
                
                super_user_search_results = pp.get_contacts(filters=f"Email eq '{super_user_email}'") # Search for super user using a straight search.
                if (len(super_user_search_results) == 0) and len(super_user_email) > 8:
                    # If the results of a straight search is empty try a partial search. 9 characters should be enough for a competent search.
                    super_user_search_results = pp.get_contacts(filters=f"contains(Email,'{super_user_email}')")
                
                if super_user_search_results:
                    print(f"Contact found in Pension Pro. Adding {super_user_first} {super_user_last} as a Novalink Payroll Superuser.")
                    super_user_contact_id = super_user_search_results[0]['Id']
                    
                    try:
                        pp.add_plan_contact_role(planid, super_user_contact_id, 550155)
                    except Exception as e:
                        e = str(e)
                        if "The selected role is already assigned to this contact" in e:
                            print(e)
                        else:
                            print(e)
                            raise Exception(f"Failed to add super user to {planid}")
            
                else:
                    print(f"{super_user_first} {super_user_last} could not be found in Pension Pro. Adding new user to system.")
            
                    new_contact_payload =     {
                      "FirstName": super_user_first,
                      "Initial": "",
                      "LastName": super_user_last,
                      "Salutation": super_user_first,
                      "Designation": "",
                      "Title": "",
                      "Email": super_user_email,
                      "CanBulkEmail": True,
                      "CanEmail": True,
                      "CanCall": True,
                      "ContactStatusId": 281,
                      "HasBeenWarned": False,
                      "IsDeactivated": False,
                      "CompanyNameId":int(company_name_id)
                    }
                    
            
                    add_contact_results = pp.add_new_contact(new_contact_payload)     
                    time.sleep(1)
                    pp.add_plan_contact_role(planid, add_contact_results["Id"],550155) # Novalink Payroll Contact contact role ID: 550155
            
            
            
                # If the contact is found in Pension Pro, add him/her as regular Novalink Payroll User.
                # Else, add the person in Pension Pro first and then add them as contact.
                payroll_user_search_results = pp.get_contacts(filters=f"Email eq '{regular_payroll_email}'")
                if (len(payroll_user_search_results) == 0) and len(regular_payroll_email) > 8:
                    # If the results of a straight search is empty try a partial search. 9 characters should be enough for a competent search.
                    payroll_user_search_results = pp.get_contacts(filters=f"contains(Email,'{regular_payroll_email}')")
                    
                if payroll_user_search_results:
                    print(f"Contact found in Pension Pro. Adding {regular_payroll_first} {regular_payroll_last} as a regular Novalink Payroll Contact.")
                    payroll_user_contact_id = payroll_user_search_results[0]['Id']
                    
                    try:
                        pp.add_plan_contact_role(planid, payroll_user_contact_id, 550156)
                    except Exception as e:
                        e = str(e)
                        if "The selected role is already assigned to this contact" in e:
                            print(e)
                        else:
                            print(e)
                            raise Exception(f"Failed to add super user to {planid}")
                            
                else:
                    print(f"{regular_payroll_first} {regular_payroll_last} could not be found in Pension Pro. Adding new user to system.")
            
                    new_contact_payload =     {
                      "FirstName": regular_payroll_first,
                      "Initial": "",
                      "LastName": regular_payroll_last,
                      "Salutation": regular_payroll_first,
                      "Designation": "",
                      "Title": "",
                      "Email": regular_payroll_email,
                      "CanBulkEmail": True,
                      "CanEmail": True,
                      "CanCall": True,
                      "ContactStatusId": 281,
                      "HasBeenWarned": False,
                      "IsDeactivated": False,
                      "CompanyNameId":int(company_name_id)
                    }
            
                    add_contact_results = pp.add_new_contact(new_contact_payload)
                    time.sleep(1)
                    pp.add_plan_contact_role(planid, add_contact_results["Id"],550156)   # Novalink Payroll Contact contact role ID: 550156  
            
            
            
                main_reference_df.at[i,'contact_added'] = True
                
                plan_novalink_directory = create_novalink_folder(planid)
                for file in glob(f"{downloads_directory}/{planid}_*.pdf"):
                    move_file_with_increment(file, plan_novalink_directory)
                main_reference_df.at[i,'files_moved'] = True    
                
                
                
                # Mark down plan as having ther ea addendum processed. This will be used in the future to prevent 
                # multiple processing in the future. 
                add_note_for_ea_addendum_received(planid)
                
                
                print('\n\n')
            main_reference_df
            
            
            # # Stage 4
            # ### For all good plans, create authentication link by doing a get request. Take the response url and add it as a "Services Provided". The description of the services provided will be the url. 
            
            # In[14]:
            
            
            TODAY = datetime.datetime.now().strftime("%m/%d/%Y")
            TODAY_PLUS_30_DAYS = (datetime.datetime.now()+ datetime.timedelta(days=30)).strftime("%m/%d/%Y")
            
            for i in main_reference_df[main_reference_df['contact_added'] == True].index[:]:
                
                planid = main_reference_df.at[i, "pdf_planid"]
            
                authentication_link = generate_authentication_link(planid)
                  
                try:
                    pp1.add_plan_services_provided(planid, authentication_link,11037)
                except:
                    print(f"Service already exists for plan {planid}.")
                
                # Launch Novalink Payroll Access Setup for plan if it doesn' exist already. 
                plan_projects_novalink = [i for i in pp.get_projects_by_planid(planid) if i["Name"] == "Novalink Payroll Access Setup" and i["CompletedOn"] == None]    
                if len(plan_projects_novalink) == 0:
                    pp.add_project(planid, novalink_payroll_access_setup_template_id, StartDate = TODAY, DueOn = TODAY_PLUS_30_DAYS)
                    print(f"Added 'Novalink Payroll Access Setup' project for {planid}.")
                
                
                # Get all active novalink initial communication projects. If more than one exists, close them all out because I dont care.
                novalink_initial_communication_projects = [i for i in pp.get_projects_by_planid(planid) if "Novalink Initial Communication" in i["Name"] and i["CompletedOn"] == None]
                
                if novalink_initial_communication_projects:
                    print(f"Found {len(novalink_initial_communication_projects)} Novalink Initial Communication Project for {planid}. Closing project...")
                    try:
                        for project in novalink_initial_communication_projects:
                            close_project(project["Id"])
                    except:
                        print(f"Failed to close Novalink Initial Communication Project for {planid}.")
                        continue
                
            
            
            # # Stage 5
            # ## Delete emails so they aren't examined again. If everything went accordingly, the email should have been saved to the plan Novalink folder. If not, an email copy should have been attached to the error email that was sent. 
            # 
            # ## Clean up downloads directory
            
            # In[15]:
            
            
            # Clean up the email so its not evaluated again next run. The emails are saved to the Novalink folder anyways.
            
            novacensus_mailbox = novacensus_account.mailbox().inbox_folder()
            novacensus_emails = novacensus_mailbox.get_messages(limit=999,download_attachments=True)
            counter = 0
            for email in novacensus_emails:
                if email.object_id in inbox_emails_to_erase:
                    email.delete()
                    counter += 1
            print(f"{counter} emails cleared from inbox.")    
            
            for i in glob(downloads_directory + "\\*.pdf"):
                print(f"Deleting {i}")
                os.remove(i)
            
            
            # In[ ]:
            
            
            
            
            
            # In[16]:
            
            
            raise SystemExit("Done")
            

    schedule_logger('Finished', pyfile= __file__)

except SystemExit:
    schedule_logger('Finished', pyfile= __file__)
    raise
    
except Exception as e:
    email_error_to_automation(e, __file__)
    schedule_logger('Error', pyfile= __file__)

            