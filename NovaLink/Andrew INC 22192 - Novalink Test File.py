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
            
            
            import sys
            sys.path.insert(0, "Y:\Automation\Team Scripts\Andrew Kim\my modules")
            import os
            import numpy as np
            from glob import glob
            from pathlib import Path
            import socket
            import copy 
            import shutil
            
            from O365 import Account, FileSystemTokenBackend
            from O365.message import Message
            
            import pandas as pd
            import pensionpro_v1 as pp
            #import pensionpro as pp
            
            
            from IPython.display import display, HTML
            pd.set_option('display.max_rows',None)
            pd.set_option('display.max_columns',None)
            
            
            # In[2]:
            
            
            hostname = socket.gethostname()
            
            if hostname == 'N4K-REM-59WTPN3':
                # Directories
                downloads_directory = './downloads'
                nova_census_oauth = r"C:\Users\akim\Documents\WPy64-31050\notebooks\novacensus_oauth_token.txt"
                nova_link_oauth = r"C:\Users\akim\Documents\WPy64-31050\notebooks\novalink_oauth_token.txt"
                automation_oauth = r"C:\Users\akim\Documents\WPy64-31050\notebooks\automation_oauth_token.txt"  
                
            elif hostname == 'N4K-AUTO01':
                # Directories
                nova_census_oauth = r"novacensus_oauth_token.txt"
                nova_link_oauth = r"novalink_oauth_token.txt"
                automation_oauth = r"automation_oauth_token.txt"
            
            # Oauth Stuff
            novacensus_client_id = '80cf8a9d-cf23-4511-8bf3-6e9783241671'
            novacensus_secret_value = 'q7Y8Q~JkwQ4L4iBdpBJmzyb8M~mSE1ChPPdPPaWt'
            novalink_client_id = '3fcc1be8-f7aa-433b-87e0-ac6dd1297622'
            novalink_secret_value = '1Tb8Q~JH5QqLbI4vHApgdd6XMQt9PaPyLYhOVabD'
            automation_client_id = "fe12d3a6-9b60-4764-ab55-868fd4533247"
            automation_secret_value = "qtw8Q~HFdlP1RR4yES4e8paQOglCiieXHR8gvbOZ"
            
            
            # In[3]:
            
            
            # Email Language
            
            
            email_body_template = """Dear %Salutation%, 
            
            Your Novalink “TEST” census file reflecting year to date data has been posted to our web portal (below).  You may access the file by logging in and selecting the Plan Documents tab at the top: 
            <ol>
            <li>Login to PlanSponsorLink through https://nova401k.plansponsorlink.com/. 
            
            <li>Login Instructions: Your Username is your email address. First time users should click on the ‘First time user?' link and enter your email address. PlanSponsorLink will automatically confirm your email address and email you a password.  
            </ol>
            Please understand that in order for us to pull your year-end census data, we need you to review this file for accuracy. It is important to review all columns. If there are errors on the test file, there will be errors in the year-end census data which can cause delays in timely completion of your compliance testing. If any errors are noted, please notify us immediately. If we do not hear from you within 5 days, we will assume the file is accurate. 
            
            Thank you for utilizing Novalink! 
             
            Sincerely, 
            
            %Account_Manager_Signature%
            """
            
            
            # In[4]:
            
            
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
            
            
            # In[ ]:
            
            
            
            
            
            # In[5]:
            
            
            def email_test_send():
                message_object = novalink_account.mailbox().new_message()                  
                message_object.to.add(['andrewkim.38@gmail.com'])   
                message_object.cc.add(['akim@nova401k.com'])   
                message_object.subject = 'Novalink Test File – ACTION REQUIRED'
                #message_object.subject = f'Novalink Authentication for kelvins plan – Action Required'
                message_object.body = email_body
                message_object.attachments.add(NOVALINK_PDF_PATH)
                message_object.send()
            
            
                
            def move_file_with_increment(source, destination):
                """
                Requires shutil and os.
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
                
                
            class get_contacts:
                """
                The Swiss army knife MEGA CLASS of getting all the contact information related to a plan. 
                
                Designed so that the reference point of a plan is in a single point rather than making multiple accidental calls
                scattered throughout the script. 
                
                It is also a suitcase for some of my useful functions. 
                
            
                """
                
                def __init__(self,planid):
                    self.plan_info = pp.get_plan_by_planid(planid,expand='MultipleEmployerPlan,AdminType,PlanType,PlanGroup,InvestmentProviderLinks.InvestmentProvider,FilingStatus,Client.CompanyName,Client.Addresses')
                    self.employee_contacts = pp.get_employee_plan_roles_by_planid(planid, expand = 'Contact.ContactPhoneNumberLinks,RoleType')
                    self.contact_roles = pp.get_plan_contact_roles_by_planid(planid, expand = 'Contact.ContactPhoneNumberLinks, Contact.ContactPreference,RoleType')
                    self.client_info = pp.get_clients(filters = f'ClientId eq {self.plan_info["ClientId"]}', expand='CompanyName,Addresses.Address')
                    self.plan_group = self.plan_info['PlanGroup']['DisplayName']
                    self.plan_type = self.plan_info['PlanType']['DisplayName']
                    self.admin_type = self.plan_info['AdminType']['DisplayName']
                    self.form5500 = self.plan_info["FilingStatus"]["DisplayName"]
                    self.is_mep = bool(self.plan_info['IsMultipleEmployerPlan'] or self.plan_info['MultipleEmployerPlanId'])
            
                    # Dont automatically pick item 0 for each unique role in case there are multiple people under the same role. 
                    # If you're wondering why a whole-ass single dictionary is in a list, thats why. I just happens so it found only 1 person.
                    self.primary_contacts = [contact["Contact"] for contact in self.contact_roles if contact["RoleType"]["DisplayName"] == 'Primary Contact']
                    self.secondary_contacts = [contact["Contact"] for contact in self.contact_roles if contact["RoleType"]["DisplayName"] == 'Secondary Contacts']
                    self.brokers = [contact["Contact"] for contact in self.contact_roles if 'broker' in contact["RoleType"]["DisplayName"].lower()]
                    self.primary_brokers = [contact["Contact"] for contact in self.contact_roles if 'primary broker' in contact["RoleType"]["DisplayName"].lower()]
                    self.secondary_brokers = [contact["Contact"] for contact in self.contact_roles if 'secondary broker' in contact["RoleType"]["DisplayName"].lower()]
                    self.investment_providers = [i for i in self.plan_info["InvestmentProviderLinks"]]   # AKA record keeper. Only grab the Primary provider.
                    self.administrators = [contact["Contact"] for contact in self.employee_contacts if contact["RoleType"]["DisplayName"] == 'Administrator']
                    self.afs_administrators = [contact["Contact"] for contact in self.contact_roles if contact["RoleType"]["DisplayName"] == '3(16) Administrator']
                    self.novalink_payroll_superuser = [i["Contact"] for i in self.contact_roles if i["RoleType"]["DisplayName"] == 'Novalink Super User']
                    self.novalink_payroll_contact = [i["Contact"] for i in self.contact_roles if i["RoleType"]["DisplayName"] == 'Novalink Payroll Contact']
                    
                    # Dont automatically pick item 0 for each unique role in case there are multiple people under the same role. 
                    # If you're wondering why a whole-ass single dictionary is in a list, thats why. I just happens so it found only 1 person.
                    self.termination_specialist = [i["Contact"] for i in self.employee_contacts if "termination specialist" in str(i["RoleType"]["DisplayName"]).lower()]
                    self.termination_specialist_lead = [i["Contact"] for i in self.employee_contacts if "terminations team lead" in str(i["RoleType"]["DisplayName"]).lower()]
                    
                    # Use SET to eliminate duplicates. Its just my habit when collecting emails.
                    # When collecting contacts for multiple people, each unique individual can have multiple overlapping roles. 
                    self.termination_specialist_emails = list(set([i["Email"] for i in self.termination_specialist if i]))
                    self.termination_specialist_lead_emails = list(set([i["Email"] for i in self.termination_specialist_lead if i]))
                    
                    # In order for these roles to be added onto the Interactions tab, they need a ContactId.
                    # They already have this but labeled wrong. Add a ContactId key by copying the Id value. 
                    # An InteractionRoleId is also required, 16 is TO. 155 is FROM. Since automation is always  
                    # sending emails on the behalf of Administrators, its assumed that Administrators interaction 
                    # role is FROM unless explicitly told otherwise. 
                    if self.primary_contacts:
                        for i in self.primary_contacts:
                            i['ContactId'] = i['Id']
                            i['InteractionRoleId'] = 16
                    if self.secondary_contacts:
                        for i in self.secondary_contacts:
                            i['ContactId'] = i['Id']
                            i['InteractionRoleId'] = 16
                    if self.brokers:
                        for i in self.brokers:
                            i['ContactId'] = i['Id'] 
                            i['InteractionRoleId'] = 16
                    if self.administrators:
                        for i in self.administrators:
                            i['ContactId'] = i['Id']
                            i['InteractionRoleId'] = 155
                    if self.afs_administrators:
                        for i in self.afs_administrators:
                            i['ContactId'] = i['Id']
                            i['InteractionRoleId'] = 155
                            
                            
                @staticmethod
                def greetings(names):
                    """
                    Takes in a list of names and outputs them with commas.
                    Intended to be used to start a letter such as "Dear name1,name2, and name3,"
                    
                    Example: ["Adam","Ben","Charles"] --->  "Adam, Ben, and Charles"
                             ["Adam","Ben"] ---> "Adam and Ben"
                             ["Adam"] ---> "Adam"
                    
                    """
                    
                    # Get rid of falsies in name list. 
                    names = [x for x in names if x]
                    names = [x for x in names if x.strip()]
                    
                    names = list(set(names)) # Get rid of duplicate names. Idc if they're actually unique individuals. 
                    names.sort()
                                     
                    if len(names) == 1:
                        # If there is only one name, don't use 'and'
                        return f'{names[0]}'
                    else:
            
                        # Join the names with commas and an 'and'
                        names_str = ', '.join(names[:-1]) + f' and {names[-1]}'
            
                        return f'{names_str}'
                
                @staticmethod
                def format_phone_number(phone_number):
                    """
                    Deletes all non-numerical characters and reformats the number as ###-###-####
                    """
                    phone_number = re.sub(r'[^\d]', '', phone_number)
                    if len(phone_number) == 10 or len(phone_number) == 11:
                        if phone_number[0] == '1':
                            area_code = re.search(r'\d{3}', phone_number[1:]).group()
                            formatted_number = "{}-{}-{}".format(area_code, phone_number[4:7], phone_number[7:])
                        else:
                            area_code = re.search(r'\d{3}', phone_number).group()
                            formatted_number = "{}-{}-{}".format(area_code, phone_number[3:6], phone_number[6:])
                        return formatted_number
                    else:
                        return phone_number
                
                
                @staticmethod
                def get_salutations(contact_list):
                    """
                    Takes a list of contact dictionaries and grabs the Salutations key. If it doesn't exist, use
                    the first name. You may want to pass this onto the greetings() method in this class. 
                    
                    Example:
                    get_salutations(self.primary_contacts + self.secondary_contacts
                    
                    
                    """
                    names = []
                    for person in contact_list:
                        if person["Salutation"]:
                            names.append(person["Salutation"])
                        else:
                            names.append(person['FirstName'])     
                    return names
                
                
                # Use SET to eliminate duplicates. 
                # When collecting contacts for multiple people, each unique individual can have multiple overlapping roles.         
                def get_primary_secondary_contact_emails(self):
                    prim_sec_contacts = self.primary_contacts + self.secondary_contacts
                    prim_sec_emails = [i["Email"] for i in prim_sec_contacts]
                    prim_sec_emails = [i for i in prim_sec_emails if i]
                    return list(set(prim_sec_emails))
                
                
                def get_primary_secondary_broker_emails(self):
                    broker_emails = [i["Email"] for i in self.brokers]
                    broker_emails = [i for i in broker_emails if i]
                    return broker_emails
                
                
                def get_primary_address(self):
                    addresses = self.client_info["Values"][0]["Addresses"]
                    if len(addresses) == 0:
                        return False
                    elif len(addresses) == 1: # Even if a company only has 1 address, it doesn't necessarily mean it will be marked as Primary. So pick the only one cus what else can it be?
                        return addresses[0]["Address"]
                    elif len(addresses) > 1:
                        # Grab the Primary address. If there are multiple address but none are Primary, grab the first address.
                        primary_address = [i["Address"] for i in addresses if i['IsPrimary'] == True]
                        if primary_address:
                            return [i["Address"] for i in addresses if i['IsPrimary'] == True][0]
                        else:
                            return addresses[0]["Address"]
                        
                
                def get_admin_signature(self):
                    administrator_contact_list = self.administrators
                    administrator_signature = ""
                    
                    raw_sig = pp.get_employees(filters=f"ContactId eq {self.administrators[0]['Id']}")["Values"][0]["Signature"]
                    
                    if raw_sig:
                        for line in raw_sig.split("\r\n"):
                            administrator_signature= administrator_signature + line + '\n'
                    else:    
                        administrator_signature = f"{self.administrators[0]['FirstName']} {self.administrators[0]['LastName']} \n{self.administrators[0]['Email']}"
                    return administrator_signature
            
            
                def all_termination_contact_emails(self):
                    all_emails = []
                    if self.termination_specialist_emails:
                        all_emails = all_emails + self.termination_specialist_emails
                    if self.termination_specialist_lead_emails:
                        all_emails = all_emails + self.termination_specialist_lead_emails
                    return all_emails
                
            
                
            def add_email_to_note(project_dict, email):
                """
                I use pp.get_project_by_projectid() as a template for adding notes since I'm not entirely sure what the required 
                fields are. So thats what this is expecting. 
                
                Adds a html text body into a plan/project. It splits the text into a list. Each item has a 2000 text limit since thats the character
                limit of a note. Adds the note in reverse order so it can be read from top to bottom. 
                """
                
                # Make a separate copy since dictionaries are mutable. It will change global dictionary otherwise.
                project_dict = copy.deepcopy(project_dict)
                project_dict["ProjectId"] = project_dict["Id"]
                project_dict['NoteCategoryId'] = 2978
                
                # Notes only take increments of 2000 characters. Divide large emails into list. Reverse order. Upload in
                # reverse order so reader can read top to bottom. 
                email = [email[i:i+2000] for i in range(0,len(email),2000)]
                email.reverse()
                
                
                for i in email:
                    project_dict["NoteText"] = i
                    pp.add_note(project_dict)
                
                return 
            
            
            def dataframe_logger(dataframe, destination_path, trim_at = 1000):
                """
                Requires os and pandas. 
                
                Saves a dataframe as an excel file for error logging. If its found at the destination,
                it will concact onto it. It will trim the newly concatenated dataframe if there are more than 1000 rows.
                
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
                    
                    
            
            
            # Directory Listing Of the Stored Census Files as Dataframe.
            # 
            # Easy reference point when comparing the plan id's in the worktray to these file directories.
            # 
            # Michael Blizman's process is storing the census files into a single location. 
            # Get a directory listing of the xlsx files there and turn it into a dataframe.
            # The planid is already prepended onto the name. Use split("_") and 
            # grab the item [0] which is the plan id. Put it into its own column. 
            
            # In[6]:
            
            
            census_file_collection_df = pd.DataFrame({
                'file_path': [],
                'planid' : []
            })
            
            census_file_collection = glob(r"Y:\Automation\NovaLink\test_census\*test_census*.xlsx")
            census_file_collection = [Path(i) for i in census_file_collection]
            census_file_collection_df['file_path'] = census_file_collection
            census_file_collection_df['planid'] = census_file_collection_df['file_path'].apply(lambda x:Path(x).name.split('_')[0]) 
            census_file_collection_df['file_path'] = census_file_collection_df['file_path'].astype(str) #
            census_file_collection_df
            
            
            # In[7]:
            
            
            #add_project_file(FILEPATH, PROJECT_ID, ProjectFileTypeId=586, ShowOnWeb=True, Title="Novalink Test File", Comment='', Archived=False, HasBeenWarned=False)
            
            
            # In[7]:
            
            
            NOVALINK_WORKTRAY = pp.get_worktray("Novalink",get_all=True)
            filt1 = NOVALINK_WORKTRAY["task_name"] == "Test File Email"
            filt2 = NOVALINK_WORKTRAY["proj_name"].str.lower() == "novalink test file"
            filt3 = NOVALINK_WORKTRAY["planid"] != '99205'
            
            NOVALINK_WORKTRAY = NOVALINK_WORKTRAY[filt1 & filt2 & filt3]
            
            
            # In[8]:
            
            
            if len(NOVALINK_WORKTRAY) == 0:
                print("No worktray items.")
                raise SystemExit()
            
            
            # In[9]:
            
            
            
            for i in NOVALINK_WORKTRAY.index[:]:
                
                print("\n")
                PLANID = NOVALINK_WORKTRAY.at[i,'planid']
                TASKID = NOVALINK_WORKTRAY.at[i,'taskid']
                PROJECT_ID = int(NOVALINK_WORKTRAY.at[i,'projid'])
                PLAN_NAME = NOVALINK_WORKTRAY.at[i,'plan_name']
                print(f"Plan ID: {PLANID}")
                print(f"Plan Name: {PLAN_NAME}")
                
                
                # From the collection of census files, grab file path corresponding with Plan ID.
                if PLANID in census_file_collection_df['planid'].values:
                    FILEPATH = census_file_collection_df.loc[census_file_collection_df['planid'] == PLANID,'file_path'].iloc[0]
                else:
                    print(f"Plan ID {PLANID} is in the Novalink Worktray but its Census file could not be found. Skipping...")
                    continue
                
                          
                # Add file to project. 
                # If the file already exists, just move on. We can investigate later when the error gets printed to the log.           
                try:
                    pp.add_project_file(FILEPATH, PROJECT_ID, ProjectFileTypeId=586, ShowOnWeb=True, Title="Novalink Test File", Comment='YTD Novalink Test Census File', Archived=False, HasBeenWarned=False)
                    print("File added.")    
                except Exception as e:
                    error_message = str(e)
                    if "A file with this title was already found on this project" in error_message:
                        print(error_message)
                        continue
                    else:
                        raise Exception(error_message)
                          
                       
                
                # Send Emails                 
                plan_contacts = get_contacts(PLANID)
                super_user_emails = [i["Email"] for i in plan_contacts.novalink_payroll_superuser]
                if len(super_user_emails) == 0:
                    raise Exception(f"{PLANID} does not have a Novalink Payroll Superuser to email. Please add one for this plan")
                print(f"Found Emails for Super User: {(', ').join(super_user_emails)}")
                cc_emails = plan_contacts.novalink_payroll_contact + plan_contacts.primary_brokers
                cc_emails = [i["Email"] for i in cc_emails if i["Email"] and i["Email"] not in super_user_emails]
                if len(cc_emails):
                    print(f"Found Emails for CC: {(', ').join(cc_emails)}")
                          
                superuser_salutation_names = plan_contacts.get_salutations(plan_contacts.novalink_payroll_superuser) # Gather all "Salutations" keys
                super_users_salutations = plan_contacts.greetings(superuser_salutation_names) # Comma separate the names if more than 1. Get it ready to be pasted into email.
                account_manager_signature = plan_contacts.get_admin_signature()
                          
                email_body = email_body_template
                email_body = email_body.replace("%Salutation%",super_users_salutations)
                email_body = email_body.replace("%Account_Manager_Signature%",account_manager_signature)
                email_body = email_body.replace("\n","<br>")
                email_body = email_body.replace("&#10;","<br>")
                
                print(email_body)          
                
                
                message_object = novalink_account.mailbox().new_message()                  
                message_object.to.add(super_user_emails)          
                message_object.cc.add(cc_emails) 
                message_object.subject = f'Novalink Test File for {PLAN_NAME} – ACTION REQUIRED'
                message_object.body = email_body
                message_object.send()
                print("Email sent.") 
            
                # Add the sent email as a project note.
                note_template = pp.get_project_by_projectid(PROJECT_ID) # Get project. Use dictionary as a template for adding notes.
                add_email_to_note(note_template, email_body)
                
                pp.override_task(TASKID)
                print("Task Overridden")
                
                move_file_with_increment(FILEPATH, r"Y:\Automation\NovaLink\test_census\done")
            
            
            # In[11]:
            
            
            raise SystemExit("Done")
            
            
            # In[ ]:
            
            
            
            
            
            # In[ ]:
            
            
            plan_contacts.contact_roles
            
            
            # In[ ]:
            
            
            #FILEPATH = r"C:\Users\akim\Desktop\error log.txt"
            PROJECT_ID = 8029965
            try:
                pp.add_project_file(FILEPATH, PROJECT_ID, ProjectFileTypeId=586, ShowOnWeb=True, Title="Novalink Test File", Comment='YTD Novalink Test Census File', Archived=False, HasBeenWarned=False)
            except Exception as e:
                error_message = str(e)
                if "A file with this title was already found on this project" in error_message:
                    print(error_message)
            
                else:
                    raise Exception(error_message)
            
            
            # In[ ]:
            
            
            raise Exception("hello")
            
            
            # In[ ]:
            
            
            plan_contacts
            
            
            # In[ ]:
            
            
            plan_contacts = pp.get_plan_contact_roles_by_planid('99205')
            novalink_contacts = [i["Contact"] for i in plan_contacts if i["RoleType"]["DisplayName"] in ['Novalink Super User','Novalink Payroll Contact']]
            # if len(plan_super_users) == 0:
            #     print(f"Warning! No Novalink Super Users for plan id {PLANID}! Skipping.")
            
            novalink_contact_emails = [i["Email"] for i in novalink_contacts]
            novalink_contact_emails
            
            
            # In[ ]:
            
            
            plan_contacts = get_contacts('99205')
            
            
            # In[ ]:
            
            
            
            
            
            # In[ ]:
            
            
            super_user_emails = [i["Email"] for i in plan_contacts.novalink_payroll_superuser]
            cc_emails = plan_contacts.novalink_payroll_contact + plan_contacts.secondary_brokers
            cc_emails = [i["Email"] for i in cc_emails if i]
            print(super_user_emails)
            print()
            print(cc_emails)
            
            
            # In[ ]:
            
            
            plan_contacts.get_salutations(plan_contacts.novalink_payroll_superuser)
            
            plan_contacts.greetings(plan_contacts.get_salutations(plan_contacts.novalink_payroll_superuser))
            
            
            # In[ ]:
            
            
            [i["Email"] for i in (plan_contacts.novalink_payroll_contact + plan_contacts.primary_brokers)]
            
            
            # In[ ]:
            
            
            plan_contacts.get_admin_signature()
            
            
            # In[ ]:
            
            
            super_users_salutations = plan_contacts.get_salutations(plan_contacts.novalink_payroll_superuser)
            superuser_salutation_names = plan_contacts.get_salutations(plan_contacts.novalink_payroll_superuser) # Gather all "Salutations" keys
            super_users_salutations = plan_contacts.greetings(superuser_salutation_names) # Comma separate the names if more than 1. Get it ready to be pasted into email.
            
            email_body = email_body_template
            email_body = email_body.replace("%Salutation%",super_users_salutations)
            email_body = email_body.replace("%Account_Manager_Signature%",plan_contacts.get_admin_signature())
            email_body = email_body.replace("\n","<br>")
            email_body = email_body.replace("&#10;","<br>")
            
            message_object = novalink_account.mailbox().new_message()                  
            message_object.to.add(['andrewkim.38@gmail.com'])          
            #message_object.cc.add(['steelsmoker@sbcglobal.net','akim@nova401k.com']) 
            message_object.subject = 'Novalink Test File – ACTION REQUIRED'
            message_object.body = email_body
            message_object.send()
            

    schedule_logger('Finished', pyfile= __file__)

except SystemExit:
    schedule_logger('Finished', pyfile= __file__)
    raise
    
except Exception as e:
    email_error_to_automation(e, __file__)
    schedule_logger('Error', pyfile= __file__)

            