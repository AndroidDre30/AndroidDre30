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
            

            
            # In[2]:
            
            
            import sys
            sys.path.insert(0, "Y:\Automation\Team Scripts\Andrew Kim\my modules")
            import datetime
            import os
            import copy
            
            from pathlib import Path
            
            import pandas as pd
            from O365 import Account, FileSystemTokenBackend
            from O365.message import Message
            
            import pensionpro_v1 as pp
            #import pensionpro as pp
            
            from IPython.display import display, HTML
            pd.set_option('display.max_rows',None)
            pd.set_option('display.max_columns',None)
            
            
            # # Resources
            
            # In[3]:
            
            
            NOVALINK_PDF_PATH = r"Y:\Payroll Integration\FINCH\Communication to Clients\Novalink + Finch Brochure.pdf"
            if os.path.exists(NOVALINK_PDF_PATH) == False:
                raise Exception("Novalink PDF brochure is missing from path Y:\\Payroll Integration\\FINCH\\Communication to Clients")
            
            
            # # Oauth Stuff
            
            # In[4]:
            
            
            nova_link_oauth = r"C:\Users\Public\WPy64-39100\notebooks\scheduler\novalink_oauth_token.txt"
            novalink_client_id = '3fcc1be8-f7aa-433b-87e0-ac6dd1297622'
            novalink_secret_value = '1Tb8Q~JH5QqLbI4vHApgdd6XMQt9PaPyLYhOVabD'
            
            token_backend = FileSystemTokenBackend(token_filename=nova_link_oauth)
            novalink_account = Account((novalink_client_id,novalink_secret_value),
                              token_backend=token_backend,
                              scopes = ['basic','message_all','mailbox'],
                             )
            
            if not novalink_account.is_authenticated:
                novalink_account.authenticate()
            print(novalink_account.is_authenticated)
            
            
            # # Functions and my custom tools
            
            # In[5]:
            
            
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
                    self.primary_brokers = [contact["Contact"] for contact in self.contact_roles if contact["RoleType"]["DisplayName"] == 'Primary Broker']
                    self.brokers = [contact["Contact"] for contact in self.contact_roles if 'broker' in contact["RoleType"]["DisplayName"].lower()]
                    self.investment_providers = [i for i in self.plan_info["InvestmentProviderLinks"]]   # AKA record keeper. Only grab the Primary provider.
                    self.administrators = [contact["Contact"] for contact in self.employee_contacts if contact["RoleType"]["DisplayName"] == 'Administrator']
                    self.afs_administrators = [contact["Contact"] for contact in self.contact_roles if contact["RoleType"]["DisplayName"] == '3(16) Administrator']
                    
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
                    get_salutations(self.primary_contacts + self.secondary_contacts)
                    
                    
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
                
                
                def get_primary_broker_emails(self):
                    primary_broker_emails = [i["Email"] for i in self.primary_brokers]
                    primary_broker_emails = [i for i in primary_broker_emails if i]
                    return primary_broker_emails
                
                
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
                Requires copy and pensionpro
                
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
            
            
            # In[6]:
            
            
            # Email Body Template. Used in every iteration and the information is replaced. 
            
            email_body_template = """
            Dear %Salutation%, 
            
            Thank you for signing up for Novalink!  
            
            Before you proceed, you will need your payroll provider UserID and Password.  
            
            Once you have your payroll provider UserID and Password handy, click %hyperlink% to authorize Finch to connect with your payroll provider to access your payroll data.  
            
            If you have any questions on this process, please contact novalink@nova401k.com. 
            
            %Account_Manager_Signature%
            """
            
            
            # In[7]:
            
            
            def email_test_send():
                message_object = novalink_account.mailbox().new_message()                  
                message_object.to.add(['andrewkim.38@gmail.com'])   
                message_object.cc.add(['akim@nova401k.com'])   
                #message_object.subject = f'Novalink Authentication for {plan_name} – Action Required'
                message_object.subject = f'Novalink Authentication for kelvins plan – Action Required'
                message_object.body = email_body
                message_object.attachments.add(NOVALINK_PDF_PATH)
                message_object.send()  
            
            
            # In[8]:
            
            
            NOVALINK_WORKTRAY = pp.get_worktray("Novalink",get_all=True)
            NOVALINK_WORKTRAY = NOVALINK_WORKTRAY[(NOVALINK_WORKTRAY["task_name"] == "Authentication Email") & (NOVALINK_WORKTRAY["planid"] != '99205')]
            
            if len(NOVALINK_WORKTRAY) == 0:
                raise SystemExit("No worktray items.")
                
            NOVALINK_WORKTRAY['email_sent'] = ''
            NOVALINK_WORKTRAY['task_overridden'] = ''
            NOVALINK_WORKTRAY['runtime'] = datetime.datetime.now().strftime("%m/%d/%Y %H:%M:%S")
            NOVALINK_WORKTRAY
            
            
            # In[9]:
            
            
            for i in NOVALINK_WORKTRAY.index[:]:
                
                planid = NOVALINK_WORKTRAY.at[i,'planid']
                plan_name = NOVALINK_WORKTRAY.at[i,'plan_name']
                taskid = NOVALINK_WORKTRAY.at[i,'taskid']
                projectid = NOVALINK_WORKTRAY.at[i,'projid']
                print(f"Plan ID: {planid}\nPlan Name:{plan_name}")
            
                
                # Grab the url from the Services Provided tab. This link was generated by project 21853 and added to the plan. 
                plan_services_provided = pp.get_plan_services_provided_by_planid(planid)
                plan_services_provided = [i for i in plan_services_provided if i["ProvidedServiceId"] == 11037] # 11037 is the 'Novalink Opt-In' service.
                
                if plan_services_provided:
                    novalink_url = plan_services_provided[-1]["Description"]
                    hyperlink = f"<a href={novalink_url}>here</a>"
                else:
                    print(f"Could not find the Novalink Opt-In service for plan {planid}. Skipping...")
                    continue 
                
                
                # Gather plan contacts
                plan_contacts = get_contacts(planid)
                super_users = [i["Contact"] for i in plan_contacts.contact_roles if i["RoleType"]["DisplayName"] == "Novalink Super User"]
                if len(super_users) == 0:
                    print(f"No Super Users found for plan {planid}. Skipping...")
                    continue
                if len(plan_contacts.administrators) == 0:
                    print(f"No Administrators found for plan {planid}. Skipping...")
                    continue
                super_users_emails = [i["Contact"]["Email"] for i in plan_contacts.contact_roles if i["RoleType"]["DisplayName"] == "Novalink Super User"]
                super_users_salutations = plan_contacts.greetings(plan_contacts.get_salutations(super_users))
                regular_payroll_contacts_emails = [i["Contact"]["Email"] for i in plan_contacts.contact_roles if i["RoleType"]["DisplayName"] == "Novalink Payroll Contact"]
                primary_broker_emails = plan_contacts.get_primary_broker_emails()
                account_manager_signature = plan_contacts.get_admin_signature()
                to_emails = super_users_emails
                cc_emails = regular_payroll_contacts_emails + primary_broker_emails
                cc_emails = [i for i in cc_emails if i not in to_emails]
                
                
                
                
                # Prep the email template
                email_body = email_body_template
                email_body = email_body.replace("%Salutation%",super_users_salutations)
                email_body = email_body.replace("%hyperlink%",hyperlink)
                email_body = email_body.replace("%Account_Manager_Signature%",account_manager_signature)
                email_body = email_body.replace("\n","<br>")
                email_body = email_body.replace("&#10;","<br>")
            
                
                
                # Send Email
                try:
                    message_object = novalink_account.mailbox().new_message()                  
                    message_object.to.add(to_emails)          
                    message_object.cc.add(cc_emails) 
                    message_object.subject = f'Novalink Authentication for {plan_name} – Action Required'
                    message_object.body = email_body
                    message_object.attachments.add(NOVALINK_PDF_PATH)
                    message_object.send()
                    NOVALINK_WORKTRAY.at[i,'email_sent'] = True
            
                except:
                    print(f"Could not send email for {planid}.")
                    continue
                    
                # Get project info dictionary and use it as a template for adding notes. 
                note_template = pp.get_project_by_projectid(projectid)
                add_email_to_note(note_template, email_body)
                
                
                
                # Override task
                try:
                    print("Overriding task")
                    pp.override_task(taskid)
                    NOVALINK_WORKTRAY.at[i,'task_overridden'] = True
                except:
                    print('Failed overriding.')
                    
                print("Task Overridden")
                print("\n")
            
            
            # In[10]:
            
            
            NOVALINK_WORKTRAY
            
            
            # In[11]:
            
            
            dataframe_logger(NOVALINK_WORKTRAY, '22081.xlsx', trim_at = 1000)
            raise SystemExit("Done")
            
            
            # # Troubleshoot
            
            # In[ ]:
            
            
            # Load up the saved excel sheet as dataframe
            troubleshoot_df = dataframe_loader('22081.xlsx', 'runtime', start_date = '1/1/1900', end_date = datetime.datetime.now())
            
            
            # In[ ]:
            
            
            
            

    schedule_logger('Finished', pyfile= __file__)

except SystemExit:
    schedule_logger('Finished', pyfile= __file__)
    raise
    
except Exception as e:
    email_error_to_automation(e, __file__)
    schedule_logger('Error', pyfile= __file__)

            