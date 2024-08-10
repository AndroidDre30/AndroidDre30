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
            

            
            # In[15]:
            
            
            #target_year = '2021'  # <------------- Change this value to a different year if other years need to be checked. This parameter determines which period start year to look in DGEM
            
            
            # In[16]:
            
            
            # This script is ready for straight run through
            
            from bs4 import BeautifulSoup
            from datetime import datetime as dt2
            import email
            from email.header import decode_header
            from email.mime.text import MIMEText
            from email.mime.multipart import MIMEMultipart
            from O365 import Account, FileSystemTokenBackend
            
            import datetime
            from pathlib import Path
            import imaplib
            from IPython.core.display import display, HTML
            import os
            import pandas as pd
            import pickle
            import re
            import requests
            from selenium import webdriver
            from selenium.webdriver.chrome.options import Options
            from selenium.webdriver.common.by import By
            from selenium.common.exceptions import NoSuchElementException
            from selenium.webdriver.support.ui import WebDriverWait
            from selenium.webdriver.support import expected_conditions as ec
            from webdriver_manager.chrome import ChromeDriverManager
            import sys
            from tqdm import tqdm
            from pathlib import Path
            import smtplib
            import time
            
            #sys.path.insert(0,'U:/Vault')
            sys.path.insert(0, r'C:\Users\Public\WPy64-39100\notebooks\Lam Hoang')
            
            import lam
            from public_vault import Username, Password, OAuth
            import pensionpro_lam as pp
            import pensionpro_afs as app
            from expiration import expired, ExpirationException
            
            display(HTML('<style>.container{width:100%;}</style>'))
            
            pd.set_option('display.max_columns',None)
            pd.set_option('display.max_rows',None)
            
            all_plans_file_nova = r"Y:\Automation\Pickle Files\nova_all_plans_dict.pkl"
            all_plans_file_afs = r"Y:\Automation\Pickle Files\afs_all_plans_dict.pkl"
            
            project_folder = r'Y:\Automation\Projects\Active\5500 Delivery Task (Ticket# 15897)'
            
            os.chdir(project_folder)
            
            o365_directory = Path(r'C:\Users\Public\WPy64-39100\notebooks\scheduler')
            
            today = dt2.today().strftime('%m/%d/%Y')
            today
            
            
            # In[17]:
            
            
            """
            The previous author duplicated this script 3 times to account for the year 2021, 2022, and 2023. As far as I can tell, they are the same exact script except the target year.
            Im tired of modifying all 3 scripts when something breaks. From now on, a command line argument should be passed into this script containing the target year. 
            I'll keep the name of this script as-is for now. 
            -Andrew 4/1/24
            """
            
            
            # ![image.png](attachment:image.png)
            
            # In[18]:
            
            
            # Use system arguments to determine the target year.
            
            if Path(sys.argv[0]).name == 'ipykernel_launcher.py': # If ran on jupyter notebook...
                print(f"Running on Jupyter Notebook. Defaulting to {datetime.datetime.now().year - 1}")
                target_year = str(datetime.datetime.now().year - 1)
                
            elif len(sys.argv) < 2:
                raise Exception("The target plan year should be passed into this script as an argument but none was provided.")
                
            else:
                target_year = sys.argv[1]
                print(f"Target year is {target_year}.")
            
            
            # In[19]:
            
            
            # Use system arguments to determine which indexes of the target worktray to work on.
            # In some cases, the worktray can be huge (+700) and validating one by one will take more than a day.
            # This way, you can run multiple instances of this script at once to process for one year by delegating chunks
            # to each script. - Andrew 6/28/24
            
            if Path(sys.argv[0]).name == 'ipykernel_launcher.py': # If ran on jupyter notebook, go through entire worktray.
                sys_argument_starting_index = 0
                sys_argument_ending_index = 99999999999999999
                
            elif len(sys.argv) < 3: # If target year was provided but nothing else, go through entire worktray.
                sys_argument_starting_index = 0
                sys_argument_ending_index = 99999999999999999
                
            else:
                sys_argument_starting_index = int(sys.argv[2])
                sys_argument_ending_index = int(sys.argv[3])
                print(f"Starting Index: {sys_argument_starting_index}")
                print(f"Ending Index: {sys_argument_ending_index}")
            
            
            # In[20]:
            
            
            #ChromeDriverManager().install()
            
            
            # In[21]:
            
            
            #print('C:\\Users\\akim\\.wdm\\drivers\\chromedriver\\win64\\122.0.6261.129\\chromedriver-win32/chromedriver.exe')
            
            
            # In[22]:
            
            
            # import webbrowser
            # from pathlib import Path
            # import os, pandas as pd
            
            # webbrowser.open(path := Path(r'Y:\Automation\Projects\Active\5500 Delivery Task (Ticket# 15897)'))
            
            # dfs = list()
            
            # for i in path.glob('*.pkl'):
            #     print(i.name)
            #     dfs.append(pd.read_pickle(i))
            
            
            # In[23]:
            
            
            def send_email_to_am(administrator_email, plan_id, plan_name, email_username, email_pw):
                
                with smtplib.SMTP("smtp-mail.outlook.com", 587) as server:
                    server.ehlo()
                    server.starttls()
                    server.login(email_username, email_pw)
            
                    message = MIMEMultipart("alternative")
                    message["Subject"] = f"[AUTOMATED] ACTION REQUIRED for Plan {plan_id} {plan_name} - Automation could not deliver the form 5500 for you!"
                    message["From"] = email_username
                    message["To"] = administrator_email     
                    # removed consultant_email cc on 06-06-23
            
                    # Create the plain-text and HTML version of your message
                    text = f"""Hello,
            
            The automation program attempted to complete the form 5500 delivery task for plan {plan_id} {plan_name}, but could not.  Please log into DGEM and check the form 5500 to ensure that the 5500 has been validated and there are no errors.  Once you have completed your review within DGEM and corrected what was needed, the automation program will attempt to complete the delivery task the following business day.
            
            Please contact your team leader or assistant team leader if you have any question regarding this email. 
            
            Thank you
            
            """
                    # Turn these into plain/html MIMEText objects
                    part1 = MIMEText(text, "plain")
            
            
                    # Add HTML/plain-text parts to MIMEMultipart message
                    # The email client will try to render the LAST PART FIRST
                    message.attach(part1)
            
                    server.sendmail(email_username, [administrator_email], message.as_string())
            
                    print('\tEmail Sent')
            
            def email_dgem_failures(year,failure_list,recipients):
                """
                Added by Andrew on 8/15/23. Dawn asked us to run these delivery tasks hourly. The likelihood of a plan not being found in DGEM will increase
                and I dont have time to troubleshoot that. When an html table cant be found, it will continue instead and the plan name/id will be appended to a failure_list.
                Its set so that Jason is the only recipient for now. 
                
                When an HTML table isn't found, this can be for a variety of reasons... A plan couldn't be found using an EIN search...or there is a disparity in the spelling name 
                between Pension Pro and the DGEM system. I'll add other reasons as I find them. 
                
                """
                the_message = f"The following plans could not be found in DGEM during the {year} 5500 Delivery Process. This can be due to multiple hits on a EIN search or no results on DGEM due to a spelling disparity between Pension Pro and DGEM.\n"
                for i in failure_list:
                    the_message += i
                the_message = the_message.replace('\n', '<br>')
                message_object = account.mailbox().new_message()
                message_object.to.add(recipients)
                message_object.subject = f'Failures : 5500 Delivery Task for {year}'
                message_object.body = the_message
                message_object.send()
            
            
            # In[24]:
            
            
            def send_custom_email_to_am(administrator_email, plan_id, plan_name, message_text):
                
                with smtplib.SMTP("smtp-mail.outlook.com", 587) as server:
                    server.ehlo()
                    server.starttls()
                    server.login(email_username, email_pw)
            
                    message = MIMEMultipart("alternative")
                    message["Subject"] = f"[AUTOMATED] ACTION REQUIRED for Plan {plan_id} {plan_name} - Automation could not deliver the form 5500 for you!"
                    message["From"] = email_username
                    message["To"] = administrator_email     
                    # removed consultant_email cc on 06-06-23
            
                    # Create the plain-text and HTML version of your message
                    text = message_text
            
                    # Turn these into plain/html MIMEText objects
                    part1 = MIMEText(text, "plain")
            
            
                    # Add HTML/plain-text parts to MIMEMultipart message
                    # The email client will try to render the LAST PART FIRST
                    message.attach(part1)
            
                    server.sendmail(email_username, [administrator_email], message.as_string())
            
                    print('\tEmail Sent')
            
            
            # # DGEM Pages as Page Object Models
            
            # In[25]:
            
            
            class DGEM_Login_Page:
                
                #=================================================================
                # If the site changed their HTML element names, change here.
                def __init__(self, browser):
                    self.browser = browser
                    
                    self.company_key_field = (By.NAME, 'tbTPA')
                    self.user_name_field = (By.NAME, 'tbUsername')
                    self.password_field = (By.NAME,'tbPassword')
                    self.login_button = (By.NAME,'btnLogin')
                #=================================================================
                
                
                def navigate(self):
                    self.browser.get('https://dgem.asc-net.com/ascidoc/login.aspx')
                    self.browser.implicitly_wait(10)
                    
                def input_login(self, company_key, user_name, password):
                    self.browser.find_element(*self.company_key_field).clear()
                    self.browser.find_element(*self.company_key_field).send_keys(company_key)
                    self.browser.find_element(*self.user_name_field).clear()
                    self.browser.find_element(*self.user_name_field).send_keys(user_name)
                    
                    WebDriverWait(self.browser,10).until(ec.visibility_of_element_located(self.password_field))
                    self.browser.find_element(*self.password_field).clear()
                    self.browser.find_element(*self.password_field).send_keys(password)
                    
                def click_login_button(self):
                    self.browser.find_element(*self.login_button).click()
                    
            
                    
                    
            class DGEM_Verification_Pages:
                
                #=================================================================
                # If the site changed their HTML element names, change here.
                def __init__(self, browser):
                    self.browser = browser
                    
                    # Attributes from page 1
                    self.send_email_option = (By.XPATH, '//*[@id="MFAUC_rbEmail"]')
                    self.send_button = (By.XPATH, '//*[@id="MFAUC_btnSend"]')
                    
                    # Attributes from page 2
                    self.verification_code_box = (By.XPATH,'//*[@id="MFAUC_tbCode"]') # This is where you type the verification code after getting it from the email.
                    self.verify_button = (By.XPATH,'//*[@id="MFAUC_btnVerify"]')
                    self.error_message_field1 = (By.ID,'MFAUC_lblErrorMessage1')
                    self.error_message_field2 = (By.ID,'MFAUC_lblErrorMessage2')
                    
                    # Attributes from page 3
                    self.yes_radio_button = (By.XPATH,'//*[@id="MFAUC_rbYes"]')
                    self.continue_button = (By.XPATH,'//*[@id="MFAUC_btnContinue"]')
                #=================================================================
                    
                    
                    
                # Page 1 actions ========================================================================================    
                def send_email_verification(self):
                    WebDriverWait(self.browser,20).until(ec.visibility_of_element_located(self.send_email_option))
                    self.browser.find_element(*self.send_email_option).click()
                    
                def click_send_button(self):
                    self.browser.find_element(*self.send_button).click()
                
                
                
                # Page 2 actions ========================================================================================
                # I decided to do OAUTH stuff outside this class. I think including that stuff goes too far beyond the scope of this class. 
                # I only put the actions here.
                def input_verification_code(self, verification_code):
                    verification_code = str(verification_code)
                    self.browser.find_element(*self.verification_code_box).clear() # In case something is there from a previous error.
                    self.browser.find_element(*self.verification_code_box).send_keys(verification_code)
                    
                def click_verify(self):
                    self.browser.find_element(*self.verify_button).click()
                    WebDriverWait(self.browser,2)
                    try:
                        # If the inputted verification code doesn't work, these messages will appear.
                        # If the error element can't be found, it will except which means everything is ok. 
                        possible_errors = ['Enter a valid code','The verification code did not match.','An error occurred, please contact support.']
                        if browser.find_element(*self.error_message_field2).text in possible_errors:
                            print(browser.find_element(*self.error_message_field).text)
                            return False
                    except:
                        return True
                        
                    
                    
                # Page 3 actions ========================================================================================
                def click_yes(self):
                    WebDriverWait(self.browser,5).until(ec.visibility_of_element_located(self.yes_radio_button))
                    self.browser.find_element(*self.yes_radio_button).click()
                
                def click_continue(self):
                    self.browser.find_element(*self.continue_button).click()
            
                    
                    
            class DGEM_Find_Documents_Page:
                
                #=================================================================
                # If the site changed their HTML element names, change here.
                def __init__(self, browser):
                    self.browser = browser
                    self.status_field = (By.NAME, 'dd5500Status')
                    self.year_field = (By.XPATH, '//*[@id="dd5500Year"]')
                    self.validated_field = (By.XPATH, '//*[@id="ddValidated"]')
                    self.next_button = (By.ID, 'lbtnNext_5500')
                #=================================================================
                
                    
                def navigate(self):
                    self.browser.get('https://dgem.asc-net.com/ascidoc/Find.aspx')
                    self.browser.implicitly_wait(10)
                    
                def fill_out_5500_documents_form(self, year, validated_status):
                    
                    # Select Draft
                    self.browser.find_element(*self.status_field).click()
                    self.browser.find_element(*self.status_field).send_keys('Draft')
                    self.browser.find_element(*self.status_field).click()
                    
                    # Select Year
                    self.browser.find_element(*self.year_field).click()
                    self.browser.find_element(*self.year_field).send_keys(str(year))
                    self.browser.find_element(*self.year_field).click()        
                    
                    # Select 'Validated' or 'Not validated'
                    self.browser.find_element(*self.validated_field).click()
                    self.browser.find_element(*self.validated_field).send_keys(validated_status)
                    self.browser.find_element(*self.validated_field).click()
                    
                def click_next_button(self):
                    self.browser.find_element(*self.next_button).click()        
                    
                    
                    
                    
            class DGEM_Search_Results_Page:
                """
                Requires import from bs4 as BeautifulSoup()
                
                """
                def __init__(self, browser):
                    self.browser = browser
                    self.filter_box = (By.ID,'tbFilter5500')
                    self.identifier_column_1st_item = (By.CSS_SELECTOR, '#dg5500SearchResults > tbody > tr.visibleRow > td:nth-child(13)') # The value of the first box in the Identifier column
                    self.check_box = (By.CSS_SELECTOR, '#dg5500SearchResults > tbody > tr.visibleRow > td > input') # The first check box on the left hand side.
                    self.dropdown_menu = (By.XPATH, '//*[@id="lbAction5500"]')
                    self.next_button = (By.XPATH,'//*[@id="lbtnClientNext5500"]')
                    self.yellow_download_label = (By.ID, 'lblMessage') # The resulting message after clicking on Pre-Validate for non-validated plans.
                    self.yellow_successful_invite_label = (By.ID, 'lblInviteSignersMessage')
                    self.soup = None
                    self.soup_df = None
                    
                    
                def filter_box_input(self,input_value):
                    self.browser.find_element(*self.filter_box).clear()
                    self.browser.find_element(*self.filter_box).send_keys(input_value)
            
                def select_from_dropdown(self,input_value):
                    """
                    Select from the drop down menu towards the bottom right. 
                    Mostly for simple drop-down actions that dont require anything more than a click. 
                    Most other drop-down selecting actions will be given their own methods due to
                    various other requirements.
                    """
                    input_value = str(input_value)
                    self.browser.find_element(*self.dropdown_menu).send_keys(input_value)
                       
                def update_soup(self):
                    """
                    Get a dataframe representation of the items on the page. 
                    """
                    self.soup = BeautifulSoup(self.browser.page_source,'lxml')
                    self.soup_df = pd.read_html(str(self.soup.select('#dg5500SearchResults')[0]))[0]
                
                def check_soup_for_one_result(self, input_value):
                    """
                    Used to check the search result for only one plan. If a specific EIN or a plan id was 
                    entered on DGEM and more than one result appears, theres really no way for the code to "know" which is correct.
                    you typically can't proceed and the code should skip the current iteration.
                    
                    'input_value' is the plan-related value to check for in the search results. Usually the plan id.
                    """
                    
                    self.update_soup()
                    
                    if len(self.soup_df) != 1:
                        print(f'Resulting filter has {len(self.soup_df)} results when it should be 1 for the plan identifier {input_value}')
                        return False
                    
                    if str(self.soup_df.Identifier.at[0]) != input_value:
                        print(f"The resulting page didn't contain the plan identifier")
                        return False
                    
            
            
                    
                    return True # Yes, there was only 1 result.
                    
                def create_sar_attachment(self,plan_id):
                    """
                    Be sure to update your soup attribute first. (Done with check_soup_for_one_result())
                    Inputting a plan id is mostly to double check if the plan id exists in the soup attribute. 
                    """
                    plan_id = str(plan_id)
                    # If the resulting filter page is messed up, dont proceed.
                    if self.check_soup_for_one_result(plan_id) == False:
                        return 1
                        
                        
                    self.browser.find_element(*self.check_box).click()
                    self.browser.find_element(*self.dropdown_menu).send_keys('Create SAR Attachment (5500VS Batch)')
                    self.browser.find_element(*self.next_button).click()
                    self.browser.implicitly_wait(5)
                    
            
                    if self.browser.find_element(*self.yellow_download_label).text == 'click here to download log file.':
                        print('\tSAR attachment created')
                        return 0
                    else:
                        print('Failed to create SAR attachment')
                        return 2
                
                def invite_signers(self, plan_id):
                    """
                    Be sure to update your soup attribute first.
                    Inputting a plan id is mostly to double check if the plan id exists in the soup attribute. 
                    """
                    self.update_soup()
                    plan_id = str(plan_id)
                    # If the resulting filter page is messed up, dont proceed.
                    if self.check_soup_for_one_result(plan_id) == False:
                        return 1
                    
                    
                    self.browser.find_element(*self.check_box).click()
                    self.browser.find_element(*self.dropdown_menu).send_keys("Invite Signers (Batch)")
                    self.browser.find_element(*self.next_button).click()
                    self.browser.implicitly_wait(5)
            
            
                    if 'Invited Signers for:' in self.browser.find_element(*self.yellow_successful_invite_label).text:
                        print('\tSigners invited successfully')
                        return 0
                    else:
                        print('Failed to invite signer')
                        return 2
                    
                    
                def pre_validate_5500vs_batch(self, plan_id):
                    """
                    Be sure to update your soup attribute first.
                    Inputting a plan id is mostly to double check if the plan id exists in the soup attribute. 
                    """
                    self.update_soup()
                    plan_id = str(plan_id)
                    # If the resulting filter page is messed up, dont proceed.
                    if self.check_soup_for_one_result(plan_id) == False:
                        return 1
                    
                    
                    self.browser.find_element(*self.check_box).click()
                    self.browser.find_element(*self.dropdown_menu).send_keys("Pre-validate (5500VS Batch)")
                    self.browser.find_element(*self.next_button).click()
                    self.browser.implicitly_wait(5)
            
                    
                    resulting_message = self.browser.find_element(*self.yellow_download_label).text
                    
                    if 'Pre-Validate completed.' in resulting_message:
                        print('Pre-Validate completed.')
                        return 0
                    else:
                        print(f'Failed to Pre-Validate. \n{resulting_message}')
                        return 2  
            
            
            # In[ ]:
            
            
            
            
            
            # In[26]:
            
            
            def filter_for(df=None):
                '''Filter for only those projects that are either ("DC Annual Governmental Forms - Audit"
            or "DC Annual Governmental Forms - Small Filer"
            or "DC Annual Governmental Forms - Small Filer (Automated)"
            or "Defined Benefit Annual Government Forms - 5500-EZ, One-Man Filer"
            or "Defined Benefit Annual Government Forms - 5500-SF")
            with task name "Filing Invitation"
            
            Added 'Amended Annual Governmental Forms' and 'DC Annual Governmental Forms - 5500-EZ, Owner Only Filer (Automated)'
            at the request of Jason. Ticket 20853. -Andrew
            
            'DC Annual Governmental Forms - Schedule I' added by Andrew per Jeremiah on 9/14/23.
            '''
            
                df = df.loc[((df.proj_name == 'DC Annual Governmental Forms - Audit')
                              | (df.proj_name == 'DC Annual Governmental Forms - Small Filer')
                              | (df.proj_name == 'DC Annual Governmental Forms - Small Filer (Automated)')
                              | (df.proj_name == 'Defined Benefit Annual Government Forms - 5500-EZ, One-Man Filer')
                              | (df.proj_name == 'Defined Benefit Annual Government Forms - 5500-SF')
                              | (df.proj_name == 'Amended Annual Governmental Forms')
                              | (df.proj_name == 'DC Annual Governmental Forms - 5500-EZ, Owner Only Filer (Automated)')
                              | (df.proj_name == 'DC Annual Governmental Forms - Schedule I')
                              ) & (df.task_name == 'Filing Invitation')].reset_index(drop=True)
                return df
            
            
            # In[58]:
            
            
            df = pp.get_worktray('5500 on HOLD')[['projid', 'taskid', 'task_name', 'planid', 'plan_name', 'proj_name','period_start','period_end']]
            df = filter_for(df)
            
            df['period_start_year'] = df['period_start'].str.split('/').str[-1]
            df = df.sort_values('period_start_year').reset_index(drop=True)
            df = df.loc[df.period_start_year.astype(int) > 2020].reset_index(drop=True)
            df
            
            
            # In[28]:
            
            
            #-------------------------New Users Update this section----------------------
            
            chrome_driver = r"Y:\Automation\Chromedriver\chromedriver.exe"
            
            
            
            my_username = Username('DGEM')
            my_pw = Password('DGEM')
            
            email_username = Username('outlook')
            email_pw = Password('outlook')
            
            
            # email_username = "fe12d3a6-9b60-4764-ab55-868fd4533247" # Client ID
            # email_pw = "qtw8Q~HFdlP1RR4yES4e8paQOglCiieXHR8gvbOZ" #Client secret value
            #-----------------------------------------------------------------------------
            
            
            # In[49]:
            
            
            #browser = webdriver.Chrome(chrome_driver)
            options = Options()
            browser = webdriver.Chrome(executable_path = ChromeDriverManager().install(), options=options)
            browser.delete_all_cookies()
            
            
            # In[51]:
            
            
            
            # Login DGEM
            DGEM_login_page = DGEM_Login_Page(browser)
            
            DGEM_login_page.navigate()
            DGEM_login_page.input_login('asc438', my_username, my_pw)
            DGEM_login_page.click_login_button()
            
            
            # In[31]:
            
            
            credentials = OAuth('o365_client_id'), OAuth('o365_secret_value')
            
            token_backend = FileSystemTokenBackend(token_path=o365_directory,
                                                   token_filename='oauth_token.txt')
            account = Account(credentials,
                              token_backend=token_backend,
                              scopes = ['basic','message_all','mailbox'])
            if not account.is_authenticated:
                account.authenticate()
            
            
            DGEM_verification_pages = DGEM_Verification_Pages(browser)
            
            # Previously, DGEM was not able to remember this device even though we explicitly selected that option. Thats why we ran through the 2 factor authentication process
            # every time. Now its remembering our device and its immediately taking us to the homepage. Do a simple verification. 
            if DGEM_verification_pages.browser.current_url != 'https://dgem.asc-net.com/ascidoc/ASCI_Homepage.aspx':
                
                DGEM_verification_pages.send_email_verification()
                DGEM_verification_pages.click_send_button()
            
            
                time.sleep(60) # Wait for Email to arrive. 
            
            
                mailbox = account.mailbox(resource='automation@nova401k.com')
            
                inbox = mailbox.inbox_folder().get_messages()
            
                for message in inbox:
                    msg_sender = str(message.sender)
                    if (msg_sender == 'ASC (no-reply@pension-plan-emails.com)'
                        and (msg_subject := message.subject) == 'Verification Code'):
                        msg_body = message.body
                        regex = re.search(r'(?<=>)(?P<code>\d{6})(?=<)',msg_body) # find 6-digit num if between < and >
                        verification_code = regex.group('code')
                        message.mark_as_read()
                        break
            
            
            
                # Enter Verification code.
                DGEM_verification_pages.input_verification_code(verification_code)
                verify_results = DGEM_verification_pages.click_verify() # This will return false if an error message is received after clicking on verify. 
            
            
                # ERROR CHECKING PORTION FOR 2-FACTOR AUTHENTICATION
            
                # verify_results will return False if an error message is found. Run through the login procedure again and wait for verification code. 
                # If it screws up again, raise an exception
                if verify_results == False:
                    DGEM_login_page.navigate()
                    DGEM_login_page.input_login('asc438', my_username, my_pw)
                    DGEM_login_page.click_login_button()
            
                    DGEM_verification_pages.send_email_verification()
                    DGEM_verification_pages.click_send_button()
            
                    time.sleep(60)
                    # Calling for inbox_folder() should refresh this mailbox object.
                    inbox = mailbox.inbox_folder().get_messages()
            
            
                    for message in inbox:
                        msg_sender = str(message.sender)
                        if (msg_sender == 'ASC (no-reply@pension-plan-emails.com)'
                            and (msg_subject := message.subject) == 'Verification Code'):
                            msg_body = message.body
                            regex = re.search(r'(?<=>)(?P<code>\d{6})(?=<)',msg_body) # find 6-digit num if between < and >
                            verification_code = regex.group('code')
                            message.mark_as_read()
                            break
            
                    DGEM_verification_pages.input_verification_code(verification_code)        
                    verify_results = DGEM_verification_pages.click_verify()
                    if verify_results == False:
                        raise ValueError("The site did not accept the 2-factor authentication code!")
                else:
                    print("Verification code accepted.")
            
            
                # Register device an continue. I dont see the point since we wipe out cookies anyways...
                DGEM_verification_pages.click_yes()
                DGEM_verification_pages.click_continue()    
            
            
            # In[ ]:
            
            
            
            
            
            # In[32]:
            
            
            # ppt_and_top_heavy_template_id = pp.get_project_template_by_name('PPT and Top Heavy Review')[0]['Id']
            
            target_years = list(set(df.period_start_year))
            target_years.sort()
            
            
            if len(target_years) > 1:
                print('NOTE: There are more than one target years so check the other target years later')
            
                
            # The below script looks like it's meant for a loop but it's not worked out yet (not neeeded) since we've been only getting plans with period end of 2021
            # The script still works though so it's ok to leave all the indentations
            
            # Download filed records of all plans with period end of target year and the year before
            print('Refreshing df worktray')
            df = pp.get_worktray('5500 on HOLD')[['projid', 'taskid', 'task_name', 'planid', 'plan_name', 'proj_name','period_start','period_end']]
            
            
            df = filter_for(df)
            
            df['period_start_year'] = df['period_start'].str.split('/').str[-1]
            
            df = df.sort_values('period_start_year').reset_index(drop=True)
            df = df.loc[df.period_start_year.astype(int) > 2020].reset_index(drop=True)
            print(target_years)
            print(target_year)
            
            
            # In[ ]:
            
            
            
            
            
            # In[52]:
            
            
            # Find all validated plans first. 
            # Create a master list of all the plans available on the site which should also have their EIN's listed.
            # Merge it with your worktray df. If a plan existed in the worktray df but not the DGEM validated df, it means it wasn't validated.
            
            DGEM_find_documents_page = DGEM_Find_Documents_Page(browser) # Create Page Class
            DGEM_find_documents_page.navigate()
            
            DGEM_find_documents_page.fill_out_5500_documents_form(target_year, 'Validated') # Fill out the target  year and choose "Validated"
            time.sleep(1)
            DGEM_find_documents_page.click_next_button()
            
            
            # In[ ]:
            
            
            
            
            
            # In[34]:
            
            
            df
            
            
            # In[35]:
            
            
            # Get all the files listed and match the plan with the EIN. It will be easier to search for the plan by EIN
            soup = BeautifulSoup(browser.page_source,'lxml')
            table_master = pd.read_html(str(soup.select('#dg5500SearchResults')[0]))[0]
            table_master.rename(columns={'Identifier':'planid'},inplace=True)
            #table_master.fillna('', inplace = True)
            table_master['EIN'] = table_master['EIN'].astype(str)
            table_master['planid'] = table_master['planid'].astype(str)
            
            # take out ".0" in case there was a nan in the list of plan ID's
            for j in table_master.index:
                table_plan_id = table_master.planid.at[j]
                if '.0' in table_plan_id:
                    table_master.planid.at[j] = table_plan_id.replace('.0','')
            
            df = pd.merge(df,table_master[['planid','EIN']],on='planid', how='left')
            df.fillna('', inplace=True)
            
            
            # In[36]:
            
            
            dft = df.loc[df.period_start_year == target_year]
            
            
            # In[37]:
            
            
            df
            
            
            # In[38]:
            
            
            dft
            
            
            # In[39]:
            
            
            # Plans without EINs. That means that the form 5500 has not been validated. The account manager and team lead will be emailed for these plans
            
            # New requirements from Jason on ticket 20853. Comment on 8/25/23. Force validate these plans by selecting "Pre-validate (5500VS Batch)"
            
            unvalidated_plans_df = df.loc[(df.EIN == '') & (df["period_start_year"]==target_year) & (df.index >= sys_argument_starting_index) & (df.index <= sys_argument_ending_index)].copy(deep=True)
            
            unvalidated_plans_df.reset_index(inplace=True,drop=True)
            unvalidated_plans_df
            
            
            # In[40]:
            
            
            # Get a dataframe of all the plans that were emailed out on the last run. Use this to see which AM is going to be emailed again.
            pickle_file = 'df_emails_sent_last.pkl'
            pd.read_pickle(pickle_file)
            
            
            # In[41]:
            
            
            plans_to_skip = ['5995']   # <--------------- add plans to skip (string) if desired
            # 12390 doesn't exist.
            
            if not expired(expires_on='7-5-23'):
                
                #### Temporarily Ignoring the following: ####
                
                # Maria Plan: 11544  # duplicate ein in plan. name in dgem differs from pp
                # Southwest Asthma Plan: 4428  # duplicate ein in plan. name in dgem differs from pp
                # Southwest Asthma & Allergy Associates  # duplicate ein in plan. same name for both
                
                new_plans_to_skip = ['11544','4428','4427']
                
                plans_to_skip += new_plans_to_skip
            
            
            # In[42]:
            
            
            unvalidated_plans_df
            
            
            # In[43]:
            
            
            # Push forward non-validated plans if they exist.
            time.sleep(3)
            
            # Grab all non-validated plans.
            if len(unvalidated_plans_df):
                
                DGEM_find_documents_page.navigate()
                DGEM_find_documents_page.fill_out_5500_documents_form(target_year, 'Not validated') 
                time.sleep(1)
                DGEM_find_documents_page.click_next_button()
                time.sleep(2)   
            else:
                print("No unvalidated plans in worktray")    
            
            
            # In[44]:
            
            
            DGEM_search_results_page = DGEM_Search_Results_Page(browser)    
            
            
            for i in unvalidated_plans_df.index[:]:
                plan_id = unvalidated_plans_df.planid.at[i]
                plan_name = unvalidated_plans_df.plan_name.at[i]
                
            
                print(plan_id, plan_name)
                DGEM_search_results_page.filter_box_input(plan_name) # Use plan name.
                DGEM_search_results_page.update_soup()
            
                #if DGEM_search_results_page.check_soup_for_one_result(plan_id) == True:
            
                pre_validate_results = DGEM_search_results_page.pre_validate_5500vs_batch(plan_id)
                
                
                if pre_validate_results == 0:
                    print(f"Pre-validation successful for {plan_id} {plan_name}")
                    continue
                
                
                # "pre_validate_5500vs_batch()" method will return 1 if the dataframe results have more than one plan (not valid).
                if pre_validate_results == 1:  
                    print("Entering non-validated plan into the search field resulted with an unexpected search result number (0 or more than 1). Skipping.")
                    continue
                    
                    
                # Returns 2 if the resulting message in yellow doesn't indicate success. 
                # Someone might give new specs on what to do if the pre-validation isn't successful. 
                # I never witnessed a failure but I'm guessing that it wont show up in the "Validated" search results.
                # Therefore, it wont be processed in the next steps anyways. 
                # In case someone does give you new direction, you can define that here. 
                if pre_validate_results == 2: 
                    print(f"{plan_id} {plan_name} attempted pre-validation but it failed!")
                    continue
            
            
            # In[45]:
            
            
            DGEM_find_documents_page.navigate()
            DGEM_find_documents_page.fill_out_5500_documents_form(target_year, 'Validated')
            DGEM_find_documents_page.click_next_button()
            
            
            # In[46]:
            
            
            # The df worktray and the DGEM master dataframe was first created to find worktray plans that 
            # needed to be validated. 
            
            # This is the second recreation. The non-validated plans should show up in the Validated section now.
            # Refresh the worktray data frame. Recreate the DGEM master dataframe by using the Validated results.
            # After merging these 2 data frames together, the items that did not have
            # an EIN pairing before should have it now.
            
            df = pp.get_worktray('5500 on HOLD')[['projid', 'taskid', 'task_name', 'planid', 'plan_name', 'proj_name','period_start','period_end']]
            df = filter_for(df)
            
            df['period_start_year'] = df['period_start'].str.split('/').str[-1]
            df = df.sort_values('period_start_year').reset_index(drop=True)
            df = df.loc[df.period_start_year.astype(int) > 2020].reset_index(drop=True)
            
            
            # Get all the files listed and match the plan with the EIN. It will be easier to search for the plan by EIN
            soup = BeautifulSoup(browser.page_source,'lxml')
            table_master = pd.read_html(str(soup.select('#dg5500SearchResults')[0]))[0]
            table_master.rename(columns={'Identifier':'planid'},inplace=True)
            #table_master.fillna('', inplace = True)
            table_master['EIN'] = table_master['EIN'].astype(str)
            table_master['planid'] = table_master['planid'].astype(str)
            
            # take out ".0" in case there was a nan in the list of plan ID's
            for j in table_master.index:
                table_plan_id = table_master.planid.at[j]
                if '.0' in table_plan_id:
                    table_master.planid.at[j] = table_plan_id.replace('.0','')
            
            df = pd.merge(df,table_master[['planid','EIN']],on='planid', how='left')
            df.fillna('', inplace=True)
            
            #dft = df.loc[df.period_start_year == target_year]
            print(f"Starting Index: {sys_argument_starting_index}")
            print(f"Starting Index: {sys_argument_ending_index}")
            
            
            
            
            
            dft = df.loc[(df.period_start_year == target_year) &              (df.index >= sys_argument_starting_index) &              (df.index <= sys_argument_ending_index)]
            
            
            # In[ ]:
            
            
            
            
            
            # In[55]:
            
            
            start = 0    # <----------------------- make sure this is at zero. It can be changed but reset it to zero afterwards
            send_emails = True    # Default should be True but if you don't want to send emails to AM's for some reason, put False
            failure_list = [] # To be sent to someone when results for a plan can't be found on DGEM.
            
            if start != 0:
                print('\n\nWARNING: starting index of loop is not zero\n')
            
            # ## get current template ID for SSA project (JB added 10/11/2023)
            # try:
            #     ssa_template = pp.get_project_template_by_name('Form 8955-SSA (Manual)')[0]
            #     ssa_template_id = ssa_template['Id']
            
            # except:
            #     time.sleep(1)
            #     ssa_template = pp.get_project_template_by_name('Form 8955-SSA (Manual)')[0]
            #     ssa_template_id = ssa_template['Id']
                
            
            # We are indexing with dft because we only want to search plans for target year.
            for i in dft.index[:]:  # <--------------- make sure to reset index to say [start:] if it isn't already so
                plan_id = df.planid.at[i]
                plan_name = df.plan_name.at[i]
                task_id = df.taskid.at[i]
                project_id = df.projid.at[i]
                period_start = df.period_start.at[i]
                period_end = df.period_end.at[i]
                ein = df.EIN.at[i]
            
                print(f'\nindex {i} of {len(df)-1} - plan {plan_id} {plan_name}')
            
                # Exceptions based on email replies
                if plan_id in plans_to_skip:    
                    print('\tSkip this plan for now')
                    continue
            
                if ein == '' and send_emails == False:
                    print('\tSKIP: EIN not identified. Skip for now.')
                    continue
            
                try:
                    plan_contacts = pp.get_plan_employee_roles_by_planid(plan_id)
                except:
                    time.sleep(2)
                    plan_contacts = pp.get_plan_employee_roles_by_planid(plan_id)
            
            
                plan_admin_email = 'clientrelations@nova401k.com'
                consultant_email = '' # removed on 06-06-23
            
                search_criteria = ein
            
                # Special exception to look up plan ID instead of EIN if there are multiple plans with the same EIN
                if list(table_master.EIN).count(ein) > 1:
                    search_criteria = plan_name   # Change search criteria here. Try plan_id then plan_name. If that fails go back after the run and try ein
            
                # Enter EIN in search field
            #     search_field = browser.find_element_by_id('tbFilter5500')
            #     search_field.clear()
            #     search_field.send_keys(search_criteria)
                DGEM_search_results_page.filter_box_input(search_criteria)
                DGEM_search_results_page.update_soup()
                
                soup = BeautifulSoup(browser.page_source,'lxml')
                table = pd.read_html(str(soup.select('#dg5500SearchResults')[0]))[0]
            
            
                if len(table) == 1 and str(table.Identifier.at[0]) == plan_id:  # This is the scenario we desire
                    print('\tplan found')
            
                    # Check to see if the plan ID == Identifier
                    css = '#dg5500SearchResults > tbody > tr.visibleRow > td:nth-child(14)'
            
                    if plan_id == browser.find_element_by_css_selector(css).text:         
                        css = '#dg5500SearchResults > tbody > tr.visibleRow > td > input'
                        browser.find_element_by_css_selector(css).click()   
            
                        # Select "Create SAR Attachment (5500VS Batch)"
                        xpath = '//*[@id="lbAction5500"]'
                        browser.find_element_by_xpath(xpath).send_keys('Create SAR Attachment (5500VS Batch)')
            
                        # Click "Next" to generate SAR (Summary Annual Report)
                        xpath = '//*[@id="lbtnClientNext5500"]'
                        browser.find_element_by_xpath(xpath).click()
            
                        # Check yellow label to see if the SAR is attached successfully
                        if browser.find_element_by_id('lblMessage').text == 'click here to download log file.':
                            print('\tSAR attachment created')
                        else:
                            raise Exception('Failed to create SAR attachment')
            
                        # Search for the plan again except this time, invite signer
                        # Enter plan ID in search field
                        search_field = browser.find_element_by_id('tbFilter5500')
                        search_field.clear()
                        search_field.send_keys(search_criteria)
            
                        soup = BeautifulSoup(browser.page_source,'lxml')
                        table = pd.read_html(str(soup.select('#dg5500SearchResults')[0]))[0]
            
            
                        if len(table) == 1 and str(table.Identifier.at[0]) == plan_id:  # This is the scenario we desire
                            print('\tplan found')
            
                            css = '#dg5500SearchResults > tbody > tr.visibleRow > td:nth-child(14)'     
                            if plan_id == browser.find_element_by_css_selector(css).text:         
                                css = '#dg5500SearchResults > tbody > tr.visibleRow > td > input'
                                browser.find_element_by_css_selector(css).click()   
            
                                # Select "Invite Signers (Batch)"
                                xpath = '//*[@id="lbAction5500"]'
                                browser.find_element_by_xpath(xpath).send_keys('Invite Signers (Batch)')
            
            
                                # Click "Next" to generate SAR (Summary Annual Report)
                                xpath = '//*[@id="lbtnClientNext5500"]'
                                browser.find_element_by_xpath(xpath).click()
            
            
                                # Check yellow label for success message
            
                                yellow_label = browser.find_element_by_id('lblInviteSignersMessage').text
            
                                if 'Invited Signers for:' in yellow_label:
                                    print('\tSigners invited successfully')
                                    df.at[i,'Signer Invited'] = True
                                else:
                                    print('\t',plan_admin_email)
                                    print('\tyellow label:',yellow_label)
                                        
            #                             raise Exception('Yellow label does not indicate that the signer is invited successfully.')
            
                                    email_message = f'''Automation was not able to complete the filing invitation task for {plan_name} for the following reason:
            
            {yellow_label}
            
            Please fix the issue and revalidate the form on DGEM. The automation will attempt to complete the task again on the next run.'''
                                    send_custom_email_to_am(plan_admin_email,plan_id,plan_name,email_message)
                                    continue
            
            
                                # Update task_item 'Date invitation sent' with today's date
                                try:
                                    payload = [task_item for task_item in pp.get_task_by_taskid(task_id, expand = 'TaskItems')['TaskItems'] if task_item['Question'] == 'Date invitation sent' or task_item['Question'] == 'Date signing invitation sent'][0]
                                except:
                                    time.sleep(2)
                                    payload = [task_item for task_item in pp.get_task_by_taskid(task_id, expand = 'TaskItems')['TaskItems'] if task_item['Question'] == 'Date invitation sent' or task_item['Question'] == 'Date signing invitation sent'][0]
            
            
                                payload['Value'] = today    # Update the value of the task_item to reflect today's date
                                task_item_id = payload['Id']
            
                                r = False
                                try:
                                    r = pp.update_taskitem(task_item_id, payload)
                                except:
                                    print('\tretrying: update taskitem 4')
                                    time.sleep(2)
                                    r = pp.update_taskitem(task_item_id, payload)
            
                                if r:
                                    print('\ttask item 4 updated')
                                    df.at[i,'task item 4 updated'] = True
                                else:
                                    raise Exception('task item failed to update value')
            
                                    
            
                                # Complete "Filing Invitation" task
                                try:
                                    r = pp.complete_task(task_id)
                                except:
                                    time.sleep(2)
                                    try:
                                        r = pp.complete_task(task_id)
                                    except:
                                        r = pp.override_task(task_id)
                                if r:
                                    print('\ttask completed')
                                    df.at[i,'task completed'] = True
                                else:
                                    raise Exception('task item failed to complete value')
            
                                # Only for 2022  
                                # Check for existing SSA project, launch manual SSA project (JB added 10/11/2023)
                                
            #                     existing_project = False
                                
            #                     try:
            #                         all_projects = pp.get_projects_by_planid(plan_id, filters=f"PeriodEnd eq '{period_end}'")
            #                         ssa_project = [project for project in all_projects if "Form 8955-SSA" in project['Name']]
                                                   
            #                     except:
            #                         time.sleep(1)
            #                         all_projects = pp.get_projects_by_planid(plan_id, filters=f"PeriodEnd eq '{period_end}'")
            #                         ssa_project = [project for project in all_projects if "Form 8955-SSA" in project['Name']]
                                                   
            #                     if len(ssa_project) > 0:
            #                         existing_project = True
                                
            #                     ssa_proj = None
                                                   
            #                     if existing_project is False:
            #                         try:
            #                             ssa_proj = pp.add_project(plan_id, ssa_template_id, StartDate=today, 
            #                                            PeriodStart=period_start, PeriodEnd=period_end)
                                                   
            #                         except:
            #                             time.sleep(1)
            #                             ssa_proj = pp.add_project(plan_id, ssa_template_id, StartDate=today, 
            #                                            PeriodStart=period_start, PeriodEnd=period_end)
                                                   
            #                         if ssa_proj:
            #                             print('\tmanual SSA project launched')
            
            #                         else:
            #                             raise Exception('manual SSA project needed but failed to launch') 
                                
            
            
                            else:
                                raise Exception('The plan ID does not match the Identifier on the table')
                        else:
                            raise Exception('The search for the plan based on the Identifier or plan ID does not lead to one result')
                    else:
                        raise Exception('The plan ID does not match the Identifier on the table')
            
                elif ein == '':
                    # Send email to AM
                    timestamp = dt2.today().strftime('%#m/%#d/%Y %#I:%M %p')
                    send_email_to_am(plan_admin_email,plan_id,plan_name,email_username, email_pw)
                    print('\temail sent to crm')
                    df.at[i,'emailed_crm'] = plan_admin_email
                    df.at[i,'emailed_consultant'] = consultant_email
                    df.at[i,'timestamp'] = timestamp
                    continue
            
                else:
                    print('HTML table could not be read. Appending to failure list.')
                    failure_list.append(f"{plan_id} {plan_name}\n")
                    continue
                    #raise Exception('HTML table could not be read')
            
                    
            if failure_list:
                print("Emailing failure list")
                email_dgem_failures(target_year,failure_list,['dcarr@nova401k.com']) # Removed Jason Worms on 6/28/24. Added Dawn. -Andrew
                
            df.fillna('', inplace = True)
            print('\nDone.')
            
            
            # In[ ]:
            
            
            
            
            
            # In[ ]:
            
            
            run_afs_side = False
            
            if not run_afs_side:
                browser.quit()
                #os.kill(browser.service.process.pid, 15)
                #os.kill(browser.service.process.pid, signal.SIGTERM)
                raise SystemExit('Done! Not launching AFS side')
            
            
            # ### Launch equivalent project on the AFS side
            # When Filing Invitation task completed on Nova Form 5500 project, launch a Form 5500 (Form 5500-Audit Review or Form 5500-SF Review based on the Nova project) project in AFS version of Pro for Partner level clients, run daily
            # * Use the same period start and period end as the project on Nova's side
            # * NOTE: right now, we are only launching "Form 5500-SF Review", the "Form 5500-Audit Review" will have to wait for discussion with Jason
            
            # In[ ]:
            
            
            # Map group ID to group name
            group_infos = requests.get('https://api.pensionpro.com/v1/plans/plangroups',headers=pp.headers).json()['Values']
            
            map_group_id_to_group_name = {}
            
            for info in group_infos:
                group_id = info['Id']
                group_name = info['DisplayName']
                map_group_id_to_group_name[group_id] = group_name 
            time.sleep(1)
            
            
            # In[ ]:
            
            
            with open(all_plans_file_nova,'rb') as f:
                all_plans_nova = pickle.load(f)
                
            with open(all_plans_file_afs,'rb') as f:
                all_plans_afs = pickle.load(f)
            
            
            # In[ ]:
            
            
            map_plan_id_to_plan_group_name = {}
            
            for plan in all_plans_nova:
                plan_id = plan['InternalPlanId']
                group_id = plan['PlanGroupId']
                plan_group = map_group_id_to_group_name[group_id]
                map_plan_id_to_plan_group_name[plan_id] = plan_group
            
            
            df['plan_group'] = df.planid.map(map_plan_id_to_plan_group_name)
            df.fillna('',inplace=True)
            
            
            # In[ ]:
            
            
            # Create a dictionary that maps Nova's plan ID to AFS Plan ID based on record keeper account ID
            map_nova_plan_id_to_afs_plan_id = lam.map_nova_plan_id_to_afs_plan_id()
            
            df['afs_plan_id'] = df.planid.map(map_nova_plan_id_to_afs_plan_id)
            df.fillna('',inplace=True)
            
            
            # In[ ]:
            
            
            len(map_nova_plan_id_to_afs_plan_id)
            
            
            # In[ ]:
            
            
            # Get all afs plans that could not be mapped
            df_not_found = df.loc[(df.plan_group != 'N/A') & (df.afs_plan_id == '')]
            
            df_not_found
            
            
            # In[ ]:
            
            
            # Add missing afs_plan_ids using afs search text
            if len(df_not_found)>0:
                
                map_afs_plan_id_to_nova_plan_id = lam.map_afs_plan_id_to_nova_plan_id_by_search_text()    
                
                for i in df_not_found.index:
                    plan_id = df.planid.at[i]
                    plan_name = df.plan_name.at[i]
                    print(f'index {i} - plan {plan_id} {plan_name}')
                    
                    for afs_plan_id, nova_plan_id in map_afs_plan_id_to_nova_plan_id.items():
                        
                        # This part is necessary because spaces were found
                        afs_plan_id = afs_plan_id.strip()
                        nova_plan_id = nova_plan_id.strip()
                        
                        if nova_plan_id == plan_id:
                            df.afs_plan_id.at[i] = afs_plan_id
                            print('\tAFS plan ID found')
                
            
            # Get all afs plans that could not be mapped
            df_not_found = df.loc[(df.plan_group != 'N/A') & (df.afs_plan_id == '')]
            
            if len(df_not_found) > 0:
                raise Exception('There are still plans that could not be mapped')
            else:
                print('Continue: all AFS plans have been mapped. Go into Pension pro and update')
            
            
            # In[ ]:
            
            
            df_not_found
            
            
            # In[ ]:
            
            
            df
            
            
            # In[ ]:
            
            
            # df.at[7,'afs_plan_id'] = '1026'
            # df
            
            
            # In[ ]:
            
            
            df_partner = df.loc[df.plan_group.str.contains('Partner')] # Do not reset index
            
            df_partner
            
            
            # In[ ]:
            
            
            # Get corresponding AFS project template IDs
            for proj in app.get_project_template()['Values']:
                if proj['Name'] == 'Form 5500-Audit Review':
                    afs_audit_review_id = proj['Id']
                if proj['Name'] == 'Form 5500-SF Review':
                    afs_sf_review_id = proj['Id']                                                                                                                                                                
            
            print(f'AFS Form 5500-Audit Review Project Template ID: {afs_audit_review_id}')
            print(f'AFS Form 5500-SF Review Project Templat ID: {afs_sf_review_id}')
            time.sleep(1)
            
            
            # In[ ]:
            
            
            # Launch project
            for i in df_partner.index[:]:
                plan_id = df.planid.at[i]
                plan_name = df.plan_name.at[i]
                afs_plan_id = df.afs_plan_id.at[i]
                
                proj_name = df.proj_name.at[i]
                period_start = df.period_start.at[i]
                period_end = df.period_end.at[i]
                
                print(f'index {i}: plan {plan_id} {plan_name}')
                
                try:
                    if df.at[i,'task completed'] == '':
                        print('\tSKIP: task was not completed')
                        continue
                except Exception:
                    continue
                
                if proj_name == 'DC Annual Governmental Forms - Small Filer':
                    
                    r = False
                    
                    try:
                        r = app.add_project(
                                planid = afs_plan_id,
                                ProjectTemplateId = afs_sf_review_id,
                                StartDate = today,
                                PeriodStart = period_start,
                                PeriodEnd = period_end,
                            )
                    except:   # check to see if a project is already launched
                        
                        proj_already_exists = False
                        projects = False
            
                        projects = app.get_projects_by_planid(afs_plan_id)
            
                        for proj in projects:
                            
                            if proj['PeriodEnd'] == None or proj['PeriodStart'] == None:
                                continue
                            
                            if proj['Name'] == 'Form 5500-SF Review' and (proj['PeriodEnd'].split()[0] == period_end or proj['PeriodStart'].split()[0] == period_start):
                                proj_already_exists = True
                                break
            
                        if proj_already_exists:
                            df.at[i,'afs_proj_launched'] = 'Already launched manually'
                            print('\tSKIP: Project already launched manually')
                            continue
                        else:
                            raise Exception(f'Check plan {plan_id}. There doesn\'t seem to be a project already launched but it still errored.')
                        
                    if r:
                        df.at[i,'afs_proj_launched'] = 'Form 5500-SF Review'
                        df.at[i,'afs_proj_id'] = r['Id']
                        print('\tAFS project launched: "Form 5500-SF Review"')
                    else:
                        raise Exception('AFS project failed to launch')
                    
            df.fillna('',inplace=True)           
            print('\nDone')      
            
            
            # # AFS Part ends here
            
            # In[ ]:
            
            
            try:
                df_emails_sent_last = df.loc[df['task completed']==''].reset_index(drop=True)
            except:
                pass
            
            
            # In[ ]:
            
            
            try:
                df_emails_sent_last
            except:
                pass
            
            
            # In[ ]:
            
            
            df
            
            
            # In[ ]:
            
            
            try:
                # Save a dataframe of all the plans that were emailed out on the last run. Use this to see which AM is being emailed twice.
                pickle_file = 'df_emails_sent_last.pkl'
                df_emails_sent_last.to_pickle(pickle_file)
            except:
                pass
            
            
            # In[ ]:
            
            
            try:
                # Update log of all emails sent
                pickle_file = 'df_all_emails_sent.pkl'
                df_email_log = pd.read_pickle(pickle_file)
                df_email_log = pd.concat([df_email_log,df_emails_sent_last],ignore_index=True)
            
                df_email_log.to_pickle(pickle_file)
            except:
                pass
            
            
            # In[ ]:
            
            
            try:
                df = df.loc[df['task completed']==True].reset_index(drop=True)
            except:
                pass
            
            
            # In[ ]:
            
            
            pickle_file = 'df_filing_invitation_tasks_completed.pkl'
            df_log = pd.read_pickle(pickle_file)
            
            
            # In[ ]:
            
            
            len(df_log),len(df)
            
            
            # In[ ]:
            
            
            df.head()
            
            
            # In[ ]:
            
            
            for column in ['afs_proj_launched','afs_proj_id','Save Email task completed']:
                
                if column not in df.columns:
                    df.insert(len(df.columns),column,'')
            
            
            # In[ ]:
            
            
            try:
                df = df[['projid', 'taskid', 'task_name', 'planid', 'plan_name', 'proj_name',
                       'period_start', 'period_end', 'period_start_year', 'EIN',
                       'Signer Invited', 'task item 4 updated', 'PPT and TH Project Launched',
                       'task item 5 updated', 'task completed','Save Email task completed','plan_group','afs_plan_id','afs_proj_launched','afs_proj_id']]
            
                df = pd.concat([df_log,df],ignore_index=True)
                df.fillna('',inplace=True)
            except:
                pass
            
            
            # In[ ]:
            
            
            df.to_pickle(pickle_file)
            print('pickle file updated')
            
            
            # In[ ]:
            
            
            len(df)
            
            
            # In[ ]:
            
            
            browser.quit()
            
            
            # In[ ]:
            
            
            dt2.today().strftime('%m-%d-%Y %I:%M %p')
            
            
            # In[ ]:
            
            
            
            
            
            # In[ ]:
            
            
            
            
            
            # In[ ]:
            
            
            
            
            
            # In[ ]:
            
            
            
            
            
            # In[ ]:
            
            
            
            
            
            # In[ ]:
            
            
            
            

    schedule_logger('Finished', pyfile= __file__)

except SystemExit:
    schedule_logger('Finished', pyfile= __file__)
    raise
    
except Exception as e:
    email_error_to_automation(e, __file__)
    schedule_logger('Error', pyfile= __file__)

            