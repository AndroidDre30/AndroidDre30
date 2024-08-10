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
            sys.path.insert(0, r'C:\Users\Public\WPy64-39100\notebooks\Anjana Shaji')
            import pensionpro_api as pp
            from dateutil.relativedelta import relativedelta
            from datetime import datetime,timedelta,date
            import numpy as np
            import time
            import pandas as pd
            import os
            from public_vault import Username, Password, OAuth
            from bs4 import BeautifulSoup
            from pathlib import Path
            
            import smtplib
            from email.mime.multipart import MIMEMultipart
            from email.mime.text import MIMEText
            from email.mime.base import MIMEBase
            from email import encoders
            
            from selenium import webdriver
            from selenium.webdriver.common.by import By
            from selenium.common.exceptions import NoSuchElementException
            from selenium.webdriver.support.ui import WebDriverWait
            from selenium.webdriver.support import expected_conditions as ec
            from webdriver_manager.chrome import ChromeDriverManager
            
            from webdriver_manager.chrome import ChromeDriverManager
            
            from O365 import Account, FileSystemTokenBackend
            
            
            # In[2]:
            
            
            my_username = Username('DGEM')
            my_pw = Password('DGEM')
            today_date = date.today()
            df_file_path = r"Y:\Automation\Team Scripts\Anjana Shaji\8955 SSA DF\df_filing.xlsx"
            user = os.getlogin()
            download_path = f'C:/Users/{user}/Downloads'
            filing_path = r'Y:\Automation\Team Scripts\Anjana Shaji\8955 SSA DF\filing_report.xlsx'
            email_box = 'form5500@nova401k.com'
            o365_directory = Path(r'C:\Users\Public\WPy64-39100\notebooks\scheduler')
            df_bad_path = r"Y:\Automation\Team Scripts\Anjana Shaji\8955 SSA DF\df_bad_status.xlsx"
            cols = ['TPA Plan ID','Filed']
            df_filing = pd.DataFrame(columns=cols)
            df_filing.to_excel(filing_path, index=False)
            
            
            # In[3]:
            
            
            def mail_filing_status_report(file_path):
                html_head = """
                <html xmlns:v="urn:schemas-microsoft-com:vml" xmlns:o="urn:schemas-microsoft-com:office:office" xmlns:w="urn:schemas-microsoft-com:office:word" xmlns:m="http://schemas.microsoft.com/office/2004/12/omml" xmlns="http://www.w3.org/TR/REC-html40">
                <head><META HTTP-EQUIV="Content-Type" CONTENT="text/html; charset=us-ascii"><meta name=Generator content="Microsoft Word 15 (filtered medium)">
                </head>"""
                
                toaddr = ['jworms@nova401k.com', 'msvehla@nova401k.com']
                cc_list = []
            
                html = html_head
            
                fromaddr = 'automation@nova401k.com'
                password = 'Rub73595'
            
                toaddrs = toaddr + cc_list
            
                if len(toaddrs) == 0:
                    x = "No emails to send"
                else:
                    msg = MIMEMultipart('alternative')
            
                    msg['From'] = fromaddr
                    msg['To'] = ','.join(toaddr)
                    msg['CC'] = ','.join(cc_list)
                    subject = f'Daily 8955-SSA Bad Filing Status Report'
                    msg['Subject'] = subject
                    part = MIMEText(html, 'html')
                    msg.attach(part)
                    
                    part1 = MIMEBase('application', "octet-stream")
                    part1.set_payload(open(file_path, "rb").read())
                    encoders.encode_base64(part1)
                    part1.add_header('Content-Disposition', 'attachment; filename= "8955 SSA Bad Filing Status Report.xlsx"')
                    msg.attach(part1)
            
            
                    with smtplib.SMTP('smtp.office365.com', 587) as server:
                        server.starttls()
                        server.login(fromaddr, password)
                        x = server.sendmail(fromaddr, toaddrs, msg.as_string())
            
            
            # In[4]:
            
            
            def get_latest_file(directory):
                ## Reads in the find results. This will let us use the names as selenium lookups
            
                fr_files = [os.path.join(directory, file) for file in os.listdir(directory) if os.path.isfile(os.path.join(directory, file))]
            
                # Sort the files based on modification time (latest first)
                sorted_fr_files = sorted(fr_files, key=lambda x: os.path.getmtime(x), reverse=True)
            
                # Get the latest modified file
                latest_find_file = sorted_fr_files[0] if sorted_fr_files else ""
            
                # Convert the latest_file path to string
                return str(latest_find_file)
            
            
            # In[5]:
            
            
            plan_name_list = []
            df = pp.get_worktray('Automation', get_all=True)
            filt1 = df['task_name'] == '8955-SSA Filing'
            filt2 = df['proj_name'] == 'Form 8955-SSA (Automated)'
            df = df[filt1 & filt2]
            if df.empty:
                raise SystemExit("Script is shutting down")
            plan_name_list = df['plan_name'].tolist()
            plan_name_list
                
            
            
            # In[6]:
            
            
            len(plan_name_list)
            
            
            # In[3]:
            
            
            now = datetime.now()
            previous_year = str(int(now.strftime("%Y"))-1)
            
            generated_import_file = 'AA'
            
            # logs in using the Automation login
            if generated_import_file != "No import":
            
                browser = webdriver.Chrome(ChromeDriverManager().install())
                # Login-----------------------------------------------------------------------
                browser.get('https://dgem.asc-net.com/ascidoc/login.aspx')
            
                tpakey = browser.find_element_by_name('tbTPA')
                tpakey.send_keys('asc438')
            
                username = browser.find_element_by_name('tbUsername')
                username.send_keys(my_username)               
            
                WebDriverWait(browser,10).until(ec.visibility_of_element_located((By.NAME,'tbPassword')))
                password = browser.find_element_by_name('tbPassword')
                password.send_keys(my_pw)
            
                loginbutton = browser.find_element_by_name('btnLogin')
                loginbutton.click()
                
                
                try:
                    # Click to send verification code via "Email"
                    WebDriverWait(browser,10).until(ec.visibility_of_element_located((By.XPATH,'//*[@id="MFAUC_rbEmail"]')))
                    browser.find_element_by_xpath('//*[@id="MFAUC_rbEmail"]').click()
            
                    # Click SEND
                    browser.find_element_by_xpath('//*[@id="MFAUC_btnSend"]').click()
                    
                    if generated_import_file != "No import":
                        from O365 import Account, FileSystemTokenBackend
            
                        credentials = OAuth('o365_client_id'), OAuth('o365_secret_value')
            
                        token_backend = FileSystemTokenBackend(token_path=o365_directory,
                                                               token_filename='oauth_token.txt')
                        account = Account(credentials,
                                          token_backend=token_backend,
                                          scopes = ['basic','message_all','mailbox'])
            
                        if not account.is_authenticated:
                            account.authenticate()
            
                        time.sleep(60)
            
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
            
                        # Enter Verification code
                        browser.find_element_by_xpath('//*[@id="MFAUC_tbCode"]').send_keys(verification_code)
            
                        # Click verify
                        browser.find_element_by_xpath('//*[@id="MFAUC_btnVerify"]').click()
            
                        # Click Yes
                        WebDriverWait(browser,10).until(ec.visibility_of_element_located((By.XPATH,'//*[@id="MFAUC_rbYes"]')))
                        browser.find_element_by_xpath('//*[@id="MFAUC_rbYes"]').click()
            
                        # Click "Continue"
                        browser.find_element_by_xpath('//*[@id="MFAUC_btnContinue"]').click()
            
                    else:
                        exit()
                except:
                    # no MFA required
                    pass
            
            
            # In[8]:
            
            
            # Download Find results: you will need to update this to select the 8955s
            time.sleep(10)
            errorlist = []
            
            browser.get('https://dgem.asc-net.com/ascidoc/Find.aspx')
            
            time.sleep(10)
            form_type = browser.find_element(by='id',value='ddDocumentType').send_keys("8955-SSA")
            status = browser.find_element(by='id',value='dd_SSA_status').send_keys("All")
            # checkbox = browser.find_element(by='name',value='cb_SSA_statusChangedAfter').click()
            
            time.sleep(5)
            checkbox = browser.find_element(by='id',value='lbtnNext_5500').click()
            
            time.sleep(20)
            
            dropdown_menu = browser.find_element(by='name',value='lbActionSSA').send_keys("Export Find Results")
            
            find_next = browser.find_element(by='id',value='lbtnClientNextSSA').click()
            
            time.sleep(10)
            
            link_check = False
            
            while link_check is False:
                if "Export is complete." in browser.page_source:
                    time.sleep(1)
                    browser.find_element(by='link text',value='here').click()
                    link_check = True
                    
            time.sleep(20)
            
            
            # In[9]:
            
            
            ## Reads in the find results. This will let us use the names as selenium lookups
            latest_find_result = get_latest_file(download_path)
            results = pd.read_html(latest_find_result, converters={"EIN":str,"Plan #":str})
            df_results = results[0]
            
            df_results['Lookup'] = df_results['Plan #'] + df_results['EIN']
            os.remove(latest_find_result)
            df_results
            
            
            # In[10]:
            
            
            df_results['Identifier'] = df_results['Identifier'].fillna(0)
            df_results['Identifier'] = df_results['Identifier'].astype(int)
            df_results
            
            
            # In[11]:
            
            
            df_submitted = df_results[df_results['Status'] == "Submitted"]
            df_valid = df_results[df_results['Status'] == "Valid"]
            
            
            # In[12]:
            
            
            df_submitted
            
            
            # In[13]:
            
            
            df_valid['Identifier'] = df_valid['Identifier'].astype(int)
            plan_name_valid_list = []
            for index, row in df.iterrows():
                plan_id = row['planid']
                plan_id = int(plan_id)
                if plan_id in df_valid['Identifier'].values:
                    plan_name = df_valid.loc[df_valid['Identifier'] == plan_id, 'Name'].iloc[0]
                    plan_name_valid_list.append(plan_name)
            plan_name_valid_list
            
            
            # In[14]:
            
            
            len(plan_name_valid_list)
            
            
            # In[15]:
            
            
            df_submitted['Identifier'] = df_submitted['Identifier'].astype(int)
            plan_id_submitted_list = []
            for index, row in df.iterrows():
                plan_id = row['planid']
                plan_id = int(plan_id)
                if plan_id in df_submitted['Identifier'].values:
                    plan_id_submitted_list.append(plan_id)
            plan_id_submitted_list
            
            
            # In[16]:
            
            
            today_date_str = today_date.strftime("%m/%d/%Y, %H:%M:%S")
            for plan_id in plan_id_submitted_list:
                projects = pp.get_projects_by_planid(plan_id,filters=f"Name eq 'Form 8955-SSA (Automated)'", expand="TaskGroups.Tasks")
                project = [project for project in projects if project['CompletedOn'] is None]
                if len(project) > 0:
                    project = project[0]
                
                project_id = project['Id']
                
                for taskgroup in project['TaskGroups']:
                    for task in taskgroup['Tasks']:
                        if task['TaskName'] == '8955-SSA Filing' and task['DateCompleted'] is None:
                            task_item = pp.get_taskitems_by_taskid(task['Id'], filters = "ShortName eq '8955-SSA File Date'")[0]
                            task_item['Value'] = today_date_str
                            pp.update_taskitem(task_item)
                            print('updated taskitem for plan: ', plan_id)
                            pp.override_task(task['Id'])
            
            
            # In[17]:
            
            
            time.sleep(10)
            errorlist = []
            
            browser.get('https://dgem.asc-net.com/ascidoc/Find.aspx')
            
            time.sleep(10)
            form_type = browser.find_element(by='id',value='ddDocumentType').send_keys("8955-SSA")
            # checkbox = browser.find_element(by='name',value='cb_SSA_statusChangedAfter').click()
            
            time.sleep(5)
            checkbox = browser.find_element(by='id',value='lbtnNext_5500').click()
            
            time.sleep(20)
            
            for plan in plan_name_valid_list:
                try:
                    browser.find_element(by='xpath',value='//td[contains(text(),"{}")]/ancestor::tr[1]//input[@type = "checkbox"]'.format(plan)).click()
                    print(plan)
            
                except:
                    errorlist.append(plan)
                    continue
            
            dropdown_menu = browser.find_element(by='name',value='lbActionSSA').send_keys("Transmit FIRE Filing")
            
            find_next = browser.find_element(by='id',value='lbtnClientNextSSA').click()
            
            time.sleep(20)
            
            
            # In[18]:
            
            
            errorlist
            
            
            # In[19]:
            
            
            time.sleep(300)
            errorlist = []
            
            browser.get('https://dgem.asc-net.com/ascidoc/Find.aspx')
            
            time.sleep(10)
            form_type = browser.find_element(by='id',value='ddDocumentType').send_keys("8955-SSA")
            status = browser.find_element(by='id',value='dd_SSA_status').send_keys("Submitted (Not yet processed to IRS)")
            # checkbox = browser.find_element(by='name',value='cb_SSA_statusChangedAfter').click()
            
            time.sleep(5)
            checkbox = browser.find_element(by='id',value='lbtnNext_5500').click()
            
            time.sleep(20)
            
            for plan in plan_name_valid_list:
                try:
                    browser.find_element(by='xpath',value='//td[contains(text(),"{}")]/ancestor::tr[1]//input[@type = "checkbox"]'.format(plan)).click()
                    print(plan)
            
                except:
                    errorlist.append(plan)
                    continue
            
            time.sleep(20)
            
            
            # In[20]:
            
            
            errorlist
            
            
            # In[21]:
            
            
            final_plan_name_list = [plan for plan in plan_name_valid_list if plan not in errorlist]
            final_plan_name_list
            
            
            # In[22]:
            
            
            len(final_plan_name_list)
            
            
            # In[23]:
            
            
            plan_id_list = []
            for plan in final_plan_name_list:
                plan_id = df_results.loc[df_results['Name'] == plan, 'Identifier'].iloc[0]
                plan_id_list.append(plan_id)
            plan_id_list
            
            
            # In[25]:
            
            
            today_date_str = today_date.strftime("%m/%d/%Y, %H:%M:%S")
            df_filing = pd.read_excel(filing_path)
            for plan_id in plan_id_list:
                projects = pp.get_projects_by_planid(plan_id,filters=f"Name eq 'Form 8955-SSA (Automated)'", expand="TaskGroups.Tasks")
                project = [project for project in projects if project['CompletedOn'] is None]
                if len(project) > 0:
                    project = project[0]
                
                project_id = project['Id']
                
                for taskgroup in project['TaskGroups']:
                    for task in taskgroup['Tasks']:
                        if task['TaskName'] == '8955-SSA Filing' and task['DateCompleted'] is None:
                            task_item = pp.get_taskitems_by_taskid(task['Id'], filters = "ShortName eq '8955-SSA File Date'")[0]
                            task_item['Value'] = today_date_str
                            pp.update_taskitem(task_item)
                            print('updated taskitem for plan: ', plan_id)
                            pp.override_task(task['Id'])
                row = {'TPA Plan ID' : plan_id,'Filed': 'Yes'}
                df_filing = pd.concat([df_filing, pd.DataFrame([row])], ignore_index=True)
            print('Done!')
            
            
            # In[26]:
            
            
            df_filing.to_excel(filing_path, index = False)
            
            
            # In[4]:
            
            
            time.sleep(10)
            errorlist = []
            
            browser.get('https://dgem.asc-net.com/ascidoc/Find.aspx')
            bad_flag = True
            time.sleep(10)
            try:
                form_type = browser.find_element(by='id',value='ddDocumentType').send_keys("8955-SSA")
                status = browser.find_element(by='id',value='dd_SSA_status').send_keys("Bad (System)")
                # checkbox = browser.find_element(by='name',value='cb_SSA_statusChangedAfter').click()
            
                time.sleep(5)
                checkbox = browser.find_element(by='id',value='lbtnNext_5500').click()
            
                time.sleep(20)
            
                dropdown_menu = browser.find_element(by='name',value='lbActionSSA').send_keys("Export Find Results")
            
                find_next = browser.find_element(by='id',value='lbtnClientNextSSA').click()
            
                time.sleep(10)
            
                link_check = False
            
                while link_check is False:
                    if "Export is complete." in browser.page_source:
                        time.sleep(1)
                        browser.find_element(by='link text',value='here').click()
                        link_check = True
            
                time.sleep(20)
            except Exception as e:
                print(e)
                bad_flag = False
            
            
            # In[6]:
            
            
            ## Reads in the find results. This will let us use the names as selenium lookups
            if bad_flag:
                latest_find_result = get_latest_file(download_path)
                results = pd.read_html(latest_find_result, converters={"EIN":str,"Plan #":str})
                df_results = results[0]
                os.remove(latest_find_result)
                df_results
            else:
                df_results = pd.DataFrame()
            
            
            # In[8]:
            
            
            if not df_results.empty:
                print('No')
                df_results.to_excel(df_bad_path)
                mail_filing_status_report(df_bad_path)
            
            
            # In[33]:
            
            
            browser.quit()
            
            
            # In[ ]:
            
            
            
            

    schedule_logger('Finished', pyfile= __file__)

except SystemExit:
    schedule_logger('Finished', pyfile= __file__)
    raise
    
except Exception as e:
    email_error_to_automation(e, __file__)
    schedule_logger('Error', pyfile= __file__)

            