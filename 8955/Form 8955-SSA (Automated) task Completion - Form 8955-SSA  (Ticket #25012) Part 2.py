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
            import os
            import shutil
            import time
            from xml.sax import saxutils
            import re
            import zipfile
            
            from public_vault import Username, Password, OAuth
            
            from selenium import webdriver
            from selenium.webdriver.common.by import By
            from selenium.common.exceptions import NoSuchElementException
            from selenium.webdriver.support.ui import WebDriverWait
            from selenium.webdriver.support import expected_conditions as ec
            from webdriver_manager.chrome import ChromeDriverManager
            
            from webdriver_manager.chrome import ChromeDriverManager
            
            from datetime import datetime, date
            from datetime import timedelta
            
            
            import lam
            import xlwt
            import pandas as pd
            import numpy as np
            import pensionpro_api as pp
            from pathlib import Path
            
            import smtplib
            from email.mime.multipart import MIMEMultipart
            from email.mime.text import MIMEText
            from email.mime.base import MIMEBase
            from email import encoders
            start = datetime.now()
            print(start)
            
            
            # In[2]:
            
            
            my_username = Username('DGEM')
            my_pw = Password('DGEM')
            contact_name = 'Nova Associates'
            contact_phone = '7135245192'
            contact_email = 'form5500@nova401k.com'
            email_box = 'automation@nova401k.com'
            
            
            # In[3]:
            
            
            user = os.getlogin()
            user
            
            
            # In[4]:
            
            
            ## input for this is the generated xml file
            today_date = date.today()
            current_year = today_date.strftime('%Y')
            today_date = datetime.strftime(today_date, "%m-%d-%Y")
            xml_folder = r'Y:\5500\2022\Automation\8955-SSA'
            xml_file_name = f'{today_date} Export 2.xml'
            generated_import_file = f'{xml_folder}\{xml_file_name}'
            import_directory = r'Y:\ASC\Exported Reports\8955 Exports\DGEM Import Logs'
            user = os.getlogin()
            download_path = f'C:/Users/{user}/Downloads'
            xml_df_file = r"Y:\Automation\Team Scripts\Anjana Shaji\8955 SSA DF\df_concat.xlsx"
            signer_file_folder = r"Y:\ASC\Exported Reports\8955 Automation\Signer Import Files"
            validation_directory = r'Y:\ASC\Exported Reports\8955 Automation\DGEM Validation Files'
            error_path = r'Y:\Automation\Team Scripts\Anjana Shaji\8955 SSA DF\error_report.xlsx'
            
            
            # In[5]:
            
            
            def mail_validation_report(file_path):
                html_head = """
                <html xmlns:v="urn:schemas-microsoft-com:vml" xmlns:o="urn:schemas-microsoft-com:office:office" xmlns:w="urn:schemas-microsoft-com:office:word" xmlns:m="http://schemas.microsoft.com/office/2004/12/omml" xmlns="http://www.w3.org/TR/REC-html40">
                <head><META HTTP-EQUIV="Content-Type" CONTENT="text/html; charset=us-ascii"><meta name=Generator content="Microsoft Word 15 (filtered medium)">
                </head>"""
                
                toaddr = ['jworms@nova401k.com', 'msvehla@nova401k.com']
            
                html = html_head
            
                fromaddr = 'automation@nova401k.com'
                password = 'Rub73595'
            
                toaddr = [address for address in toaddr if address != None]
            
                toaddrs = toaddr
            
                if len(toaddrs) == 0:
                    x = "No emails to send"
                else:
                    msg = MIMEMultipart('alternative')
            
                    msg['From'] = fromaddr
                    msg['To'] = ','.join(toaddr)
                    subject = f'Daily 8955-SSA Validation Report'
                    msg['Subject'] = subject
                    part = MIMEText(html, 'html')
                    msg.attach(part)
                    
                    part1 = MIMEBase('application', "octet-stream")
                    part1.set_payload(open(file_path, "rb").read())
                    encoders.encode_base64(part1)
                    part1.add_header('Content-Disposition', 'attachment; filename= "8955 SSA Validation Report.xlsx"')
                    msg.attach(part1)
            
            
                    with smtplib.SMTP('smtp.office365.com', 587) as server:
                        server.starttls()
                        server.login(fromaddr, password)
                        x = server.sendmail(fromaddr, toaddrs, msg.as_string())
            
            
            # In[6]:
            
            
            def get_latest_file(directory):
                ## Reads in the find results. This will let us use the names as selenium lookups
            
                fr_files = [os.path.join(directory, file) for file in os.listdir(directory) if os.path.isfile(os.path.join(directory, file))]
            
                # Sort the files based on modification time (latest first)
                sorted_fr_files = sorted(fr_files, key=lambda x: os.path.getmtime(x), reverse=True)
            
                # Get the latest modified file
                latest_find_file = sorted_fr_files[0] if sorted_fr_files else ""
            
                # Convert the latest_file path to string
                return str(latest_find_file)
            
            
            # In[7]:
            
            
            now = datetime.now()
            previous_year = str(int(now.strftime("%Y"))-1)
            o365_directory = Path(r'C:\Users\Public\WPy64-39100\notebooks\scheduler')
            
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
            
            
            # Import xml file to DGEM
            time.sleep(15)
            
            browser.get('https://dgem.asc-net.com/ascidoc/efast2/wizards/Import_3rdParty.aspx')
            
            time.sleep(15)
            
            dropdown_upload = browser.find_element(by='id',value='ddlFormType').send_keys("Form 8955-SSA")
            individual_stmt_checkbox = browser.find_element(by='id',value='cbStatement').click()
            enable_fire_checkbox = browser.find_element(by='id',value='cbElectronicInfo').click()
            overwrite_checkbox = browser.find_element(by='id',value='cbOverwrite').click()
            contact_name = browser.find_element(by='id',value='tbContactName').send_keys(contact_name)
            contact_phone = browser.find_element(by='id',value='tbContactphone').send_keys(contact_phone)
            contact_email = browser.find_element(by='id',value='tbContactEmail').send_keys(contact_email)
            upload_path = browser.find_element(by='id',value='fileSelect').send_keys(generated_import_file)
            
            email_box = browser.find_element(by='id',value='tbEmail').send_keys(email_box)
            email_next = browser.find_element(by='id',value='ibtnUpload').click()
            
            time.sleep(60)
            
            
            # In[9]:
            
            
            email_found=False
            timeout = time.time() + 60*15   # timeout 15 minutes from now (stops looking for email after this point)
            
            from O365 import Account, FileSystemTokenBackend
            
            credentials = OAuth('o365_client_id'), OAuth('o365_secret_value')
            
            token_backend = FileSystemTokenBackend(token_path=o365_directory,
                                                   token_filename='oauth_token.txt')
            account = Account(credentials,
                              token_backend=token_backend,
                              scopes = ['basic','message_all','mailbox'])
            print(account)
            if not account.is_authenticated:
                account.authenticate()
                
            mailbox = account.mailbox(resource=email_box)
            while email_found is False:
                print('in while')
                time.sleep(10)
                inbox = mailbox.inbox_folder().get_messages()
                if time.time() > timeout:
                    print("timeout")
                    break
                for message in inbox:
                    msg_sender = str(message.sender)
                    print(msg_sender)
                    msg_time_sent = message.sent #get time message was sent
                    msg_time_sent = msg_time_sent.replace(tzinfo=None)
                    recent_email = msg_time_sent > datetime.now() - timedelta(minutes=10)
                    print(recent_email)
                    if (msg_sender == 'Import results (support@pension-plan-emails.com)' # will need to check this
                        and (msg_subject := message.subject) == 'Completed importing files' # need to check the subject too
                       and recent_email is True):
                        msg_body = message.body
                        regex = re.search(r'<a class="fill-div" href="([^"]+)"',msg_body) # find sendgrid url
                        import_report_download = regex.group(1)
                        message.mark_as_read()
                        email_found=True
                        break
                        
            if email_found is True:  
                print('Email Found')
                browser.get(import_report_download)
            
            else:
                raise Exception("No email found")
                
            time.sleep(30)
            
            
            # In[10]:
            
            
            ## Reads in the import results
            latest_import_result = get_latest_file(download_path)
            
            # create your own directory to store these import logs
            
            
            df_import_results = pd.read_table(latest_import_result, converters={"EIN":str,"PlanNumber":str})
            
            
            df_import_results['Lookup'] = df_import_results['PlanNumber'] + df_import_results['EIN']
            
            just_import_file = latest_import_result.split('\\')[1]
            shutil.copy(latest_import_result, f"{import_directory}/{just_import_file}")
            os.remove(latest_import_result)
            df_import_results
            
            
            # In[11]:
            
            
            # Read in the saved df that we used for xml generation
            dff = pd.read_excel(xml_df_file, converters={"EIN":str,"plannumber":str})
            dff['Lookup'] = dff['plannumber'] + dff['EIN']
            dff
            
            
            # In[14]:
            
            
            # merges a reference file of the inputs to the import log to determine success
            
            df_import_concat = dff.merge(df_import_results, how='left', on="Lookup")
            df_successful_upload = df_import_concat[df_import_concat['Result'] == "Success"]
            
            df_failed_upload = df_import_concat[df_import_concat['Result'] != "Success"]
            
            
            # In[15]:
            
            
            df_failed_upload
            failed_plan_list = df_failed_upload['planid'].tolist()
            failed_plan_list = list(set(failed_plan_list))
            len(failed_plan_list)
            df_failed_upload
            failed_plan_name_list = df_failed_upload['PlanName'].tolist()
            failed_plan_name_list = list(set(failed_plan_name_list))
            failed_plan_name_list
            
            
            # In[16]:
            
            
            df_failed_upload
            
            
            # In[24]:
            
            
            # Advances the projects that had DGEM upload issues to specialists for correction
            # you can keep parts of this or ditch it; basically it's used to advance ones that were
            # not successfully uploaded to someone for manual completion
            plan_id_set = set()
            df_error_log = pd.read_excel(error_path)
            for i in df_failed_upload.index[:]:
                planid = df_failed_upload.at[i,'planid']
                if not planid in plan_id_set:
                    print(planid)
                    plan_id_set.add(planid)
            
                    year = df_failed_upload.at[i,'year']
                    error_message = df_failed_upload.at[i,'Message']
            
                    note_text = "DGEM Upload error - " + error_message
                    project = None
                    projects = pp.get_projects_by_planid(planid,filters=f"Name eq 'Form 8955-SSA (Automated)'", expand="TaskGroups.Tasks")
                    project = [project for project in projects if project['CompletedOn'] is None]
                    if len(project) > 0:
                        project = project[0]
            
                    project_id = project['Id']
            
                    for taskgroup in project['TaskGroups']:
                        for task in taskgroup['Tasks']:
                            if task['TaskName'] == 'Completion - Form 8955-SSA' and task['DateCompleted'] is None:
                                task_item = pp.get_taskitems_by_taskid(task['Id'], filters = "ShortName eq '8955 EIN Mismatch'")[0]
                                task_item['Value'] = 'Complete'
                                pp.update_taskitem(task_item)
                                pp.override_task(task['Id'])
            
                    payload = {
                                "ProjectID": project_id, 
                                "NoteText": f"{note_text}",
                                "NoteCategoryId": 3514,
                                "ShowOnPSL": False
                                    }
            
                    x = pp.add_note(payload)
                    df_error_log.loc[df_error_log['TPA Plan ID'].astype(str).str.contains(str(planid)), 'DGEM Upload Error'] = 'Yes'
                    print(planid,error_message)
            
            
            # In[25]:
            
            
            ## Identifier download
            
            browser.get('https://dgem.asc-net.com/ascidoc/efast2/wizards/ImportIdentifiers.aspx')
            
            time.sleep(5)
            
            browser.find_element_by_css_selector('#rbWithData').click()
            browser.find_element_by_css_selector('#btnCreateTemplate').click()
            
            template_link_found = False
            
            while template_link_found == False:
                time.sleep(5)
                if "Download your template" in browser.page_source:
                    browser.find_element_by_link_text("here").click()
                    template_link_found = True
                    
            time.sleep(15)
            
            
            # In[26]:
            
            
            latest_signer_form = get_latest_file(download_path)
            
            df_signer_results = pd.read_excel(latest_signer_form, converters={'EIN':str,'PlanNumber':str,'PlanYear':str}, na_values='')
            updated_df  = pd.DataFrame()
            df_signer_results['Lookup'] = df_signer_results['PlanNumber'] + df_signer_results['EIN']
            
            target_signer_plans = dff['Lookup'].tolist()
            for index, row in df_signer_results.iterrows():
                if row['Lookup'] in target_signer_plans:
                    mask = dff['Lookup'] == row['Lookup']
                    plan_id = dff.loc[mask, 'planid'].values[0] if mask.any() else None
                    row['Identifier8955'] = plan_id
                    updated_df = pd.concat([updated_df, pd.DataFrame([row])], ignore_index=True)
            
            updated_df = updated_df.drop(columns=['Lookup'])
            new_signer_file_name = f"signersData_import_updated_{today_date}.xlsx"
            updated_df.to_excel(f'{signer_file_folder}/{new_signer_file_name}',index=False)
            just_signer_file = latest_signer_form.split('\\')[1]
            shutil.copy(latest_signer_form, f"{signer_file_folder}/{just_signer_file}")
            os.remove(latest_signer_form)
            
            
            # In[27]:
            
            
            updated_df
            
            
            # In[28]:
            
            
            # re-import updated signer information to DGEM
            
            browser.get('https://dgem.asc-net.com/ascidoc/efast2/wizards/ImportIdentifiers.aspx')
            
            time.sleep(5)
            
            id_import_path = browser.find_element(by='id',value='fileSelect').send_keys(f'{signer_file_folder}/{new_signer_file_name}')
            
            import_button = browser.find_element(by='id',value='ibtnImport').click()
            
            signer_import_success = False
            
            while signer_import_success == False:
                print('signer_import_success')
                time.sleep(5)
                if "Data was successfuly imported" in browser.page_source:
                    signer_import_success = True
                    
                    try:
                        browser.find_element_by_link_text("Download results").click()
                        time.sleep(15)
                        signer_import_log = get_latest_file(download_path)
                        just_signer_import_log = signer_import_log.split('\\')[1]
                        shutil.copy(signer_import_log, f"{signer_file_folder}/{just_signer_import_log}")
                        os.remove(signer_import_log)
                        
                    except:
                        pass
            
            
            # In[29]:
            
            
            # Download Find results
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
            
            
            # In[30]:
            
            
            ## Reads in the find results. This will let us use the names as selenium lookups
            latest_find_result = get_latest_file(download_path)
            results = pd.read_html(latest_find_result, converters={"EIN":str,"Plan #":str})
            df_results = results[0]
            
            df_results['Lookup'] = df_results['Plan #'] + df_results['EIN']
            os.remove(latest_find_result)
            df_results
            
            
            # In[31]:
            
            
            # find out if any plan data is missing in DGEM, if yes check the task item #9 for that plan
            plan_id_processed_list = set()
            for index, row in dff.iterrows():
                plan_id = row['planid']
                project_id = row['projid']
                if not plan_id in df_results['Identifier'].values:
                    if not plan_id in plan_id_processed_list:
                        plan_id_processed_list.add(plan_id)
                        print('Plan id missing in DGEM results: ', plan_id)
            
                        task_group = pp.get_task_groups_by_projectid(project_id, expand = 'Tasks.Taskitems')[0]
                        for task in task_group['Tasks']:
                            if task['TaskName'] == 'Completion - Form 8955-SSA':
                                task_id = task['Id']
                                for task_item in task['TaskItems']:
                                    if task_item['ShortName'] == '8955 Validation Error' and task_item['Value'] == None:
                                        task_item['Value'] = 'Complete'
                                        pp.update_taskitem(task_item)
                                        note_text = 'Missing data in DGEM'
                                        payload = {
                                            "ProjectID": project_id, 
                                            "NoteText": f"{note_text}",
                                            "NoteCategoryId": 3514,
                                            "ShowOnPSL": False
                                                }
            
                                        x = pp.add_note(payload)
                                    elif task_item['ShortName'] == '8955 Validation Error' and task_item['Value'] == 'Complete':
                                        pp.override_task(task_id)
                        df_error_log.loc[df_error_log['TPA Plan ID'].astype(str).str.contains(str(plan_id)), 'DGEM Data Missing'] = 'Yes'
            
            
            # In[32]:
            
            
            df_successful_upload['planname'] = df_successful_upload['planname'].apply(lambda x: saxutils.unescape(x))
            
            df_dl_vl_targets = df_results.merge(df_successful_upload, on='Lookup')
            df_dl_vl_targets
            
            
            # In[33]:
            
            
            # tries to pull list of plan names to use as lookups for remaining parts of the process
            planlist = list(set(df_dl_vl_targets['Name'].tolist()))
            print(len(planlist))
            planlist
            
            
            # In[34]:
            
            
            removelist = []
            for plan in planlist:
                plan1 = plan.rsplit(' ')[0]
                for plan_name in failed_plan_name_list:
                    plan_name1 = plan_name.rsplit(' ')[0]
                    if plan1.lower() == plan_name1.lower():
                        removelist.append(plan)
            removelist
            planlist_new = [plan for plan in planlist if plan not in removelist]
            len(planlist_new)
            
            
            # In[35]:
            
            
            new_validation_directory = f'{validation_directory}/{today_date}_Validation Logs'
            os.mkdir(new_validation_directory)
            
            
            # In[36]:
            
            
            ## Validate the 8955s before downloading
            errorlist = []
            b=0
            
            # gotta do these in batches of 100
            c = -(-(len(planlist_new)) // 100)
            
            for i in range(c):
                planlista = planlist_new[i*100:(i+1)*100]
                browser.get('https://dgem.asc-net.com/ascidoc/Find.aspx')
            
                form_type = browser.find_element(by='id',value='ddDocumentType').send_keys("8955-SSA")
                status = browser.find_element(by='id',value='dd_SSA_status').send_keys("All")
            #     checkbox = browser.find_element(by='name',value='cb_SSA_statusChangedAfter').click()
                time.sleep(5)
                checkbox = browser.find_element(by='id',value='lbtnNext_5500').click()
                time.sleep(20)    
                for plan in planlista[:]:
                    try:
                        browser.find_element(by='xpath',value='//td[contains(text(),"{}")]/ancestor::tr[1]//input[@type = "checkbox"]'.format(plan)).click()
                        b+=1
                        print(plan, b, planlist_new.index(plan))
            
                    except:
                        errorlist.append(plan)
                        continue
            
                dropdown_menu = browser.find_element(by='name',value='lbActionSSA').send_keys("Validate (5500VS Batch)")
            
                find_next = browser.find_element(by='id',value='lbtnClientNextSSA').click()
            
                time.sleep(10)
            
                email_box = browser.find_element(by='id',value='tbEmail').send_keys("automation@nova401k.com")
                email_next = browser.find_element(by='id',value='btnEmailInputNext').click()
            
                time.sleep(15)
            
            
            # In[37]:
            
            
            errorlist
            
            
            # In[38]:
            
            
            time.sleep(60)
            email_found=False
            timeout = time.time() + 60*15   # timeout 15 minutes from now
            
            while email_found is False:
                time.sleep(10)
                inbox = mailbox.inbox_folder().get_messages()
                if time.time() > timeout:
                    print("timeout")
                    break
                for message in inbox:
                    msg_sender = str(message.sender)
                    msg_time_sent = message.sent #get time message was sent
                    msg_time_sent = msg_time_sent.replace(tzinfo=None)
                    recent_email = msg_time_sent > datetime.now() - timedelta(minutes=15)
                    if (msg_sender == '8955-SSA forms validation results (support@pension-plan-emails.com)'
                        and (msg_subject := message.subject) == 'Completed Validation for the requested 8955-SSA files'
                       and recent_email is True):
                        print(msg_sender)
                        msg_body = message.body
                        regex = re.search(r'<a class="fill-div" href="([^"]+)"',msg_body)
                        pdf_download = regex.group(1)
                        message.mark_as_read()
                        browser.get(pdf_download)
                        email_found=True
            
            
            # In[39]:
            
            
            for latest_file_string in os.listdir(download_path):
                print(latest_file_string)
                shutil.copy(f"{download_path}/{latest_file_string}", f"{new_validation_directory}/{latest_file_string}")
                os.remove(f"{download_path}/{latest_file_string}")
                time.sleep(10)
            
            
            # In[40]:
            
            
            ## concatenate all the validation files just downloaded
            # you'll need to make sure this still works
            
            os.chdir(new_validation_directory)
            
            validation_files = os.listdir()
            
            b = 0
            dfv = pd.DataFrame()
            if len(validation_files) > 0:
            
                for valfile in validation_files:
                    print(valfile)
                    if b == 0:
                        b+=1
                        try:
                            valdata = pd.read_table(valfile, converters={"EIN":str,"Plan Number":str})
                            dfv1 = valdata
                            dfv1.drop(dfv1.tail(1).index,inplace=True)   
            
                        except:
                            dfv1 = pd.read_excel(valfile, converters={"EIN":str,"Plan Number":str})
                    else:
                        try:
                            valdata1 = pd.read_table(valfile, converters={"EIN":str,"Plan Number":str})
                            dfv1 = valdata1
                            dfv1.drop(dfv1.tail(1).index,inplace=True)  
            
                        except:
                            dfv1 = pd.read_excel(valfile, converters={"EIN":str,"Plan Number":str})
                    dfv = pd.concat([dfv, dfv1], ignore_index=True, sort=False)
                    print(len(dfv))
                    
            
            dfv
            
            
            # In[41]:
            
            
            file_name = f'{today_date} Validation Report.xlsx'
            file_path = f'Y:\Automation\Team Scripts\Anjana Shaji\8955 SSA DF\{file_name}' 
            dfv.to_excel(file_path)
            mail_validation_report(file_path)
            print('Email sent with validation report')
            
            
            # In[43]:
            
            
            valid_plan_list = []
            itin_error_list = []
            non_itin_error_list = []
            df_auto = pd.read_excel(xml_df_file, converters={"EIN":str,"plannumber":str})
            df_auto['planname'] = df_auto['planname'].str.upper()
            dfv['Plan Number'] = dfv['Plan Number'].str.zfill(3)
            dfv['Lookup'] = dfv['Plan Number'] + dfv['EIN']
            
            df_validation = dfv.merge(df_results, on='Lookup')
            # print(df_validation)
            df_validation['Plan Name'] = df_validation['Name']
            message = df_validation.loc[df_validation['Result'] == 'ERROR', 'Message'].values
            valid_plan_list = df_validation.loc[df_validation['Result'] == 'VALID', 'Identifier'].tolist()
            warning_list = df_validation.loc[df_validation['Message'].astype(str).str.contains('Warning: Duplicate participant SSN number: FOREIGN', na=False), 'Identifier'].tolist()
            valid_plan_list = valid_plan_list + warning_list
            itin_error_list = df_validation.loc[(df_validation['Result'] == 'ERROR') & (df_validation['Message'].astype(str).str.contains('Invalid Participant SSN')), 'Identifier'].tolist()
            non_itin_error_list = df_validation.loc[(df_validation['Result'] == 'ERROR') & (~df_validation['Message'].astype(str).str.contains('Invalid Participant SSN', na=False)) & (~df_validation['Message'].astype(str).str.contains('Warning: Duplicate participant SSN number: FOREIGN', na=False)), 'Identifier'].tolist()
            valid_plan_list = list(set(valid_plan_list))
            itin_error_list = list(set(itin_error_list))
            non_itin_error_list = list(set(non_itin_error_list))
            df_none = df_validation[df_validation['Message'].astype(str).str.contains('Warning: Duplicate participant SSN number: FOREIGN')]
            remove_plan = df_none['Identifier'].tolist()
            non_itin_error_list = [x for x in non_itin_error_list if x not in remove_plan]
            print(len(itin_error_list))
            print(len(valid_plan_list))
            print(len(non_itin_error_list))
            
            
            # In[44]:
            
            
            for plan_id in itin_error_list:
                plan_id = int(plan_id)
                print(plan_id)
                note_text = df_validation.loc[df_validation['Identifier'] == plan_id, 'Message'].values
                if len(note_text) > 30:
                    note_text = note_text[:30]
                projects = pp.get_projects_by_planid(plan_id,filters=f"Name eq 'Form 8955-SSA (Automated)'", expand="TaskGroups.Tasks")
                project = [project for project in projects if project['CompletedOn'] is None]
                if len(project) > 0:
                    project = project[0]
            
                project_id = project['Id']
            
                for taskgroup in project['TaskGroups']:
                    for task in taskgroup['Tasks']:
                        if task['TaskName'] == 'Completion - Form 8955-SSA' and task['DateCompleted'] is None:
                            task_item = pp.get_taskitems_by_taskid(task['Id'], filters = "ShortName eq '8955 Non-People Reported'")[0]
                            task_item['Value'] = 'Complete'
                            pp.update_taskitem(task_item)
                            pp.override_task(task['Id'])
            
                payload = {
                            "ProjectID": project_id, 
                            "NoteText": f"{note_text}",
                            "NoteCategoryId": 3514,
                            "ShowOnPSL": False
                                }
            
                x = pp.add_note(payload)    
                print('Note added and taskitem updated for plan: ',plan_id )
                df_error_log.loc[df_error_log['TPA Plan ID'].astype(str).str.contains(str(plan_id)), 'Validation Error'] = 'Yes'
            
            
            # In[45]:
            
            
            for plan_id in non_itin_error_list:
                plan_id = int(plan_id)
                print(plan_id)
                try:
                    note_text = df_validation.loc[df_validation['Identifier'] == plan_id, 'Message'].values
                    if len(note_text) > 30:
                        note_text = note_text[:30]
                    projects = pp.get_projects_by_planid(plan_id,filters=f"Name eq 'Form 8955-SSA (Automated)'", expand="TaskGroups.Tasks")
                    project = [project for project in projects if project['CompletedOn'] is None]
                    if len(project) > 0:
                        project = project[0]
            
                    project_id = project['Id']
            
                    for taskgroup in project['TaskGroups']:
                        for task in taskgroup['Tasks']:
                            if task['TaskName'] == 'Completion - Form 8955-SSA' and task['DateCompleted'] is None:
                                task_item = pp.get_taskitems_by_taskid(task['Id'], filters = "ShortName eq '8955 Validation Error'")[0]
                                task_item['Value'] = 'Complete'
                                pp.update_taskitem(task_item)
                                pp.override_task(task['Id'])
            
                    payload = {
                                "ProjectID": project_id, 
                                "NoteText": f"{note_text}",
                                "NoteCategoryId": 3514,
                                "ShowOnPSL": False
                                    }
            
                    x = pp.add_note(payload)
            
                    print('Note added and taskitem updated for plan: ',plan_id)
                    df_error_log.loc[df_error_log['TPA Plan ID'].astype(str).str.contains(str(plan_id)), 'Validation Error'] = 'Yes'
                except Exception as e:
                    print(e)
                    continue
            
            
            # In[46]:
            
            
            if not valid_plan_list:
                browser.quit()
                raise SystemExit("Script is shutting down since there are no valid plans to proceed")
            
            
            # In[47]:
            
            
            print(len(valid_plan_list))
            valid_plan_list
            
            
            # In[48]:
            
            
            ## Override the task
            for plan in valid_plan_list:
                try:
                    tpa_planid = int(plan)
                    print(tpa_planid)
                    projects = pp.get_projects_by_planid(tpa_planid, filters="Name eq 'Form 8955-SSA (Automated)'", expand="TaskGroups.Tasks")
                    project = [project for project in projects if project['CompletedOn'] is None]
                    if len(project) > 0:
                        project = project[0]
                    for taskgroup in project['TaskGroups']:
                        for task in taskgroup['Tasks']:
                            if task['TaskName'] == 'Completion - Form 8955-SSA' or task['TaskName'] == 'Validation Errors':
                                pp.override_task(task['Id'])
                                print(tpa_planid, task['TaskName'], "overridden")
                except Exception as e:
                    print(tpa_planid, 'error', e)
                    continue
                        
            print('Done!')
            
            
            # In[49]:
            
            
            df_error_log.to_excel(error_path, index = False)
            
            
            # In[50]:
            
            
            # quit browser when done!
            browser.quit()
            
            
            # In[51]:
            
            
            end = datetime.now()
            print(end)
            
            
            # In[52]:
            
            
            diff = end - start 
            minutes = diff.total_seconds() / 60
            print('minutes: ',minutes)
            
            
            # In[ ]:
            
            
            
            

    schedule_logger('Finished', pyfile= __file__)

except SystemExit:
    schedule_logger('Finished', pyfile= __file__)
    raise
    
except Exception as e:
    email_error_to_automation(e, __file__)
    schedule_logger('Error', pyfile= __file__)

            