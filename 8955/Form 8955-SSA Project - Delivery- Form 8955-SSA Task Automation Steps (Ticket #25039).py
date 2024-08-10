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
            

            
            # In[5]:
            
            
            import sys
            sys.path.insert(0, r'C:\Users\Public\WPy64-39100\notebooks\Anjana Shaji')
            import requests
            import pensionpro_api as pp
            import pandas as pd
            import datetime
            from glob import glob
            import time
            import json
            import os
            import shutil
            import zipfile
            from public_vault import Username, Password, OAuth
            import re
            
            from selenium import webdriver
            from selenium.webdriver.common.by import By
            from selenium.common.exceptions import NoSuchElementException
            from selenium.webdriver.support.ui import WebDriverWait
            from selenium.webdriver.support import expected_conditions as ec
            from webdriver_manager.chrome import ChromeDriverManager
            
            from webdriver_manager.chrome import ChromeDriverManager
            
            import smtplib
            from email.mime.multipart import MIMEMultipart
            from email.mime.text import MIMEText
            from email.mime.base import MIMEBase
            from email import encoders
            from pathlib import Path
            start = datetime.datetime.now()
            print(start)
            
            
            # In[7]:
            
            
            current_time = start.time()
            current_time
            five_pm = datetime.time(16, 0, 0)
            if current_time > five_pm:
                print("Current time is greater than 5 PM.")
                raise SystemExit("Script is shutting down")
            
            
            # In[4]:
            
            
            #Plan group id for 'AFS-Partner'
            plan_grp_id_list = [464823, 89373, 99916, 438223]
            today_date = datetime.date.today()
            current_year = today_date.strftime('%Y')
            today_date = datetime.datetime.strftime(today_date, "%m-%d-%Y")
            email_box = 'automation@nova401k.com'
            my_username = Username('DGEM')
            my_pw = Password('DGEM')
            user = os.getlogin()
            download_path = f'C:/Users/{user}/Downloads'
            error_path = r'Y:\Automation\Team Scripts\Anjana Shaji\8955 SSA DF\error_report.xlsx'
            no_mail_plan_list = []
            
            
            # In[ ]:
            
            
            def mail_daily_report(file_path):
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
                    subject = f'Daily 8955-SSA Daily Status Report'
                    msg['Subject'] = subject
                    part = MIMEText(html, 'html')
                    msg.attach(part)
                    
                    part1 = MIMEBase('application', "octet-stream")
                    part1.set_payload(open(file_path, "rb").read())
                    encoders.encode_base64(part1)
                    part1.add_header('Content-Disposition', 'attachment; filename= "8955 SSA Daily Status Report.xlsx"')
                    msg.attach(part1)
            
            
                    with smtplib.SMTP('smtp.office365.com', 587) as server:
                        server.starttls()
                        server.login(fromaddr, password)
                        x = server.sendmail(fromaddr, toaddrs, msg.as_string())
            
            
            # In[5]:
            
            
            def send_email(plan_name, salutation, toaddr, cc_list, am_sig, year):
            
                html_head = """
                <html xmlns:v="urn:schemas-microsoft-com:vml" xmlns:o="urn:schemas-microsoft-com:office:office" xmlns:w="urn:schemas-microsoft-com:office:word" xmlns:m="http://schemas.microsoft.com/office/2004/12/omml" xmlns="http://www.w3.org/TR/REC-html40">
                <head><META HTTP-EQUIV="Content-Type" CONTENT="text/html; charset=us-ascii"><meta name=Generator content="Microsoft Word 15 (filtered medium)">
                </head>"""
                
                
                html_body = f"""
                <body lang=EN-US link="#0563C1" vlink="#954F72" style='word-wrap:break-word'><div class=WordSection1>
                Dear {salutation},<br>
                We have prepared the {year} Form 8955-SSA for your review.<br>
                This form is used to report plan benefits (or former plan benefits) for two categories of employees to the Social Security Administration: <br>
                * Employees coded as "A" are those who terminated on or before the last date of the prior plan year, but still maintained a plan balance as of the current plan year we are filing. For example, the 12/31/23 8955-SSA would be reporting any 2022 terminated employees with balances as of 12/31/23. <br>
                * Employees coded as "D" are those who were reported on a prior year's Form 8955-SSA and took a full distribution during the current Plan Year.<br>
            	<b>ACTION REQUIRED</b><br>
            	Please log on to our secure web portal to download a copy of the Form 8955-SSA for your review within 3 business days. This form is located in the ‘My Active Tasks’; once the form has been accessed, it will appear under the ‘Documents’ tab.  If there are no changes, have the plan sponsor and plan administrator sign the Form 8955-SSA and place the signed Form 8955-SSA with the Plan's permanent records; no further action is required on your behalf. If changes are needed, please contact me immediately.<br>
            	You may access the form by logging in and selecting the Document Tab at the top:<br>
            	<ul type = "1"><li>Log in to PlanSponsorLink through <a href="https://nova401k.plansponsorlink.com/">https://nova401k.plansponsorlink.com/</a> .</li><li>Login Instructions: Your Username is your email address. First time users should click on the 'First time user?' link and enter your email address. PlanSponsorLink will automatically confirm your email address and email you a password.</li></ul>
            	Nova 401(k) Associates will electronically file your Form 8955-SSA with the Internal Revenue Service shortly after the 3 business days have lapsed.  Changes requested after the date are subject to additional fees if an amended filing is required.  <br>
                If there are any questions, please do not hesitate to contact me.<br>
                This email was automatically generated.<br>
                {am_sig}</div></body></html>
                """.replace('\n','<br>')
            
                html = html_head + html_body
            
                fromaddr = 'clientrelations@nova401k.com'
                password = 'Pas25793'
                # toaddr = ['ashaji@nova401k.com']
                # cc_list = []
                toaddr = [address for address in toaddr if address != None]
                cc_list = [address for address in cc_list if address != None]
            
                toaddrs = toaddr + cc_list
            
                if len(toaddrs) == 0:
                    x = "No emails to send"
                else:
                    msg = MIMEMultipart('alternative')
            
                    msg['From'] = fromaddr
                    msg['To'] = ','.join(toaddr)
                    msg['CC'] = ','.join(cc_list)
                    subject = f'IMPORTANT: {year} Form 8955-SSA Now Available For Your Review - {plan_name}'
                    msg['Subject'] = subject
                    part = MIMEText(html, 'html')
                    msg.attach(part)
            
                    with smtplib.SMTP('smtp.office365.com', 587) as server:
                        server.starttls()
                        server.login(fromaddr, password)
                        x = server.sendmail(fromaddr, toaddrs, msg.as_string())
                return toaddr, cc_list, html_body, subject
            
            
            # In[6]:
            
            
            def setup_send_mail(plan_id, project_id, signatures, year, afs_flag):
                
                internal_plan_id = pp.get_sysplanid(plan_id)
                plan_name = pp.get_plan_by_planid(internal_plan_id)['Name']
                contacts = pp.get_plan_contact_roles_by_planid(plan_id)
                employees = pp.get_employee_plan_roles_by_planid(plan_id)
                
                primary_contacts = [contact for contact in contacts if contact['RoleType']['DisplayName'] == 'Primary Contact']
                primary_brokers = [contact for contact in contacts if contact['RoleType']['DisplayName'] == 'Primary Broker']
                three_sixteen_admins= [contact for contact in contacts if contact['RoleType']['DisplayName'] == '3(16) Administrator']
                account_managers = [employee for employee in employees if employee['RoleType']['DisplayName'] == 'Administrator']
                
                if primary_brokers:
                    primary_broker = primary_brokers[0]
                    primary_broker_id = primary_broker.get('Contact', {}).get('Id', None)
                    pbroker_email = [broker['Contact']['Email'] for broker in primary_brokers]
                else:
                    pbroker_email, primary_broker_id = [[], None]
                if three_sixteen_admins:
                    three_sixteen_admin = three_sixteen_admins[0]
                    three_sixteen_admin_email = [three_sixteen['Contact']['Email'] for three_sixteen in three_sixteen_admins]
                    three_sixteen_admin_id = three_sixteen_admin.get('Contact', {}).get('Id', None)
                    three_sixteen_admin_name = three_sixteen_admin.get('Contact', {}).get('FirstName', None)
                    three_sixteen_admin_name_last = three_sixteen_admin.get('Contact', {}).get('LastName', None)
                    three_six_admin = f'{three_sixteen_admin_name} {three_sixteen_admin_name_last}'
                    t_salutation = three_sixteen_admin.get('Contact', {}).get('Salutation', None)
                    if t_salutation == None:
                        t_salutation = three_sixteen_admin_name
                else:
                    three_sixteen_admin_email, three_six_admin, three_sixteen_admin_id = [[], None, None]
                if primary_contacts:
                    primary_contact = primary_contacts[0]
                    primary_contact_email = [primary['Contact']['Email'] for primary in primary_contacts]
                    primary_contact_id = primary_contact.get('Contact', {}).get('Id', None)
                    p_salutation = primary_contact.get('Contact', {}).get('Salutation', None)
                    if p_salutation == None:
                        p_salutation = primary_contact.get('Contact', {}).get('FirstName', None)
                else:
                    primary_contact_email, primary_contact_id, p_salutation = [[], None, None]
                if account_managers:
                    account_manager = account_managers[0]
                    admin_email = [account_manager.get('Contact', {}).get('Email', None)]
                    admin_email_sig = admin_email[0]
                    admin_name = account_manager.get('Contact', {}).get('FirstName', None)
                    admin_name_last = account_manager.get('Contact', {}).get('LastName', None)
                    admin_id = account_manager.get('ContactId')
                    new_sig = ''
                    if [signature['Signature'] for signature in signatures if signature['ContactId'] == admin_id][0] == None:
                        admin_sig = admin_name+' '+admin_name_last
                        new_sig = admin_sig +'\r\n'+ admin_email_sig
                    else:
                        admin_sig = [signature['Signature'] for signature in signatures if signature['ContactId'] == admin_id][0].split("\r\n")
                    
                        for line in admin_sig:
                            new_sig = new_sig + line + '\r\n'
                else:
                    admin_email = []
                    admin_id = None
                new_sig = new_sig.replace("&#10;","\r\n").replace("&#160;"," ").replace("\r\n","<br>").replace("\n","")
                try:
                    if afs_flag:
                        toaddr = three_sixteen_admin_email
                        cc_list = primary_contact_email + pbroker_email
                        salutation = t_salutation
                        email_output = send_email(plan_name, salutation, toaddr, cc_list, new_sig, year)
                        print(f'Email sent for planid {plan_id}!')
                    else:
                        toaddr = primary_contact_email
                        cc_list = pbroker_email
                        salutation = p_salutation
                        email_output = send_email(plan_name, salutation, toaddr, cc_list, new_sig, year)
                        print(f'Email sent for planid {plan_id}!')
                except Exception as e:
                    print(e)
                    print(f'Email not sent for planid {plan_id}!')
                    return True
            
                to_list = email_output[0]
                cc_list = email_output[1]
                html_body = email_output[2]
                subject = email_output[3]
            
                note_text = f"""To: {to_list}
                    CC: {cc_list}
                    Subject: {subject}
                    Email: {html_body}
                    """
                note_text = note_text[:2000]
                payload = {
                "ProjectID": f"{project_id}", 
                "NoteText": f"{note_text}",
                "ShowOnPSL": False
                        }
            
                x = pp.add_note(payload)
                print(f"Email for {plan_id} has been copied into the project notes!")
            
            
            # In[7]:
            
            
            def upload_file_to_pro(plan_id, project_id,period_end_date, final_pdf, file_path):
                # Add file to project. ProjectFileTypeId is 583 which is 'Form 8955-SSA'
                print(final_pdf)
                title = final_pdf
                try:
                    pp.add_project_file(file_path, project_id, ProjectFileTypeId=583, ShowOnWeb=True, Title= title, Comment='Form 8955-SSA for review', Archived=False, HasBeenWarned=False, EffectiveOn = period_end_date)
                    print(f'File added to PRO for plan {plan_id}.')    
                except Exception as e:
                    print(e)
                    print('No email plan: ',plan_id)
                    no_mail_plan_list.append(plan_id)
                    files = pp.get_project_files_by_projectid(project_id)
                    file = files[0]
                    file['ShowOnWeb'] = False
                    pp.update_project_file(file)
                    title = final_pdf.rsplit('.')[0]
                    title = f'{title}2.pdf'
                    print('Title 2: ', title)
                    pp.add_project_file(file_path, project_id, ProjectFileTypeId=583, ShowOnWeb=True, Title= title, Comment='Form 8955-SSA for review', Archived=False, HasBeenWarned=False, EffectiveOn = period_end_date)
                    error_message = str(e)
            
            
            # In[8]:
            
            
            def get_latest_file(directory):
                ## Reads in the find results. This will let us use the names as selenium lookups
            
                fr_files = [os.path.join(directory, file) for file in os.listdir(directory) if os.path.isfile(os.path.join(directory, file))]
            
                # Sort the files based on modification time (latest first)
                sorted_fr_files = sorted(fr_files, key=lambda x: os.path.getmtime(x), reverse=True)
            
                # Get the latest modified file
                latest_find_file = sorted_fr_files[0] if sorted_fr_files else ""
            
                # Convert the latest_file path to string
                return str(latest_find_file)
            
            
            # In[9]:
            
            
            df = pp.get_worktray2('Automation', get_all=True)
            filt1 = df['task_name'] == 'Delivery of Form 8955-SSA'
            filt2 = df['proj_name'] == 'Form 8955-SSA (Automated)'
            df = df[filt1 & filt2]
            len(df)
            
            
            # In[10]:
            
            
            df
            
            
            # In[11]:
            
            
            now = datetime.datetime.now()
            previous_year = str(int(now.strftime("%Y"))-1)
            o365_directory = Path(r'C:\Users\Public\WPy64-39100\notebooks\scheduler')
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
            
            
            # In[12]:
            
            
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
            
            
            # In[13]:
            
            
            ## Reads in the find results. This will let us use the names as selenium lookups
            latest_find_result = get_latest_file(download_path)
            results = pd.read_html(latest_find_result, converters={"EIN":str,"Plan #":str})
            df_results = results[0]
            df_results['Identifier'] = df_results['Identifier'].fillna(0)
            df_results['Identifier'] = df_results['Identifier'].astype(int)
            df_results['Lookup'] = df_results['Plan #'] + df_results['EIN']
            os.remove(latest_find_result)
            
            df_results
            
            
            # In[14]:
            
            
            valid_plan_list = []
            planlist = list(set(df_results['Identifier'].tolist()))
            for index, row in df.iterrows():
                plan_id = int(row['planid'])
                try:
                    plan_name = df_results.loc[df_results['Identifier'] == plan_id, 'Name'].iloc[0]
                except Exception as e:
                    print(e)
                valid_plan_list.append(plan_name)
            print(len(valid_plan_list))
            valid_plan_list
            
            
            # In[19]:
            
            
            # Download the batch file for 8955
            # Request PDFs to be emailed to automation@nova401k.com
            
            browser.get('https://dgem.asc-net.com/ascidoc/Find.aspx')
            
            form_type = browser.find_element(by='id',value='ddDocumentType').send_keys("8955-SSA")
            time.sleep(2)
            status = browser.find_element(by='id',value='dd_SSA_status').send_keys("All")
            # checkbox = browser.find_element(by='name',value='cb_SSA_statusChangedAfter').click()
            
            time.sleep(5)
            checkbox = browser.find_element(by='id',value='lbtnNext_5500').click()
            
            
            # In[20]:
            
            
            time.sleep(10)
            b=0
            errorlist = []
            for plan in valid_plan_list[b:]:
                print(plan)
            #     plan = plan.upper()
                try:
                    browser.find_element(by='xpath',value=f'//td[contains(text(),"{plan}")]/ancestor::tr[1]//input[@type = "checkbox"]').click()
                    b+=1
                    print(plan, b, valid_plan_list.index(plan))
            
                except:
                    errorlist.append(plan)
                    continue
            
            dropdown_menu = browser.find_element(by='name',value='lbActionSSA').send_keys("View PDF (5500VS Batch)")
            find_next = browser.find_element(by='id',value='lbtnClientNextSSA').click()
            
            time.sleep(10)
            
            email_box = browser.find_element(by='id',value='tbEmail').send_keys("automation@nova401k.com")
            email_next = browser.find_element(by='id',value='btnEmailInputNext').click()
            
            
            # In[21]:
            
            
            # part to get PDF zip file from the email.
            
            time.sleep(60)
            
            email_found=False
            timeout = time.time() + 60*15   # timeout 15 minutes from now
            from O365 import Account, FileSystemTokenBackend
            
            credentials = OAuth('o365_client_id'), OAuth('o365_secret_value')
            
            token_backend = FileSystemTokenBackend(token_path=o365_directory,
                                                   token_filename='oauth_token.txt')
            account = Account(credentials,
                              token_backend=token_backend,
                              scopes = ['basic','message_all','mailbox'])
            
            if not account.is_authenticated:
                account.authenticate()
                
            mailbox = account.mailbox(resource=email_box)
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
                    recent_email = msg_time_sent > datetime.datetime.now() - datetime.timedelta(minutes=10)
                    if (msg_sender == '8955-SSA PDFs (support@pension-plan-emails.com)'
                        and (msg_subject := message.subject) == 'Completed Batch PDF creation for 8955-SSA forms'
                       and recent_email is True):
                        msg_body = message.body
                        regex = re.search(r'<a class="fill-div" href="([^"]+)"',msg_body) # find sendgrid url
                        pdf_download = regex.group(1)
                        message.mark_as_read()
                        email_found=True
                        break
                        
            if email_found is True:       
                browser.get(pdf_download)
                print('Batch downloaded')
            
            else:
                raise Exception("No email found")
            
            
            time.sleep(10)
            
            
            # In[16]:
            
            
            ## Extract the zip files
            
            # Get latest file in dl directory
            latest_file_string = get_latest_file(download_path)
            print(latest_file_string)
            
            try:
                dirname = latest_file_string.split(".zip")[0]
                print(dirname)
                
            except:
                raise Exception("No zip file found")
                
            with zipfile.ZipFile(latest_file_string, 'r') as zip_ref:
                zip_ref.extractall(dirname)
            time.sleep(20)
            os.remove(latest_file_string)
            unzipped_pdfs = os.listdir(dirname)
            for file in unzipped_pdfs:
                shutil.copy(f"{dirname}/{file}", f'Y:/ASC/Exported Reports/8955 Automation/PDF Downloads/{file}')
                os.remove(f"{dirname}/{file}")
                            
            os.removedirs(dirname)
            
            
            # In[17]:
            
            
            browser.quit()
            
            
            # In[18]:
            
            
            # Rename the 8955 forms
            
            pdf_root = f'Y:/8955/{current_year}/Automation/SSA Production/PDF Downloads'
            os.chdir('Y:/ASC/Exported Reports/8955 Automation/PDF Downloads')
            pdf_folder = os.listdir()
            pdf_folder = [file for file in pdf_folder if file.endswith(".pdf")]
            
            pdf_folder = ([file for file in pdf_folder if 'FormSSA_' in file and not '__' in file and not 'Identifier' in file])
            if len(pdf_folder) > 0:        
                for file in pdf_folder:
                    tpa_planid = file.split("_")[1]
                    if "8955SSA.pdf" not in file: # update filenames here
                        try:
                            os.rename(file, f"{tpa_planid}_8955SSA.pdf")
                
                        except Exception as e:
                            print(e)
                
                
            print('Done!')
            
            # get folder contents again after renaming
            pdf_folder = os.listdir()
            pdf_folder = [file for file in pdf_folder if file.endswith(".pdf")]
            target_files = ([file for file in pdf_folder if not '__' in file and not 'Identifier' in file])
            final_pdfs = target_files[:]
            len(final_pdfs)
            
            
            # In[19]:
            
            
            #create a current year folder to save the downloaded PDFs.
            parent_dir = 'Y:/8955'
            directory = f'{current_year}/Automation/SSA Production/PDF Downloads/Dated Form Downloads/{today_date} PDF Downloads'
            save_pdf_path = os.path.join(parent_dir, directory)
            try: 
                os.makedirs(save_pdf_path, exist_ok = True) 
                print("Directory '%s' created successfully" % directory) 
            except OSError as error: 
                print("Directory '%s' can not be created" % directory) 
            save_pdf_path
            
            
            # In[20]:
            
            
            print(len(final_pdfs))
            
            
            # In[21]:
            
            
            source = r'Y:\ASC\Exported Reports\8955 Automation\PDF Downloads'
            os.chdir('Y:/ASC/Exported Reports/8955 Automation/PDF Downloads')
            df_error_log = pd.read_excel(error_path)
            for file in final_pdfs:
                plan_id = file.split('_')[0]
                try:
                    move_directory = r'G:'
                    client_folder = ''
                    pdf_folder = os.listdir()
                    file_string = f'{plan_id}_'
                    pdf_folder = [file for file in pdf_folder if file.endswith(".pdf")]
                    target_files = ([file for file in pdf_folder if not '__' in file and not 'Identifier' in file and file_string in file])
                    final_pdf = target_files[0]
                    print(final_pdf)
                    projects = pp.get_projects_by_planid(plan_id, filters="Name eq 'Form 8955-SSA (Automated)'")
                    project = [project for project in projects if project['CompletedOn'] is None]
                    if len(project) > 0:
                        project = project[0]
                    period_end_date = project['PeriodEnd']
                    project_id = project['Id']
                    project_start_date = project['PeriodStart']
                    project_start_year = project_start_date.rsplit(' ')[0].rsplit('/')[2]
                    file_path = f'{source}\{final_pdf}'
                    upload_file_to_pro(plan_id, project_id, period_end_date, final_pdf, file_path)
                    for folder in os.listdir(move_directory):
                        if folder.split()[0] == plan_id:
                            client_folder = folder
                    source_path = os.path.join(source, final_pdf)
                    destination_folder = f'{move_directory}\{client_folder}\\{project_start_year}\\5500\\8955'
                    if not os.path.exists(destination_folder):
                        os.makedirs(destination_folder)
                    print(destination_folder)
                    destination_path = os.path.join(destination_folder, final_pdf)
                    shutil.copyfile(source_path, destination_path)
                    shutil.copyfile(source_path, f'{save_pdf_path}\{final_pdf}')
                    time.sleep(10)
                    os.remove(final_pdf)
                    df_error_log.loc[df_error_log['TPA Plan ID'].astype(str).str.contains(str(plan_id)), 'Form Downloaded'] = 'Yes'
                except Exception as e:
                    print(plan_id, 'error', e)
            print('Done!')
            
            
            # In[22]:
            
            
            no_mail_plan_list
            
            
            # In[23]:
            
            
            signatures = pp.get_employees(filters=None)
            
            if df.empty:
                raise SystemExit("Script is shutting down")
            for index, row in df.iterrows():
                plan_id = row['planid']
                task_id = row['taskid']
                if not plan_id in no_mail_plan_list:
                    starts_with_plan = any(s.startswith(plan_id) for s in final_pdfs)
                    if starts_with_plan:
                        project_id = row['projid']
                        print(plan_id)
                        afs_flag = False
                        plan_data = pp.get_plan_by_tpaplanid(plan_id)
                        if plan_data['PlanGroupId'] in plan_grp_id_list:
                            print('Plan has AFS grouping')
                            afs_flag = True
                        period_start_date = pp.get_project_by_projectid(project_id)['PeriodStart']
                        period_start_date = datetime.datetime.strptime(period_start_date, "%m/%d/%Y %I:%M:%S %p")
                        year = str(period_start_date.year)
                        try:
                            setup_send_mail(plan_id, project_id, signatures, year, afs_flag)
                            pp.override_task(task_id)
                            df_error_log.loc[df_error_log['TPA Plan ID'].astype(str).str.contains(str(plan_id)), 'Delivered to Client'] = 'Yes'
                        except Exception as e:
                            print(e)
                            continue
                    else:
                        print('no file for plan: ', plan_id)
                else:
                    print('plan in no mail, ', plan_id)
                    pp.override_task(task_id)
            df_error_log.to_excel(error_path, index = False)    
            print('Done!')
            
            
            # In[24]:
            
            
            df = pp.get_worktray2('Automation', get_all=True)
            filt1 = df['task_name'] == 'Client corrections'
            filt2 = df['proj_name'] == 'Form 8955-SSA (Automated)'
            df = df[filt1 & filt2]
            
            if df.empty:
                raise SystemExit("Script is shutting down")
            for index, row in df.iterrows():
                plan_id = row['planid']
                project_id = row['projid']
                task_id = row['taskid']
                task_id_list = []
                override_flag = False
                print(plan_id)
                tasks = pp.get_task_groups_by_projectid(project_id, expand = 'Tasks')
                for task_group in tasks:
                    if task_group['Name'] == 'Automated 8955-SSA Tasks':
                        for task in task_group['Tasks']:
                            if task['TaskName'] == 'Client corrections' or task['TaskName'] == 'Review Corrected Form 8955-SSA' or task['TaskName'] == 'Delivery of Corrected Form 8955-SSA':
                                task_id_list.append(task['Id'])
                                if task['TaskName'] == 'Client corrections' and task['AssignedToId'] == 834602:
                                    override_flag = True
                print(override_flag)
                if override_flag:
                    for task_id in task_id_list:
                        pp.override_task(task_id)
                        print('Task overridden for plan: ',plan_id)
            print('Done!')
            
            
            # In[ ]:
            
            
            mail_daily_report(error_path)
            
            
            # In[25]:
            
            
            end = datetime.datetime.now()
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

            