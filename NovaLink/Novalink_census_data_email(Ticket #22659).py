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
            import datetime as dt
            from datetime import datetime
            from dateutil.relativedelta import relativedelta
            import os
            import requests
            import json
            
            import pandas as pd
            
            import pensionpro_api as pp
            
            import smtplib
            from email.mime.multipart import MIMEMultipart
            from email.mime.text import MIMEText
            from email.mime.base import MIMEBase
            from email import encoders
            
            
            # In[5]:
            
            
            
            def send_email(plan_name, salutation, primary_contact_email, pbroker_email, census_contact_email, payroll_contact_email, novalink_contact_email, admin_phone_number, admin_email, am_sig):
                
                html_head = """
                <html xmlns:v="urn:schemas-microsoft-com:vml" xmlns:o="urn:schemas-microsoft-com:office:office" xmlns:w="urn:schemas-microsoft-com:office:word" xmlns:m="http://schemas.microsoft.com/office/2004/12/omml" xmlns="http://www.w3.org/TR/REC-html40"><head><META HTTP-EQUIV="Content-Type" CONTENT="text/html; charset=us-ascii"><meta name=Generator content="Microsoft Word 15 (filtered medium)"><style><!--
                /* Font Definitions */
                @font-face
                    {font-family:Wingdings;
                    panose-1:5 0 0 0 0 0 0 0 0 0;}
                @font-face
                    {font-family:"Cambria Math";
                    panose-1:2 4 5 3 5 4 6 3 2 4;}
                @font-face
                    {font-family:Calibri;
                    panose-1:2 15 5 2 2 2 4 3 2 4;}
                /* Style Definitions */
                p.MsoNormal, li.MsoNormal, div.MsoNormal
                    {margin:0in;
                    font-size:11.0pt;
                    font-family:"Calibri",sans-serif;}
                a:link, span.MsoHyperlink
                    {mso-style-priority:99;
                    color:#0563C1;
                    text-decoration:underline;}
                span.EmailStyle17
                    {mso-style-type:personal-compose;
                    font-family:"Calibri",sans-serif;
                    color:windowtext;}
                .MsoChpDefault
                    {mso-style-type:export-only;
                    font-family:"Calibri",sans-serif;}
                @page WordSection1
                    {size:8.5in 11.0in;
                    margin:1.0in 1.0in 1.0in 1.0in;}
                div.WordSection1
                    {page:WordSection1;}
                /* List Definitions */
                @list l0
                    {mso-list-id:591285130;
                    mso-list-template-ids:-2043657902;}
                @list l0:level1
                    {mso-level-tab-stop:.5in;
                    mso-level-number-position:left;
                    text-indent:-.25in;}
                @list l0:level2
                    {mso-level-tab-stop:1.0in;
                    mso-level-number-position:left;
                    text-indent:-.25in;}
                @list l0:level3
                    {mso-level-tab-stop:1.5in;
                    mso-level-number-position:left;
                    text-indent:-.25in;}
                @list l0:level4
                    {mso-level-tab-stop:2.0in;
                    mso-level-number-position:left;
                    text-indent:-.25in;}
                @list l0:level5
                    {mso-level-tab-stop:2.5in;
                    mso-level-number-position:left;
                    text-indent:-.25in;}
                @list l0:level6
                    {mso-level-tab-stop:3.0in;
                    mso-level-number-position:left;
                    text-indent:-.25in;}
                @list l0:level7
                    {mso-level-tab-stop:3.5in;
                    mso-level-number-position:left;
                    text-indent:-.25in;}
                @list l0:level8
                    {mso-level-tab-stop:4.0in;
                    mso-level-number-position:left;
                    text-indent:-.25in;}
                @list l0:level9
                    {mso-level-tab-stop:4.5in;
                    mso-level-number-position:left;
                    text-indent:-.25in;}
                @list l1
                    {mso-list-id:813722724;
                    mso-list-type:hybrid;
                    mso-list-template-ids:775449646 67698689 67698691 67698693 67698689 67698691 67698693 67698689 67698691 67698693;}
                @list l1:level1
                    {mso-level-number-format:bullet;
                    mso-level-text:\F0B7;
                    mso-level-tab-stop:none;
                    mso-level-number-position:left;
                    text-indent:-.25in;
                    font-family:Symbol;}
                @list l1:level2
                    {mso-level-number-format:bullet;
                    mso-level-text:o;
                    mso-level-tab-stop:none;
                    mso-level-number-position:left;
                    text-indent:-.25in;
                    font-family:"Courier New";}
                @list l1:level3
                    {mso-level-number-format:bullet;
                    mso-level-text:\F0A7;
                    mso-level-tab-stop:none;
                    mso-level-number-position:left;
                    text-indent:-.25in;
                    font-family:Wingdings;}
                @list l1:level4
                    {mso-level-number-format:bullet;
                    mso-level-text:\F0B7;
                    mso-level-tab-stop:none;
                    mso-level-number-position:left;
                    text-indent:-.25in;
                    font-family:Symbol;}
                @list l1:level5
                    {mso-level-number-format:bullet;
                    mso-level-text:o;
                    mso-level-tab-stop:none;
                    mso-level-number-position:left;
                    text-indent:-.25in;
                    font-family:"Courier New";}
                @list l1:level6
                    {mso-level-number-format:bullet;
                    mso-level-text:\F0A7;
                    mso-level-tab-stop:none;
                    mso-level-number-position:left;
                    text-indent:-.25in;
                    font-family:Wingdings;}
                @list l1:level7
                    {mso-level-number-format:bullet;
                    mso-level-text:\F0B7;
                    mso-level-tab-stop:none;
                    mso-level-number-position:left;
                    text-indent:-.25in;
                    font-family:Symbol;}
                @list l1:level8
                    {mso-level-number-format:bullet;
                    mso-level-text:o;
                    mso-level-tab-stop:none;
                    mso-level-number-position:left;
                    text-indent:-.25in;
                    font-family:"Courier New";}
                @list l1:level9
                    {mso-level-number-format:bullet;
                    mso-level-text:\F0A7;
                    mso-level-tab-stop:none;
                    mso-level-number-position:left;
                    text-indent:-.25in;
                    font-family:Wingdings;}
                ol
                    {margin-bottom:0in;}
                ul
                    {margin-bottom:0in;}
                --></style><!--[if gte mso 9]><xml>
                <o:shapedefaults v:ext="edit" spidmax="1026" />
                </xml><![endif]--><!--[if gte mso 9]><xml>
                <o:shapelayout v:ext="edit">
                <o:idmap v:ext="edit" data="1" />
                </o:shapelayout></xml><![endif]--></head>"""
                
                
                html_body = f"""
                <body lang=EN-US link="#0563C1" vlink="#954F72" style='word-wrap:break-word'><div class=WordSection1>
                <p class=MsoNormal>Dear {salutation},<o:p></o:p></p>
                <p class=MsoNormal>Novalink has prepared the annual census file for your plan, and it has been uploaded to <a href="https://plansponsorlink.com/nova401k/login?returnUrl=%2Fnova401k%2F">PlanSponsorLink</a>.<o:p></o:p></p>
                <p class=MsoNormal><b>ACTION REQUIRED:</b> Please log in to <a href="https://plansponsorlink.com/nova401k/login?returnUrl=%2Fnova401k%2F">PlanSponsorLink</a> as soon as possible to review the census file that has been uploaded by Novalink. You will also need to complete all remaining sections of the data collection including the final approval step. <o:p></o:p></p>
                <p class=MsoNormal>If there are any errors on the census, or you have any questions, please reach out to me <b>before</b> you make any changes to the census. <o:p></o:p></p>
                <p class=MsoNormal>Please pay special attention to the following items on the census:<o:p></o:p></p>
                <p class=MsoNormal><ul><li>Date of termination- If an employee is terminated, be sure their termination date is populated on the census. In the future, when an employee terminates, update their status in your payroll system immediately after their last pay date.</li> <li>Date of rehire- If an employee is rehired, be sure the original date of hire, date of termination and date of rehire are all populated on the census.</li><li>Employee involuntarily separated- Answer “Y” if the employee was involuntarily terminated or “N” if the employee was not involuntarily terminated. </li><li>Separation due to death, disability, or retirement- Indicate “Y” or “N” if death, disability or retirement applies. </li><li>Severance Compensation- Ensure severance pay (if applicable) is accurately reported in the severance compensation column. </li><li>Employer contributions- Verify the employer contributions (if any) reported are only applicable to the current plan year and do not include amounts due from the prior year.</li><li>Hours- Hours worked must be reported for every employee for whom you track hours. Many payroll systems can default hours for salaried/full-time employees. Contact your payroll provider to confirm if this is available and update accordingly so hours will populate in the future.</li><li>If you started using this payroll provider after the first of the year, please ensure the data reported includes information for the entire year.</li></ul></p>
                <p class=MsoNormal>Please understand that both the census <b>and</b> all other sections of the data collection must be completed before your annual testing and Form 5500 preparation can commence. <o:p></o:p></p>
                <p class=MsoNormal>Thank you for the opportunity to assist you with the administration of your retirement plan. If you have any questions, please contact me at {admin_phone_number} or {admin_email}.<o:p></o:p></p>
                <p class=MsoNormal>Sincerely,<o:p></o:p></p>
                <p class=MsoNormal>{am_sig}<o:p></o:p></p></div></body></html>
                """.replace('\n','<br>')
            
            
                toaddr = [primary_contact_email, census_contact_email]
                cc_list = [pbroker_email, payroll_contact_email, novalink_contact_email]
                        
                html = html_head + html_body
            
                fromaddr = 'novalink@nova401k.com'
                password = 'Xuc51361'
            
                toaddr = [address for address in toaddr if address != None]
                cc_list = [address for address in cc_list if address != None]
                toaddrs = toaddr + cc_list
                for add in toaddrs:
                    print(add)
                if len(toaddrs) == 0:
                    x = "No emails to send"
                else:
                    msg = MIMEMultipart('alternative')
            
                    msg['From'] = fromaddr
                    msg['To'] = ','.join(toaddr)
                    msg['CC'] = ','.join(cc_list)
                    subject = f'ACTION REQUIRED- Annual Census File has Been Uploaded for {plan_name}'
                    msg['Subject'] = subject
            
                    part = MIMEText(html, 'html')
                    msg.attach(part)
                    with smtplib.SMTP('smtp.office365.com', 587) as server:
                        server.starttls()
                        server.login(fromaddr, password)
                        x = server.sendmail(fromaddr, toaddrs, msg.as_string())
                        
                return x, toaddr, cc_list, html_body, subject
            
            
            # In[ ]:
            
            
            def get_worktray():
                df = pp.get_worktray2("Novalink",get_all=True)
                filt1 = df['task_name'] == 'Census Loaded Notification'
                filt2 = df['proj_name'] == 'Novalink Census Upload'
                df = df[filt1 & filt2]
                return df
            
            
            # In[ ]:
            
            
            def get_active_records():
                signatures = pp.get_employees(filters=None)
                df = get_worktray()
                if df.empty:
                    raise SystemExit("Script is shutting down")
                for index, row in df.iterrows():
                    plan_id = row['planid']
                    print(plan_id)
                    project_id = row['projid']
                    task_id = row['taskid']
                    internal_plan_id = pp.get_sysplanid(plan_id)
                    plan_name = pp.get_plan_by_planid(internal_plan_id)['Name']
                    contacts = pp.get_plan_contact_roles_by_planid(plan_id)
                    employees = pp.get_employee_plan_roles_by_planid(plan_id)
                    
                    census_contact = [contact for contact in contacts if contact['RoleType']['DisplayName'] == 'Census Contact']
                    payroll_contact = [contact for contact in contacts if contact['RoleType']['DisplayName'] == 'Payroll Contact']
                    primary_contact = [contact for contact in contacts if contact['RoleType']['DisplayName'] == 'Primary Contact']
                    novalink_payroll_contact = [contact for contact in contacts if contact['RoleType']['DisplayName'] == 'Novalink Payroll Contact']
                    primary_brokers = [contact for contact in contacts if contact['RoleType']['DisplayName'] == 'Primary Broker']
                    account_managers = [employee for employee in employees if employee['RoleType']['DisplayName'] == 'Administrator']
            
                    if census_contact:
                        census_contact = census_contact[0]
                        census_contact_email = census_contact.get('Contact', {}).get('Email', None)
                    else:
                        census_contact_email = None
                    if payroll_contact:
                        payroll_contact = payroll_contact[0]
                        payroll_contact_email = payroll_contact.get('Contact', {}).get('Email', None)
                    else:
                        payroll_contact_email = None
                    if primary_contact:
                        primary_contact = primary_contact[0]
                        primary_contact_email = primary_contact.get('Contact', {}).get('Email', None)
                        salutation = primary_contact.get('Contact', {}).get('Salutation', None)
                    else:
                        primary_contact_email, salutation = [None, None]
                    if  primary_brokers:
                        primary_broker = primary_brokers[0]
                        pbroker_email = primary_broker.get('Contact', {}).get('Email', None)
                    else:
                        pbroker_email = None
                    if novalink_payroll_contact:
                        novalink_payroll_contact = novalink_payroll_contact[0]      
                        novalink_contact_email = novalink_payroll_contact.get('Contact', {}).get('Email', None)
                    else:
                        novalink_contact_email = None
                    
                    if len(account_managers) != 0:
                        account_manager = account_managers[0]
                        admin_email = account_manager.get('Contact', {}).get('Email', None)
                        admin_name = account_manager.get('Contact', {}).get('FirstName', None)
                        admin_name_last = account_manager.get('Contact', {}).get('LastName', None)
                        admin_id = account_manager.get('ContactId')
                        try:
                            admin_data = pp.get_phone_number_by_contactid(admin_id)[0]['PhoneNumber']
                        except Exception as e:
                            print(e)
                            continue
                        admin_phone_number = admin_data['Number']
                        if admin_phone_number:
                            if len(admin_phone_number) == 10 and not '-' in admin_phone_number:
                                admin_phone_number = format(int(admin_phone_number[:-1]), ",").replace(",", "-") + admin_phone_number[-1]
                            
                        else:
                            admin_phone_number =''
                        
                        new_sig = ''
                        if [signature['Signature'] for signature in signatures if signature['ContactId'] == admin_id][0] == None:
                            admin_sig = admin_name+' '+admin_name_last
                            new_sig = admin_sig +'\r\n'+ admin_email
                        else:
                            admin_sig = [signature['Signature'] for signature in signatures if signature['ContactId'] == admin_id][0].split("\r\n")
                        
                            for line in admin_sig:
                                new_sig = new_sig + line + '\r\n'
                    else:
                        admin_email = None
                    new_sig = new_sig.replace("&#10;","\r\n").replace("&#160;"," ").replace("\r\n","<br>").replace("\n","")
                    try:
                        email_output = send_email(plan_name, salutation, primary_contact_email, pbroker_email, census_contact_email, payroll_contact_email, novalink_contact_email, admin_phone_number, admin_email, new_sig)
                        print('Email sent!')
                    except:
                       print('Email not sent to ',plan_id)
                       continue
                
                    # copying the email to project notes
                    to_list = email_output[1]
                    cc_list = email_output[2]
                    html_body = email_output[3]
                    subject = email_output[4]
            
                    #add note for Novalink Census Upload project within the plan
                    
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
                    
                    # override the task Census Loaded Notification
                    pp.override_task(task_id) 
                    print("Task overridden!")
                    
                print('Done!')         
                
            get_active_records()
            
            
            # In[ ]:
            
            
            
            

    schedule_logger('Finished', pyfile= __file__)

except SystemExit:
    schedule_logger('Finished', pyfile= __file__)
    raise
    
except Exception as e:
    email_error_to_automation(e, __file__)
    schedule_logger('Error', pyfile= __file__)

            