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
            

            
            # In[ ]:
            
            
            
            
            
            # In[48]:
            
            
            # NOTE: This variable determines which year to pull the report.
            #       Do not run for any period start date that has a year prior to two years ago.
            #       Example: If this year is 2022, we only want to run this script for 2020 and beyond if there are projects in the worktray to check.
            #       The reason is that any date prior to two years ago is used by special projects and not for us to mess with.
            
            target_year = '2023'  # <---------------------- Change as needed
            
            
            # In[49]:
            
            
            from IPython.display import display, HTML                              #py_ignore
            display(HTML("<style>.container { width:100% !important; }</style>"))  #py_ignore
            
            import datetime as dt
            from datetime import datetime as dt2
            import email
            from email.header import decode_header
            import getpass
            import imaplib
            import os
            import pickle
            import re
            import pandas as pd
            from selenium import webdriver
            from selenium.webdriver.common.by import By
            from selenium.common.exceptions import NoSuchElementException
            from selenium.webdriver.support.ui import WebDriverWait
            from selenium.webdriver.support import expected_conditions as ec
            from webdriver_manager.chrome import ChromeDriverManager
            import sys
            import time
            from pathlib import Path
            
            import pensionpro as pp
            import lam
            import time
            
            sys.path.insert(0, r'C:\Users\Public\WPy64-39100\notebooks\Lam Hoang')
            #sys.path.insert(1,'U:/Vault')
            
            from public_vault import Username, Password, OAuth
            
            pd.set_option('display.max_columns',None)
            pd.set_option('display.max_rows',None)
            
            project_folder = r'Y:\Automation\Projects\Active\Complete 5500 Filing Status Task in Pro (Ticket# 16523)'
            os.chdir(project_folder)
            
            pickle_df_log_file = 'df_filing_status_tasks_completed.pkl'
            excel_log = 'df_filing_status_tasks_completed.xlsx'
            
            # Delete all files in download folder
            local_user = getpass.getuser()
            download_folder = f'C:/Users/{local_user}/Downloads'
            
            for file in os.listdir(download_folder):
                os.remove(os.path.join(download_folder,file))
            
            #-------------------------New Users Update this section----------------------
            
            chrome_driver = r"Y:\Automation\Chromedriver\chromedriver.exe"
            
            my_username = Username('DGEM')
            my_pw = Password('DGEM')
            
            email_username = Username('outlook')
            email_pw = Password('outlook')
            
            
            # Hard coded by Andrew on 7/19/23 from lam vault. These variables aren't referenced anywhere but i'll keep them just in case
            # my_username = 'Automation Team' 
            # my_pw = '4fhi3yBB'
            
            
            # email_username = "fe12d3a6-9b60-4764-ab55-868fd4533247" # Client ID
            # email_pw = "qtw8Q~HFdlP1RR4yES4e8paQOglCiieXHR8gvbOZ" #Client secret value
            #-----------------------------------------------------------------------------
            
            
            # In[ ]:
            
            
            
            
            
            # In[50]:
            
            
            df = pp.get_worktray('5500 Pending Signature')
            df['period_start_year'] = df['period_start'].str.split('/').str[-1]
            df
            
            
            # In[9]:
            
            
            df.period_start.value_counts() # Do not run for any period start date that has a year prior to two years ago.
            
            
            # In[18]:
            
            
            # Log in to DGEM
            # browser = webdriver.Chrome(chrome_driver)
            browser = webdriver.Chrome(ChromeDriverManager().install())
            browser.delete_all_cookies()
            
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
            
            
            # In[20]:
            
            
            # Click to send verification code via "Email"
            # WebDriverWait(browser,10).until(ec.visibility_of_element_located((By.XPATH, '//*[@id="MFAUC_rbEmail"]')))
            
            # browser.find_element_by_xpath('//*[@id="MFAUC_rbEmail"]').click()
            
            # # Click SEND
            # browser.find_element_by_xpath('//*[@id="MFAUC_btnSend"]').click()
            
            
            # In[15]:
            
            
            # from O365 import Account, FileSystemTokenBackend
            
            # cwd = Path.cwd()
            
            # credentials = OAuth('o365_client_id'), OAuth('o365_secret_value')
            
            # token_backend = FileSystemTokenBackend(token_path=cwd,
            #                                        token_filename='oauth_token.txt')
            # account = Account(credentials,
            #                   token_backend=token_backend,
            #                   scopes = ['basic','message_all','mailbox'])
            
            # if not account.is_authenticated:
            #     account.authenticate()
            
            
            # In[ ]:
            
            
            
            
            
            # In[16]:
            
            
            # time.sleep(60)
            
            
            # In[ ]:
            
            
            
            
            
            # In[17]:
            
            
            # mailbox = account.mailbox(resource='automation@nova401k.com')
            
            # inbox = mailbox.inbox_folder().get_messages()
            
            # for message in inbox:
            #     msg_sender = str(message.sender)
            #     if (msg_sender == 'ASC (no-reply@pension-plan-emails.com)'
            #         and (msg_subject := message.subject) == 'Verification Code'):
            #         msg_body = message.body
            #         regex = re.search(r'(?<=>)(?P<code>\d{6})(?=<)',msg_body) # find 6-digit num if between < and >
            #         verification_code = regex.group('code')
            #         message.mark_as_read()
            #         break
            
            
            # In[18]:
            
            
            # # Enter Verification code
            # browser.find_element_by_xpath('//*[@id="MFAUC_tbCode"]').send_keys(verification_code)
            
            # # Click verify
            # browser.find_element_by_xpath('//*[@id="MFAUC_btnVerify"]').click()
            
            # # Click Yes
            # WebDriverWait(browser,10).until(ec.visibility_of_element_located((By.XPATH,'//*[@id="MFAUC_rbYes"]')))
            # browser.find_element_by_xpath('//*[@id="MFAUC_rbYes"]').click()
            
            # # Click "Continue"
            # browser.find_element_by_xpath('//*[@id="MFAUC_btnContinue"]').click()
            
            
            # In[ ]:
            
            
            
            
            
            # In[ ]:
            
            
            
            
            
            # In[ ]:
            
            
            
            
            
            # In[21]:
            
            
            len(df)
            
            
            # In[22]:
            
            
            df.head()
            
            
            # In[23]:
            
            
            time.sleep(5)
            browser.get('https://dgem.asc-net.com/ascidoc/Find.aspx')
            
            # Go to "Status" and select "Sending to EFAST"
            status1 = browser.find_element_by_name('dd5500Status')
            status1.click()
            status1.send_keys('Filing received by EFAST')   
            status1.click()
            
            # Select the year
            year_field = browser.find_element_by_xpath('//*[@id="dd5500Year"]')
            year_field.click()
            year_field.send_keys(str(target_year))
            year_field.click()
            
            # Click "Next" under 5500 documents
            nextbutton2 = browser.find_element_by_id('lbtnNext_5500')
            nextbutton2.click()
            
            # Go to "Status Details" dropdown and click 'Export Find Results'
            status3 = browser.find_element_by_name('lbAction5500')
            status3.click()
            status3.send_keys('Export Find Results')   
            status3.click()
            
            # Click "Next" button
            nextbutton3 = browser.find_element_by_id('lbtnClientNext5500')
            nextbutton3.click()
            
            # CLick "here" to download
            downloadbutton = browser.find_element_by_link_text('here')
            
            downloadbutton.click()
            
            # Wait for file to be downloaded
            downloaded_file = lam.wait_for_file(download_folder,'5500Documents',10,ends_with='.xls')
            
            # Rename file
            old_file_name = os.path.join(download_folder,downloaded_file)
            new_file_name = os.path.join(download_folder,f'DGEM_{target_year}.xls')
            os.rename(old_file_name,new_file_name)
            
            print("Download complete!")
            print(f'File downloaded: {old_file_name}')
            time.sleep(1)
            
            
            # In[24]:
            
            
            # Pull data from file
            df2 = pd.read_html(new_file_name,
                               converters={'f5500Id':str})[0][['f5500Id','PlanName','filingRecordId','CurrentDOLStatus','planYearEndDate','CurrentStatusTimestamp']]
            
            df2.rename(columns={'f5500Id':'plan_id',
                        'PlanName':'plan_name',
                        'planYearEndDate':'period_end'},inplace=True)
            
            
            # In[25]:
            
            
            df2.head()
            
            
            # In[ ]:
            
            
            
            
            
            # In[26]:
            
            
            df2.dtypes
            
            
            # In[ ]:
            
            
            
            
            
            # In[27]:
            
            
            # Filter out only plans that have filing DOL Status of "FILING_RECEIVED"
            df2 = df2[~df2['CurrentDOLStatus'].isna()]
            df2 = df2.loc[df2.CurrentDOLStatus.str.contains('FILING_RECEIVED')].reset_index(drop=True)
            df2.period_end = df2.period_end.str.split().str[0]
            df2.CurrentStatusTimestamp = df2.CurrentStatusTimestamp.str.split().str[0]
            
            
            # In[ ]:
            
            
            
            
            
            # In[ ]:
            
            
            
            
            
            # In[ ]:
            
            
            
            
            
            # In[28]:
            
            
            df2.dtypes
            
            
            # In[29]:
            
            
            df2.head()
            
            
            # In[30]:
            
            
            df.head()
            
            
            # In[31]:
            
            
            df
            
            
            # In[32]:
            
            
            len(df),len(df2)
            
            
            # In[33]:
            
            
            #df = df[~df.planid.isin(['6447', '6392'])].copy()
            len(df)
            
            
            # In[34]:
            
            
            time.sleep(1)
            
            
            # In[35]:
            
            
            df2
            
            
            # In[36]:
            
            
            df
            
            
            # In[37]:
            
            
            # Complete tasks
            for i in df.index[:]:    # <------------------ Don't forget to reset index range
                plan_id = df.planid.at[i]
                plan_name = df.plan_name.at[i]
                period_end = df.period_end.at[i]
                task_id = df.taskid.at[i]
                
                print(f'index {i} of {len(df)-1}: plan {plan_id} {plan_name}')
                
                # Remember that df2 was already filtered out to include only plans that have a filing status of "received"
                df3 = df2.loc[(df2.plan_id == plan_id) & (df2.period_end == period_end)].reset_index(drop=True)
                
                if len(df3) == 0:
                    print('\tSKIP: Filing Not Yet Received')
                
                if len(df3) > 1:
                    print('There should not be more than one row of data found')
                    continue
                    raise Exception('There should not be more than one row of data found')
                
                if len(df3) == 1:
                    print('\tFiling received')
                    df.at[i,'Filing Status'] = 'Received'
                    date_filed = df3.CurrentStatusTimestamp.at[0]
                    df.at[i,'date_filed'] = date_filed
                    
                    task_items = False
                    
                    try:
                        task_items = pp.get_task_items_by_taskid(task_id)
                    except:
                        time.sleep(2)
                        task_items = pp.get_task_items_by_taskid(task_id)
                    
                    task_item_id = False
                    
                    task_item_updated = False
                    
                    questions_to_find = [
                        'Date Filed',
                        'Date electronically filed',
                        'Confirm 5500 electronically submitted - date accepted:',  # "date" instead of "data"
                        'Confirm 5500 electronically submitted - data accepted:',
                        'Confirm amended 5500 electronically submitted - date accepted:',
                        'Confirm amended  5500 electronically submitted - date accepted:',
                        #               ^ may need space here:  
                    ]
                    
                    for task_item in task_items:
                        
                        if any(task_item['Question'] == question for question in questions_to_find):    
                            
                            task_item_id = task_item['Id']
                            task_item['Value'] = date_filed
                            
                            try:
                                r = pp.update_taskitem(task_item_id, payload=task_item, expand=None)
                            except:
                                time.sleep(2)
                                r = pp.update_taskitem(task_item_id, payload=task_item, expand=None)
                            
                            print('\ttask item updated')
                            df.at[i,'task_item_updated'] = True
                            task_item_updated = True 
                            
                            break
                    
                    if task_item_updated == True:
                        try:
                            r = pp.complete_task(task_id)
                        except:
                            time.sleep(2)
                            #r = pp.complete_task(task_id)
                            r = pp.override_task(task_id)
                            
                        print('\ttask completed')
                        df.at[i,'task_completed'] = True
                    else:
                        print("A task for inputing electronic file date was not available")
                        continue
                        raise Exception("A task for inputing electronic file date was not available")
                    
            df.fillna('',inplace=True)
              
            print('\nDone')
            
            
            # In[28]:
            
            
            # Failed on
            
            
            # In[38]:
            
            
            time.sleep(1)
            
            
            # In[39]:
            
            
            try:
                df = df.loc[df.task_completed == True].reset_index(drop=True)
            except:
                pass
            
            
            # In[40]:
            
            
            df
            
            
            # In[41]:
            
            
            df_log = pd.read_pickle(pickle_df_log_file)
            
            
            # In[42]:
            
            
            len(df_log),len(df)
            
            
            # In[43]:
            
            
            df_log = pd.concat([df_log,df],ignore_index=True)
            len(df_log)
            
            
            # In[44]:
            
            
            df_log.to_pickle(pickle_df_log_file)
            
            
            # In[45]:
            
            
            df_log.to_excel(excel_log)
            
            
            # In[46]:
            
            
            browser.quit()
            
            
            # In[47]:
            
            
            timestamp = dt2.today().strftime('%m/%d/%Y %I:%M %p')
            print(timestamp)
            
            
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

            