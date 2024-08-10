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
            import requests
            import pensionpro_api as pp
            import pandas as pd
            import datetime
            from glob import glob
            import time
            import json
            import os
            
            import smtplib
            from email.mime.multipart import MIMEMultipart
            from email.mime.text import MIMEText
            from email.mime.base import MIMEBase
            from email import encoders
            
            
            # In[2]:
            
            
            def pull_census_data(plan_id):
                url = f'https://bgs872jw77.execute-api.us-east-1.amazonaws.com/getInvitationStatus?act=by_plan&plan_id={plan_id}'
                response = requests.get(url)
                parsed = json.loads(response.content)
                if parsed:
                    test_census_create_date = parsed[0]["test_census_create_dt"]
                    full_census_request_dt = parsed[0]["full_census_request_dt"]
                    if not test_census_create_date:
                        return True
                    if full_census_request_dt:
                        return True
                    company_id = parsed[0]["company_id"]
                    census_url = f'https://bgs872jw77.execute-api.us-east-1.amazonaws.com/setInvitationStatus?act=startFullCensus&compareDt={test_census_create_date}&compareMe={company_id}&invitationCd={plan_id}'
                    census_response = requests.get(census_url)
                    census_parsed = json.loads(census_response.content)
                    print("census_parsed: ",census_parsed)
                    if census_parsed['changedRows'] == 0:
                        return True
                else:
                    print('No backend data available')
                    return True
            
            
            # In[3]:
            
            
            df = pp.get_worktray2('Novalink', get_all=True)
            filt1 = df['task_name'] == 'Census File Creation'
            filt2 = df['proj_name'] == 'Novalink Census Upload'
            filt3 = df['proj_name'] == 'NovaLink Census Upload'
            df = df[filt1 & (filt2 | filt3)]
            if df.empty:
                raise SystemExit("Script is shutting down")
            for index, row in df.iterrows():
                plan_id = row['planid']
                print(plan_id)
                if pull_census_data(plan_id):
                    print(f'test_census_create_dt missing or full census already requested or Json data incorrect for plan {plan_id}')
                    continue
            print('Done!')    
            
            
            # In[ ]:
            
            
            
            

    schedule_logger('Finished', pyfile= __file__)

except SystemExit:
    schedule_logger('Finished', pyfile= __file__)
    raise
    
except Exception as e:
    email_error_to_automation(e, __file__)
    schedule_logger('Error', pyfile= __file__)

            