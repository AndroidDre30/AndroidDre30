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
            

            
            # In[22]:
            
            
            import sys
            sys.path.insert(0, r'C:\Users\Public\WPy64-39100\notebooks\Anjana Shaji')
            import requests
            import pensionpro_api as pp
            import pandas as pd
            import datetime
            import json
            import os
            
            
            # In[27]:
            
            
            today_date = datetime.date.today()
            today_date_string = today_date.strftime("%Y-%m-%d")
            print(today_date_string)
            if today_date_string == '2024-02-05':
                print('The date is February 5, 2024')
                data = pp.get_projects(filters="Name eq 'Novalink Census Upload' and CompletedOn eq null")
                for project in data:
                    project_id = project['Id']
                    plan_id = project['PlanId']
                    task_data = pp.get_task_groups_by_projectid(project_id,expand="Tasks")
                    override_flag = False
                    wait_id = ''
                    followup_id = ''
                    for tasks_groups in task_data:
                        for tasks in tasks_groups['Tasks']:
                            if tasks['TaskName'] =='Census File Creation' and not tasks['TaskActive']:
                                override_flag = True
                                print(f'Census File Creation not yet active for internal plan id {plan_id}')
                            elif tasks['TaskName'] =='Wait' and not tasks['DateCompleted']:
                                wait_id = tasks['Id']
                            elif tasks['TaskName'] =='AM Follow-Up' and not tasks['DateCompleted']:
                                followup_id = tasks['Id']
                    if override_flag:
                        if wait_id:
                            pp.override_task(wait_id)
                        if followup_id:
                            pp.override_task(followup_id)
            else:
                print('Its not Feb 5th of 2024')
                raise SystemExit("Script is shutting down")
            print('Done')                
            
            
            # In[ ]:
            
            
            
            

    schedule_logger('Finished', pyfile= __file__)

except SystemExit:
    schedule_logger('Finished', pyfile= __file__)
    raise
    
except Exception as e:
    email_error_to_automation(e, __file__)
    schedule_logger('Error', pyfile= __file__)

            