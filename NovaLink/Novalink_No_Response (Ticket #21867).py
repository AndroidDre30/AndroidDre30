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
            
            
            import sys
            sys.path.insert(0, r'C:\Users\Public\WPy64-39100\notebooks\Anjana Shaji')
            import requests
            import pensionpro_api as pp
            import pandas as pd
            import datetime
            import numpy as np
            
            
            # In[ ]:
            
            
            def override_project(project_id):
                tasks_json = pp.get_task_groups_by_projectid(project_id,expand="Tasks",filters="DateCompleted eq null")
                tasks_of_active_task_groups  = []
                for tasks in tasks_json:
                    tasks_of_active_task_groups.append(tasks["Tasks"])
                combined_tasks= []
                for tasks in tasks_of_active_task_groups:
                    combined_tasks.extend(tasks)
                active_tasks = []
                for task in combined_tasks:
                    if not task["DateCompleted"]:
                        active_tasks.append(task)
                for task in active_tasks:
                    response_data = pp.override_task(task["Id"])   
            
            
            # In[ ]:
            
            
            def calculate_wait(start_date, end_date):
                days = np.busday_count(start_date.date(), end_date.date())
                return days 
            
            
            # In[ ]:
            
            
            def get_wait_worktray():
                df = pp.get_worktray3('Wait', get_all=True)
                filt1 = df['task_name'] == 'Wait for Client Response to Phone Call'
                filt2 = df['proj_name'] == 'Novalink Initial Communication'
                df = df[filt1 & filt2]
                df["task_active"] = pd.to_datetime(df["task_active"])
                print(df)
                return df
            
            # get_wait_worktray()
            
            
            # In[ ]:
            
            
            def get_records_in_wait():
                df = get_wait_worktray()
                if df.empty:
                    raise SystemExit("Script is shutting down")
                for index, row in df.iterrows():
                    plan_id = row['planid']
                    wait = calculate_wait(row['task_active'], datetime.datetime.now())
                    if wait > 10:
                         pp.override_task(row['taskid'])
                    else:    
                        continue
                        
            get_records_in_wait()
            
            
            # In[ ]:
            
            
            worktray_name = 'Novalink'
            df = pp.get_worktray2(worktray_name)
            filt1 = df['task_name'] == 'Document Client Response and Authorization'
            filt2 = df['proj_name'] == 'Novalink Initial Communication'
            df = df[filt1 & filt2]
            if df.empty:
                    raise SystemExit("Script is shutting down")
            for index, row in df.iterrows():
                    project_id = row["projid"]
                    override_project(project_id)
            print("Done")
            

    schedule_logger('Finished', pyfile= __file__)

except SystemExit:
    schedule_logger('Finished', pyfile= __file__)
    raise
    
except Exception as e:
    email_error_to_automation(e, __file__)
    schedule_logger('Error', pyfile= __file__)

            