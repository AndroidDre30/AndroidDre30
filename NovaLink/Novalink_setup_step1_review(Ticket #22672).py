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
            import datetime
            import os
            
            
            # In[3]:
            
            
            def add_services_provided(tpa_id):
                internal_planid = pp.get_sysplanid(tpa_id)
                print(internal_planid)
                url_key = "bgs872jw77" # uncomment this for production url
                # url_key = "kr7fe09ic8" #comment this for production, since this is the development url
                url = f"https://{url_key}.execute-api.us-east-1.amazonaws.com/getInvitationLink?plan_id={tpa_id}"
                r = requests.get(url)
                description = r.text
                payload = {
                            "Description": description,
                            "PlanId": internal_planid,
                            "ProvidedServiceId": 11037,
                          }
                
                response_data = pp.add_services_provided_by_planid(payload, internal_planid)
            
            
            # In[4]:
            
            
            def launch_project(tpa_id):
                data_json = pp.get_project_template_by_name("Novalink Payroll Access Setup")
                current_datetime = datetime.datetime.now()
                start_date = current_datetime.strftime("%m/%d/%Y, %H:%M:%S")
                start_date_30 = current_datetime + datetime.timedelta(days=30)
                due_on = start_date_30.strftime("%m/%d/%Y, %H:%M:%S")
                print("start_date: ",start_date)
                print("due_on: ",due_on)
                project_template_id = data_json[0]["Id"]
                print(project_template_id)
                start_date = start_date
                due_on = due_on
                period_start = ''
                period_end = ''
                description = data_json[0]["Description"]
                response_data = pp.add_project(tpa_id, project_template_id, start_date, due_on, period_start, period_end, description)
            
            
            # In[5]:
            
            
            # Override rest of "Novalink Initial Communication" project
            def override_project(project_id):
                print(project_id)
                tasks_json = pp.get_task_groups_by_projectid(project_id,expand="Tasks",filters="DateCompleted eq null")
                print(tasks_json)
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
            
            
            df = pp.get_worktray2("Novalink",get_all=True)
            filt1 = df['task_name'] == 'Initial Setup'
            filt2 = df['proj_name'] == 'Novalink Setup-Step 1'
            df = df[filt1 & filt2]
            if df.empty:
                raise SystemExit("Script is shutting down")
            for index, row in df.iterrows():
                plan_id = row['planid']
                project_id = row['projid']
                client_directory = r'G:'
                for folder in os.listdir(client_directory):
                    if folder.split()[0] == str(plan_id):
                        destination_folder =f'G:/{folder}/Novalink'
                        if not os.path.exists(destination_folder):
                            os.makedirs(destination_folder)
                
                add_services_provided(plan_id)
                launch_project(plan_id)
                override_project(project_id)
                print(f'Services added, launched project and completed project for plan id {plan_id}')
            print('Done!')
            
            
            # In[ ]:
            
            
            
            

    schedule_logger('Finished', pyfile= __file__)

except SystemExit:
    schedule_logger('Finished', pyfile= __file__)
    raise
    
except Exception as e:
    email_error_to_automation(e, __file__)
    schedule_logger('Error', pyfile= __file__)

            