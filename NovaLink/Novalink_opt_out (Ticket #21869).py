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
            

            
            # In[2]:
            
            
            import sys
            sys.path.insert(0, r'C:\Users\Public\WPy64-39100\notebooks\Anjana Shaji')
            import requests
            import pensionpro_api as pp
            
            
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
            
            
            def add_services_provided(internal_planid):
                payload = {
                            "PlanId": internal_planid,
                            "ProvidedServiceId": 11038,
                            "Description": "Client opted out of Novalink services"
                          }
                
                response_data = pp.add_services_provided_by_planid(payload, internal_planid)
                
            
            
            # In[10]:
            
            
            def opt_out():
                name = 'Novalink'
                df = pp.get_worktray2(name,get_all=True)
                filt1 = df['proj_name'] == 'Novalink Initial Communication'
                df = df[filt1]
                for index, row in df.iterrows():
                    if  row["proj_name"] == "Novalink Initial Communication":
                        project_id = row["projid"]
                        data_json = pp.get_project_fields_by_projectid(project_id)
                        for data in data_json:
                            if data["FieldName"] == "Novalink Opt-Out" and data["FieldValue"] == "yes":
                                override_project(data["ProjectId"])
                                add_services_provided(data["PlanId"])
                print("Done!") 
            opt_out()    
                
                
            
            
            # In[ ]:
            
            
            
            

    schedule_logger('Finished', pyfile= __file__)

except SystemExit:
    schedule_logger('Finished', pyfile= __file__)
    raise
    
except Exception as e:
    email_error_to_automation(e, __file__)
    schedule_logger('Error', pyfile= __file__)

            