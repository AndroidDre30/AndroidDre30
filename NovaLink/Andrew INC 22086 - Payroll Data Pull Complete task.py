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
            sys.path.insert(0, "Y:\Automation\Team Scripts\Andrew Kim\my modules")
            import datetime
            import os
            import requests 
            import json
            import numpy as np
            
            import pandas as pd
            import pensionpro_v1 as pp
            #import pensionpro as pp
            
            from IPython.display import display, HTML
            pd.set_option('display.max_rows',None)
            pd.set_option('display.max_columns',None)
            
            
            # In[2]:
            
            
            def convert_utc_into_central(utc_timestamp):
                
                formats = ["%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S.%fZ"]
                for fmt in formats: # Try each format since some UTC timestamps have triple zeres in the Seconds field.
                    try:
                        utc_dt = datetime.datetime.strptime(utc_timestamp, fmt)
                        break
                    except ValueError:
                        continue
                        
                us_central_offset = datetime.timedelta(hours=-6) 
                
                central_time = utc_dt + us_central_offset
                
                return central_time
            
            def weekdays_between(start_date, end_date):
                """
                Get difference between 2 dates and return the number of days.
                """
                days = np.busday_count(start_date.date(), end_date.date())
                return days   
            
            
            # In[3]:
            
            
            json_url = "https://bgs872jw77.execute-api.us-east-1.amazonaws.com/getInvitationStatus?act=status_all"
            json_data_df = pd.DataFrame(requests.get(json_url).json())
            
            # invitation_cd is the TPA Plan ID
            json_data_df
            
            
            # In[4]:
            
            
            WAIT_WORKTRAY = pp.get_worktray("Wait",get_all=True)
            filt1 = WAIT_WORKTRAY["task_name"] == "Wait for Finch Access"
            filt2 = WAIT_WORKTRAY["proj_name"].str.lower() == "novalink payroll access setup"
            filt3 = WAIT_WORKTRAY["planid"] != '99205'
            WAIT_WORKTRAY = WAIT_WORKTRAY[filt1 & filt2 & filt3]
            WAIT_WORKTRAY["task_active"] = pd.to_datetime(WAIT_WORKTRAY["task_active"])
            WAIT_WORKTRAY
            
            
            # In[5]:
            
            
            # There is no script that is reaching into the wait task and checking for the available timestamp information. Now I 
            # am searching for the target timestamp and overriding the WAIT task if the timestamp is available. 
            
            for planid in WAIT_WORKTRAY['planid']:
                plan_name = WAIT_WORKTRAY.loc[WAIT_WORKTRAY['planid'] == planid,'plan_name'].iloc[0]
                taskid = WAIT_WORKTRAY.loc[WAIT_WORKTRAY['planid']== planid,'taskid'].iloc[0]
                projectid = WAIT_WORKTRAY.loc[WAIT_WORKTRAY['planid']== planid,'projid'].iloc[0]
                task_active = WAIT_WORKTRAY.loc[WAIT_WORKTRAY['planid']== planid,'task_active'].iloc[0]
                days_active = weekdays_between(task_active, datetime.datetime.now())
                
                try:
                    initial_data_pull_dt = json_data_df.loc[json_data_df['invitation_cd'] == planid,'initial_data_pull_dt'].iloc[0]
                except IndexError:
                    print(f"Plan Id {planid} does not exist in the JSON data. Skipping.\n")
                    continue
                #if (initial_data_pull_dt == None) & (days_active < 3):
                if (initial_data_pull_dt == None):
                    print(f"{planid} {plan_name} has an None authorization date and client still has time to submit information. Skipping.\n")
                    continue
                # There isn't a backup task that instructs us what to do if we've been stuck in the "Wait for Finch Access"
                # for greater than 2 days. ¯\_(ツ)_/¯
                    
                    
                    
            #     # Client waited too long to provide information. Advance the task to 'Authentication Follow Up'
            #     elif (initial_data_pull_dt == None) & (days_active > 2):
                    
            #         novalink_project_tasks = pp.get_task_groups_by_projectid(projectid,expand="Tasks")
            #         finch_authentication_task_group = [i for i in novalink_project_tasks if i["Name"] == 'Finch Authentication'][0]
            
            #         # Override *up to* 'Authentication Follow Up' task.
            #         for task in finch_authentication_task_group['Tasks']:
            #             if task["TaskName"] == 'Authentication Follow Up':
            #                 break
            #             pp.override_task(task['Id'])
            #         continue
                    
                    
                # Found the 'initial_data_pull_dt' information for the planid in the json data. Move forward. You dont have to wait 5 days.    
                print(f"Advancing {planid} {plan_name} from WAIT task.")
                pp.override_task(taskid)
            
            
            # In[6]:
            
            
            WAIT_WORKTRAY = pp.get_worktray2("Wait",get_all=True)
            
            filt1 = WAIT_WORKTRAY["task_name"] == "Payroll Data Pull Complete"
            filt2 = WAIT_WORKTRAY["proj_name"].str.lower() == "novalink payroll access setup"
            filt3 = WAIT_WORKTRAY["planid"] != '99205'
            
            WAIT_WORKTRAY = WAIT_WORKTRAY[filt1 & filt2 & filt3]
            
            if len(WAIT_WORKTRAY) == 0:
                print('0 worktray items.')
                raise SystemExit("No worktray items.")
                
            
            #WAIT_WORKTRAY['runtime'] = datetime.datetime.now().strftime("%m/%d/%Y %H:%M:%S")
            WAIT_WORKTRAY
            
            
            # In[ ]:
            
            
            for i in WAIT_WORKTRAY.index[:]:
                
                planid = WAIT_WORKTRAY.at[i,'planid']
                projectid = WAIT_WORKTRAY.at[i,'projid']
                taskid = WAIT_WORKTRAY.at[i,'taskid']
                
                #TEST
            #     planid = '6702'
            #     projectid = 9633970 #Kelvin's project
            #     taskid = 66262762 # Kelvin project task id
                
                try:
                    initial_data_pull_dt = json_data_df.loc[json_data_df['invitation_cd'] == planid,'initial_data_pull_dt'].iloc[0]
                except IndexError:
                    print(f"Plan Id {planid} does not exist in the JSON data. Skipping.")
                    continue
                if initial_data_pull_dt == None:
                    print(f"{planid} has an None authorization date.")
                    continue
                    
                    
                initial_data_pull_dt =  convert_utc_into_central(initial_data_pull_dt) # Convert to datetime
                initial_data_pull_dt =  initial_data_pull_dt.strftime("%m/%d/%Y")
                
                
                print(f"{planid} authorized on {initial_data_pull_dt}")
                
                
                # Get the task item as a dictionary and use it as a payload for the PUT request.
                target_task_item_dict = [i for i in pp.get_task_items_by_taskid(taskid) if i["Question"] == 'Date Initial Data Pull Complete'][0]
                target_task_item_dict["Value"] = initial_data_pull_dt
                pp.put_taskitem(target_task_item_dict)
                print(f"Task item updated with value {initial_data_pull_dt}.")
                
                pp.override_task(taskid)
                print("Task overridden.")
            
            
            # In[ ]:
            
            
            
            

    schedule_logger('Finished', pyfile= __file__)

except SystemExit:
    schedule_logger('Finished', pyfile= __file__)
    raise
    
except Exception as e:
    email_error_to_automation(e, __file__)
    schedule_logger('Error', pyfile= __file__)

            