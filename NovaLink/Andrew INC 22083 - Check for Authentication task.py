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
            
            def find_task(project_id, task_group, task_name,skip_count=0, taskgroup_wildcard = False, task_wildcard = False):
                
                """
                Get the dictionary of the target task.
                Skip count is how many hits the function should skip in case there are duplicate task names in the same grouping. 
                
                Setting the wildcard option to True will use your inputted substring and see if its in the Taskgroup name.
                Not recommended unless the the substring contains rare names.
                
                """
                
                counter = 0
                for i in pp.get_task_groups_by_projectid(project_id,expand="Tasks.CurrentTaskState"):
                    if (i["Name"] == task_group) or (taskgroup_wildcard == True and task_group.lower() in i["Name"].lower()):
                        for j in i["Tasks"]:
                            if j["TaskName"] == task_name or (task_wildcard == True and task_name.lower() in i["Name"].lower()):
                                counter += 1
                                if counter <= skip_count:
                                    continue
                                else:
                                    return j  
                                
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
            filt1 = WAIT_WORKTRAY["task_name"] == "Wait for Client Response to Email"
            filt2 = WAIT_WORKTRAY["proj_name"].str.lower() == "novalink payroll access setup"
            filt3 = WAIT_WORKTRAY["planid"] != '99205'
            WAIT_WORKTRAY = WAIT_WORKTRAY[filt1 & filt2 & filt3]
            WAIT_WORKTRAY["task_active"] = pd.to_datetime(WAIT_WORKTRAY["task_active"])
            WAIT_WORKTRAY
            
            
            # In[5]:
            
            
            for planid in WAIT_WORKTRAY['planid']:
                plan_name = WAIT_WORKTRAY.loc[WAIT_WORKTRAY['planid'] == planid,'plan_name'].iloc[0]
                taskid = WAIT_WORKTRAY.loc[WAIT_WORKTRAY['planid']== planid,'taskid'].iloc[0]
                projectid = WAIT_WORKTRAY.loc[WAIT_WORKTRAY['planid']== planid,'projid'].iloc[0]
                task_active = WAIT_WORKTRAY.loc[WAIT_WORKTRAY['planid']== planid,'task_active'].iloc[0]
                days_active = weekdays_between(task_active, datetime.datetime.now())
                print('projectid', projectid)
                
                try:
                    auth_complete_dt = json_data_df.loc[json_data_df['invitation_cd'] == planid,'auth_complete_dt'].iloc[0]
                    print(f'auth_complete_dt {auth_complete_dt}')
                except IndexError:
                    print(f"Plan Id {planid} does not exist in the JSON data. Skipping.\n")
                    continue
                if auth_complete_dt:
                    print(f"Advancing {planid} {plan_name} from WAIT task.")
                    pp.override_task(taskid)
                    continue
                elif (auth_complete_dt == None) & (days_active < 6):
                    print(f'days active is {days_active} for plan {planid} in less than 5 days')
                    print(f"{planid} {plan_name} has an None authorization date and client still has time to submit information. Skipping.\n")
                    continue
                    
                # Client waited too long to provide information. Advance the task to 'Authentication Follow Up'
                elif (auth_complete_dt == None) & (days_active > 5):
                    print(f'days active is {days_active} for plan {planid} in greater than 5 days')
                    novalink_project_tasks = pp.get_task_groups_by_projectid(projectid,expand="Tasks")
                    finch_authentication_task_group = [i for i in novalink_project_tasks if i["Name"] == 'Finch Authentication'][0]
            
                    # Override *up to* 'Authentication Follow Up' task.
                    for task in finch_authentication_task_group['Tasks']:
                        if task["TaskName"] == 'Wait for Client Response to Email' or task["TaskName"] == 'Check For Authentication':
                            task_id = task['Id']
                            print(f'inside Wait for Client Response to Email or Check For Authentication for task id {task_id}')
                            pp.override_task(task_id)
                    continue
                    
            
            
            # In[6]:
            
            
            # # There is no script that is reaching into the wait task and checking for the available timestamp information. Now I 
            # # am searching for the target timestamp and overriding the WAIT task if the timestamp is available. 
            
            # for planid in WAIT_WORKTRAY['planid']:
            #     plan_name = WAIT_WORKTRAY.loc[WAIT_WORKTRAY['planid'] == planid,'plan_name'].iloc[0]
            #     taskid = WAIT_WORKTRAY.loc[WAIT_WORKTRAY['planid']== planid,'taskid'].iloc[0]
            #     projectid = WAIT_WORKTRAY.loc[WAIT_WORKTRAY['planid']== planid,'projid'].iloc[0]
            #     task_active = WAIT_WORKTRAY.loc[WAIT_WORKTRAY['planid']== planid,'task_active'].iloc[0]
            #     days_active = weekdays_between(task_active, datetime.datetime.now())
                
            #     try:
            #         auth_complete_dt = json_data_df.loc[json_data_df['invitation_cd'] == planid,'auth_complete_dt'].iloc[0]
            #     except IndexError:
            #         print(f"Plan Id {planid} does not exist in the JSON data. Skipping.\n")
            #         continue
            #     if (auth_complete_dt == None) & (days_active < 6):
            #         print(f"{planid} {plan_name} has an None authorization date and client still has time to submit information. Skipping.\n")
            #         continue
                    
            #     # Client waited too long to provide information. Advance the task to 'Authentication Follow Up'
            #     elif (auth_complete_dt == None) & (days_active > 5):
                    
            #         novalink_project_tasks = pp.get_task_groups_by_projectid(projectid,expand="Tasks")
            #         finch_authentication_task_group = [i for i in novalink_project_tasks if i["Name"] == 'Finch Authentication'][0]
            
            #         # Override *up to* 'Authentication Follow Up' task.
            #         for task in finch_authentication_task_group['Tasks']:
            #             if task["TaskName"] == 'Authentication Follow Up':
            #                 break
            #             pp.override_task(task['Id'])
            #         continue
                    
                    
            #     # Found the 'auth_complete_dt' information for the planid in the json data. Move forward. You dont have to wait 5 days.    
            #     print(f"Advancing {planid} {plan_name} from WAIT task.")
            #     pp.override_task(taskid)
            
            
            # In[7]:
            
            
            NOVALINK_WORKTRAY = pp.get_worktray("Novalink",get_all=True)
            
            filt1 = NOVALINK_WORKTRAY["task_name"] == "Check For Authentication"
            filt2 = NOVALINK_WORKTRAY["proj_name"].str.lower() == "novalink payroll access setup"
            filt3 = NOVALINK_WORKTRAY["planid"] != '99205'
            
            NOVALINK_WORKTRAY = NOVALINK_WORKTRAY[filt1 & filt2 & filt3]
            
            if len(NOVALINK_WORKTRAY) == 0:
                raise SystemExit("No worktray items.")
                
            
            #NOVALINK_WORKTRAY['runtime'] = datetime.datetime.now().strftime("%m/%d/%Y %H:%M:%S")
            NOVALINK_WORKTRAY
            
            
            # In[8]:
            
            
            for i in NOVALINK_WORKTRAY.index[:]:
                
                planid = NOVALINK_WORKTRAY.at[i,'planid']
                projectid = NOVALINK_WORKTRAY.at[i,'projid']
                taskid = NOVALINK_WORKTRAY.at[i,'taskid']
                
                
                try:
                    auth_complete_dt = json_data_df.loc[json_data_df['invitation_cd'] == planid,'auth_complete_dt'].iloc[0]
                except IndexError:
                    print(f"Plan Id {planid} does not exist in the JSON data. Skipping.")
                    continue
                if auth_complete_dt == None:
                    print(f"{planid} has an None authorization date.")
                    pp.override_task(taskid)
                    continue
                    
                    
                auth_complete_dt =  convert_utc_into_central(auth_complete_dt) # Convert to datetime
                auth_complete_dt =  auth_complete_dt.strftime("%m/%d/%Y")
                
                
                print(f"{planid} authorized on {auth_complete_dt}")
                
                
                # Get the task item as a dictionary and use it as a payload for the PUT request.
                target_task_item_dict = [i for i in pp.get_task_items_by_taskid(taskid) if i["Question"] == 'Date Authorization Completed'][0]
                target_task_item_dict["Value"] = auth_complete_dt
                pp.put_taskitem(target_task_item_dict)
                print(f"Task item updated  with value {auth_complete_dt}.")
                
                
                
                novalink_project_tasks = pp.get_task_groups_by_projectid(projectid,expand="Tasks")
                finch_authentication_task_group = [i for i in novalink_project_tasks if i["Name"] == 'Finch Authentication'][0]
                
                # Override *up to* "Wait for Finch Access" task.
                for task in finch_authentication_task_group['Tasks']:
                    if task["TaskName"] == 'Check For Authentication' or task["TaskName"] == 'Authentication Follow Up':
                        print(f'overriding tasks till wait for finch access for plan {planid}')
                        pp.override_task(task['Id'])
                    
                
            
            
            # In[9]:
            
            
            raise SystemExit("Done")
            
            
            # In[ ]:
            
            
            finch_authentication_task_group
            
            
            # In[ ]:
            
            
            for task in finch_authentication_task_group['Tasks']:
                # Override ***up to*** 'Wait for Finch Access'
                if task["TaskName"] == 'Wait for Finch Access':
                    break
                pp.override_task(task['Id'])
            
            
            # In[ ]:
            
            
            pp.override_task(task['Id'])
            
            
            # In[ ]:
            
            
            pp.get_task_items_by_taskid(taskid)
            
            
            # In[ ]:
            
            
            #pp.get_projects_by_planid('99205')
            novalink_project_tasks = pp.get_task_groups_by_projectid(9633970,expand="Tasks")
            finch_authentication_task_group = [i for i in novalink_project_tasks if i["Name"] == 'Finch Authentication'][0]
            finch_authentication_task_group
            
            
            # In[ ]:
            
            
            [i["Id"] for i in finch_authentication_task_group["Tasks"] if i["TaskName"] == 'Check For Authentication'][0]
            
            
            # In[ ]:
            
            
            find_task(9633970, 'Finch Authentication', 'Check For Authentication')
            
            
            # In[ ]:
            
            
            test_dict = [i for i in pp.get_task_items_by_taskid(66262759) if i["Question"] == 'Date Authorization Completed'][0]
            test_dict["Value"] = '10/27/1988'
            pp.put_taskitem(test_dict)
            
            
            # In[ ]:
            
            
            novalink_project_tasks
            
            
            # In[ ]:
            
            
            
            
            convert_utc_into_central('2023-11-06T19:12:56.000Z')
            #datetime.datetime.strptime('2023-11-08T18:02:14.000Z', "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=pytz.UTC).astimezone(pytz.timezone('US/Central'))
            
            
            # In[ ]:
            
            
            
            
            
            # In[ ]:
            
            
            "%Y-%m-%dT%H:%M:%SZ" "%Y-%m-%dT%H:%M:%S.%fZ"
            
            
            # In[ ]:
            
            
            
            

    schedule_logger('Finished', pyfile= __file__)

except SystemExit:
    schedule_logger('Finished', pyfile= __file__)
    raise
    
except Exception as e:
    email_error_to_automation(e, __file__)
    schedule_logger('Error', pyfile= __file__)

            