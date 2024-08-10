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
            

            
            # In[18]:
            
            
            import sys
            sys.path.insert(0, "Y:\Automation\Team Scripts\Andrew Kim\my modules")
            import datetime
            import os
            import requests 
            import json
            import numpy as np
            
            import pandas as pd
            import pensionpro_v1 as pp1
            import pensionpro as pp
            
            from IPython.display import display, HTML
            pd.set_option('display.max_rows',None)
            pd.set_option('display.max_columns',None)
            
            
            # In[19]:
            
            
            def convert_utc_into_central(utc_timestamp):
                
                formats = ["%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%d %H:%M:%S"]
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
            
            
            def close_project(project_id):
                """
                Overrides all open tasks of a project.
                """
                # Get all active task groups with their tasks. 
                tasks_of_active_task_groups = [i["Tasks"] for i in pp.get_task_groups_by_projectid(project_id,expand="Tasks",filters="DateCompleted eq null")]
                
                
                # Each task is a dictionary. A task group is a list with dictionaries. Break these dictionaries out of its parent list
                # by extending each list into a single list.    
                combined_tasks = []
                for tasks in tasks_of_active_task_groups: 
                    combined_tasks.extend(tasks)
                
                # Filter tasks based on having no completion date. Override in sequence. 
                all_active_tasks = [i for i in combined_tasks if not i["DateCompleted"]]
                for task in all_active_tasks:
                    pp.override_task(task["Id"])
            
                # Keep a record of all completed tasks just in case you need to undo it.
                
                # If you ever need to uncomplete these, be sure to do a try/except.
                # Uncompleting a task in a task group will uncomplete everything down stream of a task group. 
                # This means you may try to uncomplete a task that was already uncompleted. 
                return [i["Id"] for i in all_active_tasks] 
            
            
            # In[20]:
            
            
            json_url = "https://bgs872jw77.execute-api.us-east-1.amazonaws.com/getInvitationStatus?act=status_all"
            json_data_df = pd.DataFrame(requests.get(json_url).json())
            
            # invitation_cd is the TPA Plan ID
            json_data_df
            
            
            # In[21]:
            
            
            WAIT_WORKTRAY = pp1.get_worktray("Wait",get_all=True)
            filt1 = WAIT_WORKTRAY["task_name"] == "Wait for Client To Complete Paycode Mapping File"
            #filt2 = WAIT_WORKTRAY["proj_name"] == "Novalink Payroll Access Setup"
            filt2 = WAIT_WORKTRAY["proj_name"].str.lower() == "novalink payroll access setup"
            filt3 = WAIT_WORKTRAY["planid"] != '99205'
            WAIT_WORKTRAY = WAIT_WORKTRAY[filt1 & filt2 & filt3]
            WAIT_WORKTRAY["task_active"] = pd.to_datetime(WAIT_WORKTRAY["task_active"])
            WAIT_WORKTRAY
            
            
            # In[22]:
            
            
            for planid in WAIT_WORKTRAY['planid']:
                plan_name = WAIT_WORKTRAY.loc[WAIT_WORKTRAY['planid'] == planid,'plan_name'].iloc[0]
                taskid = WAIT_WORKTRAY.loc[WAIT_WORKTRAY['planid']== planid,'taskid'].iloc[0]
                projectid = WAIT_WORKTRAY.loc[WAIT_WORKTRAY['planid']== planid,'projid'].iloc[0]
                task_active = WAIT_WORKTRAY.loc[WAIT_WORKTRAY['planid']== planid,'task_active'].iloc[0]
                days_active = weekdays_between(task_active, datetime.datetime.now())
                
                try:
                    decoder_submitted_dt = json_data_df.loc[json_data_df['invitation_cd'] == planid,'decoder_submitted_dt'].iloc[0]
                except IndexError:
                    print(f"Plan Id {planid} does not exist in the JSON data. Skipping.\n")
                    continue
                if decoder_submitted_dt:
                    print(f"Advancing {planid} {plan_name} from WAIT task.")
                    pp.override_task(taskid)
                    continue
                elif (decoder_submitted_dt == None) & (days_active < 6):
                    print(f'days active is {days_active} for plan {planid} in less than 5 days')
                    print(f"{planid} {plan_name} has an None decoder_submitted date and client still has time to submit information. Skipping.\n")
                    continue
                    
                # Client waited too long to provide information. Advance the task to 'Authentication Follow Up'
                elif (decoder_submitted_dt == None) & (days_active > 5):
                    print(f'days active is {days_active} for plan {planid} in greater than 5 days')
                    print(f"{planid} waited too long to provide data. Pushing the project into the follow-up task")
                    
                    novalink_project_tasks = pp.get_task_groups_by_projectid(projectid,expand="Tasks")
                    populating_paycode_mapping_file_task_group = [i for i in novalink_project_tasks if i["Name"] == 'Populating Paycode Mapping File'][0]
            
                    # Override *up to* 'Follow-up with Client' task.
                    for task in populating_paycode_mapping_file_task_group['Tasks']:
                        if task["TaskName"] == 'Wait for Client To Complete Paycode Mapping File' or task["TaskName"] == 'Check For Paycode Mapping File':
                            task_id = task['Id']
                            print(f'inside Wait for Client To Complete Paycode Mapping File or Check For Paycode Mapping File for task id {task_id}')
                            pp.override_task(task_id)
                    continue
            
            
            # In[23]:
            
            
            # There is no script that is reaching into the wait task and checking for the available timestamp information. Now I 
            # am searching for the target timestamp and overriding the WAIT task if the timestamp is available. 
            
            # for planid in WAIT_WORKTRAY['planid']:
            #     plan_name = WAIT_WORKTRAY.loc[WAIT_WORKTRAY['planid'] == planid,'plan_name'].iloc[0]
            #     taskid = WAIT_WORKTRAY.loc[WAIT_WORKTRAY['planid']== planid,'taskid'].iloc[0]
            #     projectid = WAIT_WORKTRAY.loc[WAIT_WORKTRAY['planid']== planid,'projid'].iloc[0]
            #     task_active = WAIT_WORKTRAY.loc[WAIT_WORKTRAY['planid']== planid,'task_active'].iloc[0]
            #     days_active = weekdays_between(task_active, datetime.datetime.now())
                
            #     try:
            #         decoder_submitted_dt = json_data_df.loc[json_data_df['invitation_cd'] == planid,'decoder_submitted_dt'].iloc[0]
            #     except IndexError:
            #         print(f"Plan Id {planid} does not exist in the JSON data. Skipping.\n")
            #         continue
            #     if (decoder_submitted_dt == None) & (days_active < 6):
            #         print(f"{planid} {plan_name} has an None decoder_submitted date and client still has time to submit information. Skipping.\n")
            #         continue
                    
            #     # Client waited too long to provide information. Advance the task to 'Authentication Follow Up'
            #     elif (decoder_submitted_dt == None) & (days_active > 5):
                    
            #         print(f"{planid} waited too long to provide data. Pushing the project into the follow-up task")
                    
            #         novalink_project_tasks = pp.get_task_groups_by_projectid(projectid,expand="Tasks")
            #         populating_paycode_mapping_file_task_group = [i for i in novalink_project_tasks if i["Name"] == 'Populating Paycode Mapping File'][0]
            
            #         # Override *up to* 'Follow-up with Client' task.
            #         for task in populating_paycode_mapping_file_task_group['Tasks']:
            #             if task["TaskName"] == 'Follow-up with Client':
            #                 break
            #             pp.override_task(task['Id'])
            #         continue
                    
                    
            #     # Found the 'decoder_submitted_dt' information for the planid in the json data. Move forward. You dont have to wait 5 days.    
            #     print(f"Advancing {planid} {plan_name} from WAIT task.")
            #     pp.override_task(taskid)
            #     print("\n")
            
            
            # In[28]:
            
            
            NOVALINK_WORKTRAY = pp1.get_worktray2("Novalink")
            
            filt1 = NOVALINK_WORKTRAY["task_name"] == "Check For Paycode Mapping File"
            filt2 = NOVALINK_WORKTRAY["proj_name"].str.lower() == "novalink payroll access setup"
            filt3 = NOVALINK_WORKTRAY["planid"] != '99205'
            
            NOVALINK_WORKTRAY = NOVALINK_WORKTRAY[filt1 & filt2 & filt3]
            
            if len(NOVALINK_WORKTRAY) == 0:
                raise SystemExit("No worktray items.")
                
            NOVALINK_TEST_FILE_PROJECT_ID = pp.get_project_template_by_name("Novalink Test File")[0]["Id"]
            TODAY = datetime.datetime.now().strftime("%m/%d/%y")
            TODAY_PLUS_10_DAYS = (datetime.datetime.now() + datetime.timedelta(days=10)).strftime("%m/%d/%y")
            
            
            #NOVALINK_WORKTRAY['runtime'] = datetime.datetime.now().strftime("%m/%d/%Y %H:%M:%S")
            NOVALINK_WORKTRAY
            
            
            # In[ ]:
            
            
            
            
            
            # In[29]:
            
            
            for i in NOVALINK_WORKTRAY.index[:]:
                
                planid = NOVALINK_WORKTRAY.at[i,'planid']
                projectid = NOVALINK_WORKTRAY.at[i,'projid']
                taskid = NOVALINK_WORKTRAY.at[i,'taskid']
                
                
                try:
                    decoder_submitted_dt = json_data_df.loc[json_data_df['invitation_cd'] == planid,'decoder_submitted_dt'].iloc[0]
                except IndexError:
                    print(f"Plan Id {planid} does not exist in the JSON data. Skipping.")
                    continue
                if decoder_submitted_dt == None:
                    print(f"{planid} has an None authorization date.")
                    pp.override_task(taskid)
                    continue
                    
                
                # Decoder submitted used to be utc and I had to convert it to Central. Now its just a straight date. This conversion saved here just in case. 
                #decoder_submitted_dt =  convert_utc_into_central(decoder_submitted_dt) # Convert to datetime
                decoder_submitted_dt =  datetime.datetime.strptime(decoder_submitted_dt,"%Y-%m-%d")
                decoder_submitted_dt =  decoder_submitted_dt.strftime("%m/%d/%Y")
                
                
                print(f"{planid} submitted paycode mapping file on {decoder_submitted_dt}")
                
                
                # Get the task item as a dictionary and use it as a payload for the PUT request.
                target_task_item_dict = [i for i in pp.get_task_items_by_taskid(taskid) if i["Question"] == 'Date Authorization Completed'][0]
                target_task_item_dict["Value"] = decoder_submitted_dt
                pp.put_taskitem(target_task_item_dict)
                print(f"Task item updated with value {decoder_submitted_dt}.")
                
                
                print("Closing project.")
                closed_task_ids = close_project(projectid)
                
                novalink_test_file_projects = [i for i in pp.get_projects_by_planid(planid) if i["Name"].lower() == 'novalink test file' and i["CompletedOn"] == None]
                if len(novalink_test_file_projects) == 0:
                    pp.add_project(planid, NOVALINK_TEST_FILE_PROJECT_ID,TODAY,TODAY_PLUS_10_DAYS)
                    print("Launched Novalink Test Project.")
                else:
                    print(f"Novalink Test File project already exists for {planid}. Skipping.")
                    
                print('\n')
            
            
            # In[ ]:
            
            
            raise SystemExit("Done")
            
            
            # In[30]:
            
            
            "HELLO".lower()
            
            
            # In[ ]:
            
            
            [i for i in pp.get_projects_by_planid('99205') if i["Name"] == 'Novalink Test File' and i["CompletedOn"] == None]
            
            
            # In[ ]:
            
            
            83605
            
            
            # In[ ]:
            
            
            NOVALINK_TEST_FILE_PROJECT_ID
            
            
            # In[ ]:
            
            
            novalink_project_tasks = pp.get_task_groups_by_projectid(9661112,expand="Tasks")
            populating_paycode_mapping_file_task_group = [i for i in novalink_project_tasks if i["Name"] == 'Populating Paycode Mapping File'][0]
            
            
            # In[ ]:
            
            
            populating_paycode_mapping_file_task_group
            
            
            # In[ ]:
            
            
            
            
            
            # In[ ]:
            
            
            TODAY = datetime.datetime.now().strftime("%m/%d/%y")
            TODAY_PLUS_10_DAYS = (datetime.datetime.now() + datetime.timedelta(days=10)).strftime("%m/%d/%y")
            
            
            # In[ ]:
            
            
            TODAY
            

    schedule_logger('Finished', pyfile= __file__)

except SystemExit:
    schedule_logger('Finished', pyfile= __file__)
    raise
    
except Exception as e:
    email_error_to_automation(e, __file__)
    schedule_logger('Error', pyfile= __file__)

            