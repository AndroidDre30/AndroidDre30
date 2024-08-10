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
            
            
            import datetime
            import os
            import sys
            sys.path.insert(0, "Y:\Automation\Team Scripts\Andrew Kim\my modules")
            from glob import glob
            
            import pandas as pd
            import numpy as np
            
            import pensionpro_v1 as pp
            from IPython.display import display
            
            pd.set_option('display.max_rows',None)
            pd.set_option('display.max_columns',None)
            
            
            # In[2]:
            
            
            # My custom functions
            
            def dataframe_logger(dataframe, destination_path, trim_at = 100):
                """
                Requires os and pandas. Saves a dataframe as an excel file for error logging. If its found at the destination,
                it will concact onto it. It will trim the newly concatenated dataframe if there are more than 100 rows.
                
                """
                
                if os.path.exists(destination_path):
                    existing_excel_dataframe = pd.read_excel(destination_path)
                    
                    new_dataframe = pd.concat([dataframe,existing_excel_dataframe], ignore_index=True) # Old df tacked onto new df.
                    
                    # Trim old records so excel file doesn't become huge.
                    counter = 0
                    while len(new_dataframe) > int(trim_at): 
                        new_dataframe.drop(new_dataframe.index[-1], inplace=True)
                        counter += 1
                    if counter:
                        print(f"{counter} record(s) deleted.")
                        
                    new_dataframe.to_excel(destination_path, index = False)
                    print(f"Log file found. Concatenated to {destination_path}")
                    
                else:
                    dataframe.to_excel(destination_path, index = False)
                    print(f"New log created at {destination_path}")  
                    
                    
                    
            def find_task(project_id, task_group, task_name,skip_count=0, taskgroup_wildcard = False, task_wildcard = False):
                
                """
                Get the dictionary of the target task.
                Skip count is how many hits the function should skip in case there are duplicate task names in the same grouping. 
                
                Setting the wildcard option to True will use your inputted substring and see if its in the Taskgroup name.
                Not recommended unless the the substring contains rare names.
                
                """
                
                counter = 0
                for i in pp.get_project_by_projectid(project_id,expand="TaskGroups.Tasks")["TaskGroups"]:
                    if (i["Name"] == task_group) or (taskgroup_wildcard == True and task_group.lower() in i["Name"].lower()):
                        for j in i["Tasks"]:
                            if j["TaskName"] == task_name or (task_wildcard == True and task_name.lower() in i["Name"].lower()):
                                counter += 1
                                if counter <= skip_count:
                                    continue
                                else:
                                    return j  
                return {}
            
            
            def get_all_tasks_of_project(projectid : int) -> list:
                """
                Grabs all the tasks of a project and lines them up into a single list. The
                list will be filled with a dictionary representation of each task. This function also adds 
                the TaskGroupName key into each dictionary. 
                
                
                pp.get_task_groups_by_projectid already returns a "TaskGroup" key in each task
                but this is always None and I dont know why. Attempting to expand Tasks.TaskGroup 
                actually eliminates the entire key! No idea why. This function will manually add the key for the name
                of the task group. Dont use the key name "TaskGroup". I dont know whats going to happen
                in the future when someone attempts some sort of PUT action using this key. If they use the entire task 
                dictionary, PensionPro might freak out when their officially used key has some wonky value instead of None. 
                
                - Andrew 7/4/24
                """
                task_groups = pp.get_task_groups_by_projectid(projectid, expand="Tasks")
                
                tasks = []
                for task_group in task_groups:
                    for task in task_group["Tasks"]:
                        task["TaskGroupName"] = task_group["Name"]
                    tasks.extend(task_group["Tasks"])
                    
                return tasks
            
            
            # In[ ]:
            
            
            
            
            
            # In[ ]:
            
            
            
            
            
            # In[3]:
            
            
            _5500_AUTOMATION_WORKTRAY = pp.get_worktray("5500 Automation", get_all=True)
            _5500_AUTOMATION_WORKTRAY = _5500_AUTOMATION_WORKTRAY[_5500_AUTOMATION_WORKTRAY["task_name"] == "Confirm Plan is Balanced in ASC"]
            
            # Some projects have no PeriodStart
            _5500_AUTOMATION_WORKTRAY = _5500_AUTOMATION_WORKTRAY.loc[~_5500_AUTOMATION_WORKTRAY["per_start"].isna()] 
            
            _5500_AUTOMATION_WORKTRAY.reset_index(inplace=True)
            _5500_AUTOMATION_WORKTRAY = _5500_AUTOMATION_WORKTRAY.drop("index", axis=1)
            #_5500_AUTOMATION_WORKTRAY['period_start'] = pd.to_datetime(_5500_AUTOMATION_WORKTRAY['period_start'])
            _5500_AUTOMATION_WORKTRAY['runtime'] = ''
            _5500_AUTOMATION_WORKTRAY['overridden'] = ''
            _5500_AUTOMATION_WORKTRAY = _5500_AUTOMATION_WORKTRAY.astype(object)
            print(f"{len(_5500_AUTOMATION_WORKTRAY)} items in the worktray.")
            _5500_AUTOMATION_WORKTRAY
            
            
            # 
            
            # In[4]:
            
            
            # Per instructions received from Dawn and Andrea on June 2024, we are now checking to see if the target tasks are
            # completed in EITHER Annual Valuation Project OR the RK Project and overriding the worktray task if completed.
            
            for INDEX,ROW in _5500_AUTOMATION_WORKTRAY.iterrows():
            
                print(INDEX, ROW["planid"])
                ROW['runtime'] = datetime.datetime.now().strftime('%m/%d/%y %H:%M:%S')
                override_flag = False
            
                # Added on 7/10/23. Do not override if any 'Form 5500 Confirmations' projects are pending.
            #     if len(FORM_5500_CONFIRMATIONS := pp.get_projects_by_planid(planid=ROW['planid'], filters="contains(Name, 'Form 5500 Confirmations') and CompletedOn eq null")):
            #         print("Pending 'Form 5500 Confirmations' project. Skipping.")
            #         continue
            #     elif len(FORM_5500_CONFIRMATIONS) == 0:
            #         print("No pending 'Form 5500 Confirmations' projects.")           
                    
                
                # Check for target tasks in the Annual Valuations project.
                SMALL_FILER_START_DATE = datetime.datetime.strptime(ROW['per_start'], '%m/%d/%Y %I:%M:%S %p').strftime('%m/%d/%Y') # Get the start date of the project this task belongs to. 
                # If plan has a pending Annual Valuation project
                if len(ANNUAL_VALUATION_PROJECTS := pd.DataFrame.from_dict(pp.get_projects_by_planid(ROW['planid'], filters=f"contains(Name, 'annual valuation') and PeriodStart eq '{SMALL_FILER_START_DATE}'"))):
                    
                    # I dont think there can be multiple Annual Valuation projects with the same period start? I'll just pick the last one??? ¯\_(ツ)_/¯.
                    ANNUAL_VALUATION_PROJECT_ID = ANNUAL_VALUATION_PROJECTS.iloc[-1]["Id"] 
                    
                    all_annual_valuation_project_tasks = get_all_tasks_of_project(ANNUAL_VALUATION_PROJECT_ID)
                    
                    for task in all_annual_valuation_project_tasks:
                        if task['DateCompleted'] == None: # We only care about completed tasks.
                            continue
                            
                        if task["TaskGroupName"] == "Cash Reconciliation" and task["TaskName"] == "Escalated Review":
                            override_flag = True
                            break         
                        if task["TaskGroupName"] == "Asset Reconcilation" and task["TaskName"] == "Escalated Review":
                            override_flag = True
                            break  
                        if task["TaskGroupName"] == "Missing Contribution Verification" and task["TaskName"] == "Report Delivery":
                            override_flag = True
                            break  
                        if task["TaskGroupName"] == "Asset Reconciliation" and task["TaskName"] == "Report Delivery":
                            override_flag = True
                            break  
                        if task["TaskGroupName"] == "Valuation Preparation" and task["TaskName"] == "Report Delivery":
                            override_flag = True
                            break  
                        if task["TaskGroupName"] == "Cash Reconciliation" and task["TaskName"] == "Review - Valuation":
                            override_flag = True
                            break                  
                        if task["TaskGroupName"] == "Missing Contribution Verification" and task["TaskName"] == "Review - Valuation":
                            override_flag = True
                            break       
                            
                    else:
                        print("Pending 'Escalated Review' or 'Report Delivery' task in this Annual Valuation project.")
                    
                    if override_flag:
                        print(f"***{ROW['planid']} : Target task found in Annual Valuation project. Worktray task overridden.***")
                        pp.override_task(ROW['taskid'])
                        ROW['overridden'] = 'yes'
                        print('\n')
                        continue
            
                        
            #         # New requirements given on 8/7/23. In addition to searching for a non-complete "Escalated Review" ,
            #         # also search for these target tasks and see if they're completed.
            #         ESCALATED_REVIEW_TASK = [find_task(ANNUAL_VALUATION_PROJECT_ID, "Cash Reconciliation", "Escalated Review"), # 2 possible task groups containing Escalated Review.
            #                         find_task(ANNUAL_VALUATION_PROJECT_ID, "Asset Reconcilation", "Escalated Review"),# Reconcilation is spelled wrong. Someone might fix it in the future.
            #                                 find_task(ANNUAL_VALUATION_PROJECT_ID,"Missing Contribution Verification","Report Delivery"),
            #                                 find_task(ANNUAL_VALUATION_PROJECT_ID,"Asset Reconciliation","Report Delivery"),
            #                                 find_task(ANNUAL_VALUATION_PROJECT_ID,"Valuation Preparation","Report Delivery"),
            #                                 find_task(ANNUAL_VALUATION_PROJECT_ID,"Cash Reconciliation","Review - Valuation"),
            #                                 find_task(ANNUAL_VALUATION_PROJECT_ID,"Missing Contribution Verification","Review - Valuation"),] 
            
            #         ESCALATED_REVIEW_TASK = [i for i in ESCALATED_REVIEW_TASK if i]
            #         ANY_COMPLETED = any([True for i in ESCALATED_REVIEW_TASK if i and i["DateCompleted"] != None])
                    
            #         # New direction given by Jeremiah on 10/11/23. Previously, I needed to check if one of the target tasks above is
            #         # completed. Now, I'm finding projects with more than one of these when I thought they would be alone. 
            #         # If multiple of these tasks exists in a project and ANY of them are complete,  move forward.
            #         if ANY_COMPLETED == False: # If none are completed, skip plan.
            #             print("Pending 'Escalated Review' or 'Report Delivery' task in this Annual Valuation project.")
            #             continue
            
            #         pp.override_task(ROW['taskid'])
            #         ROW['overridden'] = 'yes'
            #         print('\n')
            #         continue
                
                
                
                # Added by ticket 24298. "Manual ASC Balance - Escalated Review [Manually by Hand] (FINAL)" must be completed before override.
                # I have no idea whether a plan can have more than 1 of these projects. Find all. Make sure they all have this completed.
                
                print("Target tasks not found in Annual Valuation project. Checking RK Project...")
                TASKS_COMPLETED = [] # Append True or False to this.
                ALL_RK_PROJECTS = pp.get_projects_by_planid(planid=ROW["planid"], filters=f"contains(Name, 'RK File Download, Import and Balancing') and PeriodStart eq '{SMALL_FILER_START_DATE}'")
                for RK_PROJECT in ALL_RK_PROJECTS:
                    MANUAL_ASC_BALANCE_TASK = find_task(RK_PROJECT['Id'], 'File Download and Balance', 'Manual ASC Balance - Escalated Review [Manually by Hand] (FINAL)')
                    if MANUAL_ASC_BALANCE_TASK.get("DateCompleted",False):
                        TASKS_COMPLETED.append(True)
                    else:
                        TASKS_COMPLETED.append(False)
                        
                if len(TASKS_COMPLETED) == 0: # all() will return True if empty. Check if it even found anything at all.
                    print(f"{ROW['planid']} : RK Project does not have a project with this target task. Skipping.")
                    continue
                if all(TASKS_COMPLETED) == False: 
                    print(f"{ROW['planid']} : RK Project has the task 'Manual ASC Balance - Escalated Review [Manually by Hand] (FINAL)' pending.")
                    print(f"Please investigate the 'RK File Download, Import and Balancing' project for this plan.\n")
                    continue
                                
                        
            #     # If override conditions are met, delete all files related to that specific plan under 
            #     # 'Y:\\ASC\Exported Reports\\5500 Automation\\All Output (new)'
            #     # This is to delete stale information in case a plan needs to be rolled back into this worktray. 
            #     if OLD_PLAN_FILES_TO_DELETE := glob(f"Y:\\ASC\Exported Reports\\5500 Automation\\All Output (new)\\{ROW['planid']}*"):
            #         print("Old files detected. Deleting...")
            #         [os.remove(i) for i in OLD_PLAN_FILES_TO_DELETE]
                
                print(f"*** {ROW['planid']} : RK Project with target task found. Worktray task overridden.***")
                pp.override_task(ROW['taskid'])
                ROW['overridden'] = 'yes'
                print('\n')
                
            
            
            # In[7]:
            
            
            print(f"Number of tasks overridden: {len(_5500_AUTOMATION_WORKTRAY.loc[_5500_AUTOMATION_WORKTRAY['overridden'] == 'yes'])}")
            
            
            # In[8]:
            
            
            dataframe_logger(_5500_AUTOMATION_WORKTRAY, '20353.xlsx', trim_at = 5000)
            raise SystemExit("Done")
            
            
            # In[ ]:
            
            
            troubleshooting_df = pd.read_excel(r"C:\Users\akim\Desktop\061924_troubleshooting.xlsx",d)
            
            
            # In[ ]:
            
            
            for index in _5500_AUTOMATION_WORKTRAY.loc[_5500_AUTOMATION_WORKTRAY['overridden'] == 'yes'].index:
                planid = _5500_AUTOMATION_WORKTRAY.at[index,'planid']
                taskid = _5500_AUTOMATION_WORKTRAY.at[index,'taskid']
                
            #     if OLD_PLAN_FILES_TO_DELETE := glob(f"Y:\\ASC\Exported Reports\\5500 Automation\\All Output (new)\\{ROW['planid']}*"):
            #         print("Old files detected. Deleting...")
            #         [os.remove(i) for i in OLD_PLAN_FILES_TO_DELETE]
                    
                pp.override_task(taskid)
            
            
            # In[ ]:
            
            
            len(_5500_AUTOMATION_WORKTRAY.loc[_5500_AUTOMATION_WORKTRAY['overridden'] == 'yes'])
            
            
            # In[ ]:
            
            
            _5500_AUTOMATION_WORKTRAY
            
            
            # In[ ]:
            
            
            # Removed this condition block because these types of projects no longer exists. Kept it here in case I need to add it again.    
                # Commented out as of 4/25/24 per ticket 24298. TED projects no longer exists!
                # Condition 1 checks. If a plan has any TED Balance projects and at least 1 is not completed, the task
                # should not be overridden.
            #     TED_BALANCE_PROJECTS1 = pd.DataFrame.from_dict(pp.get_projects_by_planid(planid=ROW['planid'], filters="contains(Name, 'ted balance') and CompletedOn eq null"))
            #     TED_BALANCE_PROJECTS2 = pd.DataFrame.from_dict(pp.get_projects_by_planid(planid=ROW['planid'], filters="contains(Name, 'Manual TED Upload and Balancing') and CompletedOn eq null"))
            #     if len(TED_BALANCE_PROJECTS1) or len(TED_BALANCE_PROJECTS2):
            #         print("Pending TED Balance project. Skipping.")
            #         continue
            #     elif len(TED_BALANCE_PROJECTS1 + TED_BALANCE_PROJECTS2) == 0:
            #         print("No pending TED Balance projects.")
            
            
            # In[ ]:
            
            
            _5500_AUTOMATION_WORKTRAY[_5500_AUTOMATION_WORKTRAY['overridden'] == 'yes'].to_excel('20353_debug.xlsx',index=False)
            
            
            # In[ ]:
            
            
            _5500_AUTOMATION_WORKTRAY
            
            
            # In[ ]:
            
            
            UNDO_DF = pd.read_excel('20353.xlsx')
            
            
            # In[ ]:
            
            
            UNDO_DF = UNDO_DF[UNDO_DF['runtime'] > datetime.datetime(2023,8,6)]
            
            
            # In[ ]:
            
            
            UNDO_DF = UNDO_DF[UNDO_DF['overridden'] == 'yes']
            
            
            # In[ ]:
            
            
            UNDO_DF
            
            
            # In[ ]:
            
            
            [pp.uncomplete_task(i) for i in UNDO_DF['taskid'].to_list()]
            
            
            # In[ ]:
            
            
            ANNUAL_VALUATION_PROJECT_ID = 9238469
            ESCALATED_REVIEW_TASK = [find_task(ANNUAL_VALUATION_PROJECT_ID, "Cash Reconciliation", "Escalated Review"), # 2 possible task groups containing Escalated Review.
                            find_task(ANNUAL_VALUATION_PROJECT_ID, "Asset Reconcilation", "Escalated Review"),# Reconcilation is spelled wrong. Someone might fix it in the future.
                                    find_task(ANNUAL_VALUATION_PROJECT_ID,"Missing Contribution Verification","Report Delivery")]
            
            
            # In[ ]:
            
            
            find_task(8571965,"Missing Contribution Verification","Report Delivery")
            
            
            # In[ ]:
            
            
            "Missing Contribution Verification","Report Delivery"
            "Asset Reconciliation","Report Delivery"
            "Valuation Preparation","Report Delivery"
            

    schedule_logger('Finished', pyfile= __file__)

except SystemExit:
    schedule_logger('Finished', pyfile= __file__)
    raise
    
except Exception as e:
    email_error_to_automation(e, __file__)
    schedule_logger('Error', pyfile= __file__)

            