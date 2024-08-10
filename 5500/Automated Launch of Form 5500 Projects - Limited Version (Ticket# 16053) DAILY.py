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
            
            
            # Approved for straight run-through
            
            from datetime import datetime as dt2
            from IPython.core.display import display,HTML
            import os
            import pandas as pd
            import requests
            import shutil
            import time
            import sys
            
            sys.path.insert(0, r'C:\Users\Public\WPy64-39100\notebooks\Lam Hoang')
            
            import lam
            import pensionpro as pp
            
            #display(HTML('<style>.container{width:100%}</style'))
            
            pd.set_option('display.max_columns',None)
            pd.set_option('display.max_rows',None)
            
            exe = r"Y:\Automation\Projects\Active\Asset Reconciliation Report from ASC\Mapping Asset Recon Report.exe.lnk"
            
            project_folder = r'Y:\Automation\Projects\Active\Automated Launch of Form 5500 Projects (Ticket# 16053)'
            os.chdir(project_folder)
            
            log = '5500 Projects Launched.pkl'
            excel_log = 'Log.xlsx'
            
            #date_to_start_check = '6/26/2023'
            date_to_start_check = '1/1/2024'
            
            this_year = dt2.today().year
            
            today = dt2.today().strftime('%m/%d/%Y')
            
            
            # In[2]:
            
            
            def check_for_conflicting_5500_projects(planid, period_end):
                """
                Checks for existing 5500 projects with the specified period end dates. Please use pd.to_datetime() for the period_end
                parameter and turn it into a timestamp object.
                
                Added at the request of Jeremiah by Andrew on 9/13/2023. Issues with duplicate 5500 projects being added for a plan.
                Some 5500 projects dont have safeguards in the template to prevent duplicate launches.
                
                """
                
                projects_df = pd.DataFrame.from_dict(pp.get_projects_by_planid(planid))
                projects_df["PeriodEnd"] = pd.to_datetime(projects_df["PeriodEnd"], format= "%m/%d/%Y %I:%M:%S %p")
                projects_df.sort_values(by="PeriodEnd", inplace=True)
            
                project_5500_df = projects_df.loc[projects_df['Name'].str.contains("Annual Government", case=False)]
            
                if len(project_5500_df.loc[project_5500_df["PeriodEnd"] >= period_end]):
                    return True
                return False
            
            
            # In[3]:
            
            
            # df_log is a pickle file of all the 5500 projects that are already launched or have been looked. 
            # Some do not need launching because the 5500 Form type for that plan suggests that it does not need a 5500 project
            time.sleep(3)
            df_log = pd.read_pickle(log)   
            len(df_log)
            
            
            # In[5]:
            
            
            # Generate a blank df to add new projects that we have not looked at
            time.sleep(3)
            df = df_log.iloc[0:0]
            
            
            # In[6]:
            
            
            time.sleep(3)
            
            # Try 15 times
            for i in range(15):
                try:
                    print('Getting tasks 1-1000...')
                    tasks = pp.get_tasks(filters=f"TaskName eq 'Deliver Report to Client'"+
                                         f" and DateCompleted gt '{date_to_start_check}'",
                                         expand='Taskgroup.Project')
            
                    # To get total number of tasks. Pension Pro API only allows for 1000 items per call
                    loop = True
                    skip = 0
            
                    while len(tasks)%1000 == 0 and loop == True:
                        skip += 1000
            
                        print(f'Getting tasks {skip} to {skip+1000}...')
                        tasks_to_add = pp.get_tasks(filters=f"TaskName eq 'Deliver Report to Client'" +
                                                    f" and DateCompleted gt '{date_to_start_check}'",
                                                    expand='Taskgroup.Project',
                                                    skip=skip)
                        tasks.extend(tasks_to_add)
            
                        if len(tasks_to_add) < 1000:
                            loop = False
                    
                except Exception:
                    time.sleep(pause := i**2)
                    print(f'{pause = }')
                    if i == 14:
                        raise
                    continue
                else:
                    break
            
            
            print('Tasks found:',len(tasks))
            time.sleep(1)
            print(f'{dt2.now():%F %T}')
            
            
            # In[7]:
            
            
            time.sleep(3)
            # Create a map of form 5500 type ID to the form type name
            form_5500_types = requests.get('https://api.pensionpro.com/v1/plans/form5500types',headers=pp.headers).json()['Values']
            map_form_5500_type_id_to_name = {}
            
            for item in form_5500_types:
                form_type_name = item['DisplayName']
                form_5500_type_id = item['Id']
                map_form_5500_type_id_to_name[form_5500_type_id] = form_type_name
                
            time.sleep(1)
            
            map_form_5500_type_id_to_name
            
            
            # In[ ]:
            
            
            
            
            
            # Notes:
            # Only launch:
            #           '5500 SF':'DC Annual Governmental Forms - Small Filer' ->
            #           '5500 SF':'DC Annual Governmental Forms - Small Filer (Automated)'
            
            # In[8]:
            
            
            time.sleep(3)
            # Create a map of proj to launch based on form 5500 type. This info is given in documentation
            
            ####### OLD VERSION #######
            # map_form_5500_type_to_proj_template = {
            #     '5500 EZ':'DC Annual Governmental Forms - 5500-EZ, Owner Only Filer',
            #     'Owner only < $250,000':'DC Annual Governmental Forms - 5500-EZ, Owner Only Filer Under $250,000',
            #     'Large Plan Filer':'DC Annual Governmental Forms - Audit',
            #     '5500 SF':'DC Annual Governmental Forms - Small Filer',
            #     '5500 Sch. I':'DC Annual Governmental Forms - Small Filer'
            # }
            
            # Limiting it to audited plans
            map_form_5500_type_to_proj_template = {
                'Large Plan Filer':'DC Annual Governmental Forms - Audit',
                '5500 SF':'DC Annual Governmental Forms - Small Filer (Automated)',
                '5500 EZ':'DC Annual Governmental Forms - 5500-EZ, Owner Only Filer (Automated)',
                'Owner only < $250,000':'DC Annual Governmental Forms - 5500-EZ, Owner Only Filer Under $250,000 (Automated)'}
            
            time.sleep(1)
            
            map_form_5500_type_to_proj_template
            
            
            # In[9]:
            
            
            time.sleep(3)
            # Create a map of form 5500 project template to template ID
            # project_names = [    
            #     'DC Annual Governmental Forms - 5500-EZ, Owner Only Filer',
            #     'DC Annual Governmental Forms - 5500-EZ, Owner Only Filer Under $250,000',
            #     'DC Annual Governmental Forms - Audit',
            #     'DC Annual Governmental Forms - Small Filer',
            #     'DC Annual Governmental Forms - Small Filer']
            
            # Limiting it to audited plans
            project_names = ['DC Annual Governmental Forms - Audit',
                             'DC Annual Governmental Forms - Small Filer (Automated)',
                            'DC Annual Governmental Forms - 5500-EZ, Owner Only Filer (Automated)',
                            'DC Annual Governmental Forms - 5500-EZ, Owner Only Filer Under $250,000 (Automated)']
            
            map_proj_template_to_id = {}
            
            for item in project_names:
                template_id = pp.get_project_template_by_name(item)[0]['Id']
                map_proj_template_to_id[item] = template_id
            
            time.sleep(1)   
                
            map_proj_template_to_id
            
            
            # In[10]:
            
            
            print('Tasks found:',len(tasks))
            
            
            # In[12]:
            
            
            time.sleep(3)
            
            # Populate df
            tasks_already_checked = list(df_log['task_id'])
            
            for task in tasks[:]:
                   
                index = tasks.index(task)
                task_name = task['TaskName']
                task_id = task['Id']
                proj_name = task['TaskGroup']['Project']['Name']       
                proj_id = task['TaskGroup']['Project']['Id']    
                pp_plan_id = task['TaskGroup']['Project']['PlanId']
                    
                
                # Skip if project name does not contain 'DC Annual Administration'
                if 'DC Annual Administration' not in proj_name:
            #         print(f'index {index} of {len(tasks)-1}: task ID: {task_id}')        
                    print(f'\tSKIP: project name: {proj_name}')
                    continue
                    
                # Skip if task already checked based on task ID's found in df_log:
                if task_id in tasks_already_checked:
                    print(f'index {index} of {len(tasks)-1}: task ID: {task_id}')                
                    print(f'\tSKIP: task already checked: {task_id}')
                    continue    
            
                try:
                    plan = pp.get_plan_by_pp_plan_id(pp_plan_id)
            
                except Exception:
                    time.sleep(2)
                    plan = pp.get_plan_by_pp_plan_id(pp_plan_id)
                
                plan_id = plan['InternalPlanId']
                plan_name = plan['Name']
             
                
                print(f'index {index} of {len(tasks)-1}: plan {plan_id} {plan_name} - task ID: {task_id}')
                
                    
                task_completed_date = dt2.strptime(task['DateCompleted'].split('T')[0],'%Y-%m-%d').strftime('%m/%d/%Y')
                period_start = task['TaskGroup']['Project']['PeriodStart'].split()[0]
                period_end = task['TaskGroup']['Project']['PeriodEnd'].split()[0]
                
                timestamp = dt2.today().strftime('%m/%d/%Y %#I:%m %p')
                
                form_5500_type_id = plan['FilingStatusId']
                
                form_5500_type = map_form_5500_type_id_to_name[form_5500_type_id]
                
                
                
                if form_5500_type in ['N/A','No 5500/Non-electing Church','No Filing Required','SB only']:
            #     if form_5500_type != 'Large Plan Filer':
                
                    print(f'\tSKIP: Form 5500 type insignficant')
                    
                    proj_5500_launched = f'SKIP: Form 5500 type insignificant'
                    proj_5500_template_id = ''
            
                else:
                    if not (proj_5500_launched := map_form_5500_type_to_proj_template.get(form_5500_type)):
                        continue
                    if not (proj_5500_template_id := map_proj_template_to_id.get(proj_5500_launched)):
                        continue
                        
            #     set_proj_5500_template_id.add(proj_5500_template_id)
            
                df = pd.concat([df,pd.DataFrame({
                    'timestamp' : [timestamp], 
                    'plan_id' : [plan_id], 
                    'plan_name' : [plan_name], 
                    'proj_name' : [proj_name], 
                    'proj_id' : [proj_id], 
                    'task_name' : [task_name],
                    'task_id' : [task_id], 
                    'task_completed_date' : [task_completed_date],
                    'form_5500_type': [form_5500_type],
                    'proj_5500_launched' : [proj_5500_launched],
                    'proj_5500_template_id' : [proj_5500_template_id], 
                    'proj_5500_launched_id' : [''],
                    'period_start' : [period_start],
                    'period_end' : [period_end]
                    })],ignore_index=True)       
            
                
                
                
            #     # Clear variables
            #     del (timestamp, plan_id, plan_name, proj_name, proj_id, task_name, task_id, task_completed_date, form_5500_type,
            #     proj_5500_launched, proj_5500_template_id, period_start, period_end)
            
            df = df.fillna('')
                
            print('\nDone')
            
            
            # In[ ]:
            
            
            
            
            
            # In[13]:
            
            
            df.tail()
            
            
            # In[26]:
            
            
            df.tail(4)
            
            
            # In[30]:
            
            
            # time.sleep(3)
            # # Launch projects
            # # NOTE: For whoever is backing up Lam, for the ones that error out, just delete the row from df and continue
            # time.sleep(1)
            # for i in df.index[:]:   # <---------------------- Check index before starting
            #     plan_id = df.at[i,'plan_id']
            #     plan_name = df.at[i,'plan_name']
            #     proj_5500_template_id = df.at[i,'proj_5500_template_id']
            #     period_start = df.at[i,'period_start']
            #     period_end = df.at[i,'period_end']
            #     proj_to_launch = df.at[i,'proj_5500_launched']
                
            #     print(f'index {i} of {len(df)-1}: plan {plan_id} {plan_name}')
                
            #     if proj_5500_template_id == '':
            #         print('\tSKIPPED')
            #         continue
                    
            #     if df.proj_5500_launched_id.at[i] == 'Already launched manually':
            #         print('\tSKIP: Project already launched manually\n')
            #         continue
                
            #     r = False
                
            #     # Launch project
            # #     time.sleep(1)
                
            
            
            # ## < temp
            
            # In[16]:
            
            
            time.sleep(3)
            # Launch projects
            # NOTE: For whoever is backing up Lam, for the ones that error out, just delete the row from df and continue
            time.sleep(1)
            for i in df.index[:]:   # <---------------------- Check index before starting
                plan_id = df.at[i,'plan_id']
                plan_name = df.at[i,'plan_name']
                proj_5500_template_id = df.at[i,'proj_5500_template_id']
                period_start = df.at[i,'period_start']
                period_end = df.at[i,'period_end']
                proj_to_launch = df.at[i,'proj_5500_launched']
                
                print(f'index {i} of {len(df)-1}: plan {plan_id} {plan_name}')
                
                # Added at the request of Jeremiah by Andrew on 9/13/2023. Issues with duplicate 5500 projects being added for a plan.
                # Some 5500 projects dont have safeguards in the template to prevent duplicate launches.
                try:
                    if check_for_conflicting_5500_projects(plan_id, pd.to_datetime(period_end)):
                        print(f'\tSKIPPED. Conflicting 5500 exists with a Period End greater than or equal to {period_end}')
                        continue
                except: # Primarily used to skip "Plan ID not in system" exceptions within the check_for_conflicting_5500_projects() function.
                    continue
                
                if proj_5500_template_id == '':
                    print('\tSKIPPED')
                    continue
            
            #     if (proj_5500_template_id != 87859 or proj_5500_template_id != 88189):
            #         print('\tSKIPPED b/c not "DC Annual Gov Form Audit" or SF')
            #         continue
                    
            #     if proj_5500_template_id != 81111:
            #         print('\tSKIPPED b/c not "DC Annual Gov Form Audit"')
            #         continue
                    
                if df.proj_5500_launched_id.at[i] == 'Already launched manually':
                    print('\tSKIP: Project already launched manually\n')
                    continue
                
                r = False
                
                # Launch project
                time.sleep(3)
                ###***
                
            
                try:
                    r = pp.add_project(
                        planid = plan_id,
                        ProjectTemplateId = str(proj_5500_template_id),
                        StartDate = today,
                        PeriodStart = period_start,
                        PeriodEnd = period_end,
                    )
                
                except Exception as e:  # Check to see if project already exists
                    proj_already_exists = False
                    
                    projects = False
                    
                    try:
                        time.sleep(1)
                        projects = pp.get_projects_by_planid(plan_id)
                    except:
                        df.proj_5500_launched_id.at[i] = f'Plan ID not found in system.'
                        continue
                    
                    for proj in projects:
                        if proj['Name'] == proj_to_launch and (proj['PeriodEnd'].split()[0] == period_end or proj['PeriodStart'].split()[0] == period_start):
                            proj_already_exists = True
                            break
            
                    if proj_already_exists:
                        df.proj_5500_launched_id.at[i] = 'Already launched manually'
                        print('\tSKIP: Project already launched manually\n')
                        continue
                    else:
                        #raise Exception(f'Check plan {plan_id}. There doesn\'t seem to be a project already launched but it still errored.')
                        df.proj_5500_launched_id.at[i] = f'Error launching: {e}.'
                        continue
                
                if r:
                    print('\tProject launched')
                
                    # Get ID of project launched
                    df.at[i,'proj_5500_launched_id'] = r['Id']
                    #df.at[i,'due_on'] =  r['DueOn'].split()[0]
                
            #     # Clear variables
            #     del plan_id, plan_name, proj_5500_template_id, period_start, period_end 
            
            df = df.fillna('')
            print('\nDone')
            
            
            # In[26]:
            
            
            df
            
            
            # In[25]:
            
            
            time.sleep(3)
            
            client_folder_not_found = []
            
            # Create 5500 folder and copy exe file to the folder
            time.sleep(1)
            for i in df.index[:]:    # <---------------------- Check index before starting
                plan_id = df.at[i,'plan_id']
                plan_name = df.at[i,'plan_name']
                
                print(f'index {i} of {len(df)-1}: plan {plan_id} {plan_name}')
                
                try:
                    time.sleep(1)
                    client_folder = lam.get_nova_client_folder_by_plan_id(plan_id)
                except:
                    client_folder_not_found.append(plan_id)
                    print('\t!!!!!!!!!!!!!!!!!!!!!!! SKIPPED: client folder could not be found !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
                    continue
                
                
                if df.proj_5500_launched_id.at[i] == '':
                    print('\tno projects were launched')
                    continue
                    
                period_start_year = dt2.strptime(df.at[i,'period_start'],'%m/%d/%Y').year    
                    
                
                folder_5500 = f'{client_folder}/{period_start_year}/5500'
                exe_path = f'{folder_5500}/Mapping Asset Recon Report.exe.lnk'
                
                # Check to see if folder exists. If not, create it.
                if os.path.exists(folder_5500):
                    print('\t5500 folder already exists :',folder_5500)
                else:
                    print('\t5500 folder DOES NOT EXIST')
                    os.mkdir(folder_5500)
                    
                    if os.path.exists(folder_5500):
                        print('\tfolder created.')
                    else:
                        raise Exception('Folder failed to create')
                
                # Check to see if the EXE exists. Most likely not. If so, copy the exe to the 5500 folder
                if os.path.exists(exe_path):
                    print('\tEXE file already exists :',exe_path)
                else:
                    print('\tEXE file DOES NOT EXIST')
                    shutil.copy(exe,exe_path)
                    
                    if os.path.exists(exe_path):
                        print('\tEXE copied.')
                    else:
                        raise Exception('EXE failed to copy')            
                    
                del folder_5500, exe_path
                    
            print('\nDone')
            #     print(os.path.exists(folder_5500),':',folder_5500)
            
            
            # In[ ]:
            
            
            #df
            
            
            # In[ ]:
            
            
            # # If there are any client folders not found, please let Kim know by adding a comment to samanage ticket# 16053
            # if len(client_folder_not_found) > 0:    # This list should be empty
            #     raise Exception('There are client folders not found')
            
            
            # In[ ]:
            
            
            #client_folder_not_found
            
            
            # In[ ]:
            
            
            # Temp: Check which client folders could not be found
            # for i in df.index[:]:
            #     plan_id = df.plan_id.at[i]
            #     print(f'index {i} of {len(df)-1}')
                
            #     try:
            #         lam.get_nova_client_folder_by_plan_id(plan_id)
            #         print('\tfound')
            #     except:
            #         print('\tNOT FOUND')
            #         client_folder_not_found.append(plan_id)
            
            
            # In[ ]:
            
            
            #len(df_log), len(df)
            
            
            # In[34]:
            
            
            # 7/1/24: There was an issue where the script ran for a group of projects but its associated project launched was turned off.
            # This resulted in the projects being added to the df_log so it counted as being checked. When the time came to
            # actually add a project for these plans, it would skip it! I had to use a date filter (starting from 3/1/24) and chop
            # off a significant portion of the df_log. I had to do this because I had no idea which plans were incorrectly skipped.
            # Then I got to thinking that keeping a log for years back is pointless. I am saving the chopped
            # df_log and renaming the old one with the "old_" prefix. 
            
            
            # For your reference. Log path: "Y:\Automation\Projects\Active\Automated Launch of Form 5500 Projects (Ticket# 16053)"
            
            
            time.sleep(1)
            if len(df) > 0:
                # Concatenate df to df_log and save to pickle file
                df = pd.concat([df_log,df],ignore_index=True)
            
                len(df)
            
                # Save to pickle file and excel file
                df.to_pickle(log)
            
                df_log = pd.read_pickle(log)
                if len(df_log) == len(df):
                    df.to_excel(excel_log)
                    print('df saved to pickle and excel file successfully') 
                else:
                    raise Exception(f'The length of the df_log {len(df_log)} does not match what the length should be: {len(df)}.')
            
            else:
                print('There was nothing new to log')
                
            
            timestamp = dt2.today().strftime('%m/%d/%Y %I:%M %p')
            print(timestamp)
            
            
            # # Troubleshoot
            
            # In[ ]:
            
            
            # for task in tasks:
                   
            #     index = tasks.index(task)
            #     task_name = task['TaskName']
            #     task_id = task['Id']
            #     pp_plan_id = task['TaskGroup']['Project']['PlanId']
            # #     plan = pp.get_plan_by_pp_plan_id(pp_plan_id)
            # #     plan_id = plan['InternalPlanId']
            # #     plan_name = plan['Name']
            # #     proj_name = task['TaskGroup']['Project']['Name']    
            # #     proj_id = task['TaskGroup']['Project']['Id']
                
            #     print(f'index {index} of {len(tasks)-1}')
            #     print('\t',period_start)
            
            
            # In[ ]:
            
            
            # # Update dueOn date
            # for i in df.index[1:]:
            #     print(f'index {i} of {len(df)-1}')
            #     proj_id = df.proj_5500_launched_id.at[i]
            #     proj = pp.get_project_by_projectid(proj_id)
                
            #     proj['DueOn'] = '10/15/2022'
                
            #     r = False
            #     r = pp.update_project(proj)
                
            #     if r:
            #         print('\tproj due on updated')
            #         df.due_on.at[i] = '10/15/2022'
            
            
            # In[ ]:
            
            
            # # Create blank pickle file [USE THIS ONLY IF REBUILDING df_log]
            # headers = ['timestamp','plan_id','plan_name','proj_name','proj_id','task_name','task_id','task_completed_date','form_5500_type','proj_5500_launched','proj_5500_template_id','proj_5500_launched_id','period_start','period_end']
            # df = pd.DataFrame(columns=headers)
            # df.to_pickle(log)
            
            # # Read from list to launch [This is only used when provided a list of projects to launch]
            # df2 = pd.read_excel(r"C:\Users\lhoang\Downloads\5500's to launch.xlsx")
            
            # df.plan_id = df2['TPA Plan ID']
            # df.plan_name = df2['Plan Name']
            # df.proj_name = df2['Project Name']
            # df.form_5500_type = df2['Form 5500 Type']
            # df.period_end = pd.to_datetime(df2['Period End']).dt.strftime('%m/%d/%Y')
            
            # df.fillna('',inplace=True)
            
            # # For imported Excel File ONLY
            
            # plans_not_found = []
            
            # for i in df.index[2140:]:       # <----------------   Don't forget to check index
            #     plan_id = df.plan_id.at[i]
            #     plan_name = df.plan_name.at[i]
            #     form_5500_type = df.form_5500_type.at[i]
            #     period_end = df.period_end.at[i]
            #     proj_name = df.proj_name.at[i]
                
            #     print(f'index {i} of {len(df)-1} - plan {plan_id} {plan_name}')
                
                
            #     if form_5500_type in ['N/A','No 5500/Non-electing Church','No Filing Required','SB only','']:
                
            #         print(f'\tSKIP: Form 5500 type insignficant')  
            
            #         df.proj_5500_launched.at[i] = 'SKIP: Form 5500 type insignificant'
            #         df.proj_5500_template_id.at[i] = ''
            #         continue
                
            #     try:
            #         plan = pp.get_plan_by_planid(plan_id,expand='Projects.Taskgroups')
            #     except:
            #         time.sleep(2)
            #         try:
            #             plan = pp.get_plan_by_planid(plan_id,expand='Projects.Taskgroups')
            #         except IndexError:
            #             plans_not_found.append(plan_id)
            #             print('\tPlan was not found.')
            #             continue
                        
            #     pp_plan_id = plan['Id']
                    
            #     projects = plan['Projects']
                
            #     for proj in projects:
                    
            #         task_groups = False
            #         task_group_id = False
            #         if proj['Name'] == proj_name and proj['PeriodEnd'].split()[0] == period_end:
            #             df.proj_id.at[i] = proj['Id']
            #             df.period_start.at[i] = proj['PeriodStart'].split()[0]
                        
            #             form_5500_proj_template = map_form_5500_type_to_proj_template[form_5500_type]
                        
            #             df.proj_5500_launched.at[i] = form_5500_proj_template
                        
            #             df.proj_5500_template_id.at[i] = map_proj_template_to_id[form_5500_proj_template]
                        
                        
            #             task_groups = proj['TaskGroups']
                        
            #             for task_group in task_groups:
            #                 if task_group['Name'] == 'Report Delivery':
            #                     task_group_id = task_group['Id']
            #                     break
            #             break
                
            #     try:
            #         tasks = pp.get_tasks_by_taskgroupid(task_group_id)
            #     except:
            #         time.sleep(2)
            #         tasks = pp.get_tasks_by_taskgroupid(task_group_id)        
                
            #     task_name = False
            #     task_id = False
            #     task_completed_date = False
                
            #     for task in tasks:
            #         if task['TaskName'] == 'Deliver Report to Client':
            #             df.task_name.at[i] = task['TaskName']
            #             df.task_id.at[i] = task['Id']
                        
            #             task_completed = task['DateCompleted'].split('T')[0].split('-')
                        
            #             df.task_completed_date.at[i] = task_completed[1] + '/' + task_completed[2] + '/' + task_completed[0]
            #             print('\ttask info updated')
            
            # print('\nDone!')
            
            # df.to_pickle('check.pkl')
            # df.to_excel('check.xlsx')
            
            
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

            