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
            import pandas as pd
            import pensionpro_api as pp
            import json
            import datetime
            import os
            import time
            import openpyxl
            start = datetime.datetime.now()
            print(start)
            
            
            # In[2]:
            
            
            d12_df = pd.read_table(r'F:\ASC\USER\All ASC Cases - D12.TXT',index_col=None).astype(object)
            d18_df = pd.read_table(r'F:\ASC\USER\All ASC Cases - D18.TXT',index_col=None).astype(object)
            
            d12_df.rename(columns={'S:CLIENTNO': 'tpa_plan_id'}, inplace=True)
            d18_df.rename(columns={'S:CLIENTNO': 'tpa_plan_id'}, inplace=True)
            output_folder = r'Y:\ASC\Exported Reports\SSAPlanList.txt'
            asc_file_folder = r'Y:\ASC\Exported Reports\SSAs'
            d18_df
            
            
            # In[3]:
            
            
            # Fired - Pending Final Services - 12879, Pending Termination - 64622, Terminated - 12689
            plan_startus_id_list = [12879, 64622, 12689]
            error_path = r'Y:\Automation\Team Scripts\Anjana Shaji\8955 SSA DF\error_report.xlsx'
            cols = ['TPA Plan ID', 'ASC Client number Missing', 'ASC Text File Missing', 'Project Closed Out', 'DGEM Upload Error', 'DGEM Data Missing', 'Validation Error', 'Form Downloaded', 'Delivered to Client']
            df_error = pd.DataFrame(columns=cols)
            df_error.to_excel(error_path, index=False)
            
            
            # In[4]:
            
            
            def launch_project(tpa_id, period_end_date, period_start_date):
                project_name = "Form 8955-SSA (Automated)"
                data_json = pp.get_project_template_by_name(project_name)
                project_template_id = data_json[0]["Id"]
                show_on_PSL = True
                response_data = pp.add_project(planid = tpa_id, ProjectTemplateId = project_template_id, PeriodStart = period_start_date, PeriodEnd = period_end_date, EnableToShowOnPSLProjectsTab = show_on_PSL)
                print('Project launched successfully for plan: ', tpa_id)
            
            
            # In[5]:
            
            
            def pad_with_zeros(string):
                string = str(string)
                if len(string) < 4:
                    zeros_to_add = 4 - len(string)
                    padded_string = '0' * zeros_to_add + string
                    return padded_string
                else:
                    return string
            
            
            # In[6]:
            
            
            def check_if_asc_file_exists(plan_id, period_end_date, asc_file_list):
                found_flag = False
                file_name = None
                for file in asc_file_list:
                    file_plan_id = file.split("_")[0]
                    file_period_end = file.split("_")[1]
                    file_period_end_dt = datetime.datetime.strptime(file_period_end,'%m%d%Y')
                    file_period_end_dt = file_period_end_dt + datetime.timedelta(hours=12)
                    if file_plan_id == plan_id and file_period_end_dt == period_end_date:
                        found_flag = True
                        file_name = file
                return found_flag, file_name 
            
            
            # In[7]:
            
            
            def create_file_for_asc_wiz(asc_target):
                # Find ASC client id using TPA Plan ID.
                asc_client_id_list = []
                df_error_log = pd.read_excel(error_path)
                for plan_id in asc_target:
                    try:
                        plan_id = str(plan_id)
                        if plan_id in d12_df['tpa_plan_id'].to_list():
                            print("D12", plan_id)
                            asc_client_id = d12_df.loc[d12_df['tpa_plan_id'] == plan_id, '$PLANKEY'].values[0]
                            asc_client_id = pad_with_zeros(asc_client_id)
                            print('asc_client_id: ',asc_client_id)
                            text = f'D12:{asc_client_id}'
                            asc_client_id_list.append(text)
                        else:
                            print("D18", plan_id)
                            asc_client_id = d18_df.loc[d18_df['tpa_plan_id'] == plan_id, '$PLANKEY'].values[0]
                            asc_client_id = pad_with_zeros(asc_client_id)
                            print('asc_client_id: ',asc_client_id)
                            text = f'D18:{asc_client_id}'
                            asc_client_id_list.append(text)
                    except Exception as e:
                        print(e)
                        projects = pp.get_projects_by_planid(plan_id,filters=f"Name eq 'Form 8955-SSA (Automated)'")
                        project = [project for project in projects if project['CompletedOn'] is None]
                        if len(project) > 0:
                            project = project[0]
                        project_id = project['Id']
                        
                        task_group = pp.get_task_groups_by_projectid(project_id, expand = 'Tasks.Taskitems')[0]
                        for task in task_group['Tasks']:
                            if task['TaskName'] == 'Completion - Form 8955-SSA':
                                task_id = task['Id']
                                for task_item in task['TaskItems']:
                                    if task_item['ShortName'] == '8955 Validation Error' and task_item['Value'] == None:
                                        task_item['Value'] = 'Complete'
                                        pp.update_taskitem(task_item)
                                        note_text = 'ASC equivalent client number missing in D12 and D18'
                                        payload = {
                                            "ProjectID": project_id, 
                                            "NoteText": f"{note_text}",
                                            "NoteCategoryId": 3514,
                                            "ShowOnPSL": False
                                                }
                            
                                        x = pp.add_note(payload)
                                    elif task_item['ShortName'] == '8955 Validation Error' and task_item['Value'] == 'Complete':
                                        pp.override_task(task_id)
                        df_error_log.loc[df_error_log['TPA Plan ID'].astype(str).str.contains(plan_id), 'ASC Client number Missing'] = 'Yes'
                        continue
                print(df_error_log)
                df_error_log.to_excel(error_path, index = False)        
                with open(output_folder, 'w') as f:
                    for client_id in asc_client_id_list:
                        f.write(f"{client_id}\n")    
            
            
            # In[8]:
            
            
            def get_asc_files():
                asc_files = os.listdir(asc_file_folder)
                asc_file_list = [file for file in asc_files if 'FormSSA' in file and file.endswith('.txt') and not file.startswith('_')]
                return asc_file_list
            
            
            # In[9]:
            
            
            def get_ssa_and_asc_df(plan_dict):
                file_found_index = []
                file_not_found_index = []
                ssa_target = []
                asc_target = []
                plan_id_dict_asc = {}
                for plan_id in plan_dict:
                    period_end_date = plan_dict[plan_id]
                    period_end_date = datetime.datetime.strptime(period_end_date,'%m/%d/%Y %H:%M:%S %p')
                    asc_file_list = get_asc_files()
                    file_exists_flag, file_name = check_if_asc_file_exists(plan_id, period_end_date, asc_file_list)
                    if file_exists_flag:
                        print('ASC file found for plan: ', plan_id)
                        plan_id_dict_asc[plan_id] = file_name
                        ssa_target.append(plan_id)
                    else:
                        print('ASC file not found for plan: ', plan_id)
                        asc_target.append(plan_id)
                return ssa_target, asc_target, plan_id_dict_asc
            
            
            # In[10]:
            
            
            #do not run if project is launched by Jason
            today = datetime.date.today()
            two_days_ago = today - datetime.timedelta(days = 2)
            two_days_ago = datetime.datetime.strftime(two_days_ago, "%Y-%m-%d")
            print(two_days_ago)
            expand = 'TaskGroup.Project'
            filters = f"(TaskName eq 'Filing Invitation' and (TaskGroup.Project.Name eq 'DC Annual Governmental Forms - Schedule I' or TaskGroup.Project.Name eq 'DC Annual Governmental Forms - Small Filer (Automated)')  and DateCompleted ge '{two_days_ago}') or (TaskName eq 'Client Communications' and TaskGroup.Project.Name eq 'DC Annual Governmental Forms - Audit' and DateCompleted ge '{two_days_ago}')"
            tasks = pp.get_tasks(filters=filters, expand=expand, get_all=True)
            len(tasks)
            
            
            # In[11]:
            
            
            #do not run if project is launched by Jason
            today = datetime.date.today()
            yesterday = today - datetime.timedelta(days = 1)
            yesterday = datetime.datetime.strftime(yesterday, "%Y-%m-%d")
            print(yesterday)
            i = 0
            for task in tasks:
                date_completed = task['DateCompleted']
                date_completed = date_completed.rsplit('T')[0]
                # print(date_completed)
                if date_completed == yesterday:
                    i = i+1
            print(i)
            
            
            # In[12]:
            
            
            #do not run if project is launched by Jason
            plan_id_dict = {}
            today = datetime.date.today()
            yesterday = today - datetime.timedelta(days = 1)
            yesterday = datetime.datetime.strftime(yesterday, "%Y-%m-%d")
            for task in tasks:
                date_completed = task['DateCompleted']
                date_completed = date_completed.rsplit('T')[0]
                if date_completed == yesterday:
                    project = task['TaskGroup']['Project']
                    internal_plan_id = project['PlanId']
                    period_end_date = project['PeriodEnd']
                    period_start_date = project['PeriodStart']
                    plan = pp.get_plan_by_planid(internal_plan_id)
                    tpa_id = plan['InternalPlanId']
                    plan_status_id = plan['PlanStatusId']
                    try:
                        if plan_status_id in plan_startus_id_list:
                            print('Yes')
                            terminated_on_date = plan['TerminatedOn']
                            print(terminated_on_date)
                            if terminated_on_date < period_end_date:
                                print('Plan is exempted from launching project form 8955')
                                continue
                            else:
                                project_data = pp.get_projects_by_planid(tpa_id, filters= f"Name eq 'Form 8955-SSA (Automated)' and PeriodEnd eq '{period_end_date}'")
                                if not project_data:
                                    plan_id_dict[tpa_id] = period_end_date
                                    launch_project(tpa_id, period_end_date, period_start_date) 
                                else:
                                    print(f'Project form 8955 already found for plan: {tpa_id}')
                        else:
                            print('in else')
                            project_data = pp.get_projects_by_planid(tpa_id, filters= f"Name eq 'Form 8955-SSA (Automated)' and PeriodEnd eq '{period_end_date}'")
                            if not project_data:
                                plan_id_dict[tpa_id] = period_end_date
                                launch_project(tpa_id, period_end_date, period_start_date) 
                            else:
                                print(f'Project form 8955 already found for plan: {tpa_id}')
                    except Exception as e:
                        print(e)
                        project_data = pp.get_projects_by_planid(tpa_id, filters= f"Name eq 'Form 8955-SSA (Automated)' and PeriodEnd eq '{period_end_date}'")
                        if not project_data:
                            plan_id_dict[tpa_id] = period_end_date
                            try:
                                launch_project(tpa_id, period_end_date, period_start_date) 
                            except Exception as e:
                                print(e)
                                continue
                        else:
                            print(f'Project form 8955 already found for plan: {tpa_id}')
                        continue
                        
            print('Done!')
            
            
            # In[13]:
            
            
            #needs to be removed once the bulk launch is completed by Jason 
            df = pp.get_worktray('Automation', get_all=True)
            filt1 = df['task_name'] == 'Completion - Form 8955-SSA'
            filt2 = df['proj_name'] == 'Form 8955-SSA (Automated)'
            df = df[filt1 & filt2]
            print(len(df))
            
            
            # In[14]:
            
            
            df_error.dtypes
            
            
            # In[14]:
            
            
            plan_id_dict = {}
            plan_id_list = []
            for index, row in df.iterrows():
                    plan_id =  row['planid']
                    period_end_date = row['per_end']
                    plan_id_dict[plan_id] = period_end_date
                    plan_id_list.append(plan_id)
            df_error_log = pd.read_excel(error_path)
            df_error_log['TPA Plan ID'] = plan_id_list
            df_error_log.to_excel(error_path, index = False)
            plan_id_dict
            
            
            # In[15]:
            
            
            len(plan_id_dict)
            
            
            # In[16]:
            
            
            create_file_for_asc_wiz(plan_id_dict)
            
            
            # In[33]:
            
            
            df_error_log
            
            
            # In[ ]:
            
            
            done_file_flag = False
            while not done_file_flag:
                asc_files = os.listdir(asc_file_folder)
                asc_file_list = [file for file in asc_files if 'Done_' in file and file.endswith('.txt')]
                if asc_file_list:
                    print('Done file found!!!')
                    asc_file = asc_file_list[0]
                    done_file_flag = True
                    path = f'{asc_file_folder}\{asc_file}'
                    os.remove(path)
                    print('Removed done file')
                else:
                    print('Done file not found yet, waiting!!!')
                    time.sleep(600)
            
            
            # In[21]:
            
            
            end = datetime.datetime.now()
            diff = end - start 
            minutes = diff.total_seconds() / 60
            print('minutes: ',minutes)
            
            
            # In[ ]:
            
            
            
            

    schedule_logger('Finished', pyfile= __file__)

except SystemExit:
    schedule_logger('Finished', pyfile= __file__)
    raise
    
except Exception as e:
    email_error_to_automation(e, __file__)
    schedule_logger('Error', pyfile= __file__)

            