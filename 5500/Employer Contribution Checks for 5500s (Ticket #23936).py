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
            

            
            # In[54]:
            
            
            import os
            import shutil
            import time
            from xml.sax import saxutils
            import subprocess
            path_7zip = r"C:\Program Files\7-Zip\7z.exe"
            
            import nova as nova
            
            import re
            import sys
            import pickle
            
            from public_vault import Username, Password, OAuth
            sys.path.insert(0, "Y:\Automation\Team Scripts\Andrew Kim\my modules")
            
            from glob import glob
            from IPython.display import display
            
            
            from datetime import datetime
            from datetime import timedelta
            sys.path.insert(0, r'C:\Users\Public\WPy64-39100\notebooks\Lam Hoang')
            
            import lam
            
            import xlwt
            
            import pandas as pd
            import numpy as np
            import pensionpro as pp
            
            from tqdm import tqdm
            from pathlib import Path
            import smtplib
            
            
            # In[55]:
            
            
            now = datetime.now()
            previous_year = str(int(now.strftime("%Y"))-1)
            
            today = now.strftime('%Y-%m-%d_%H.%M.%S')
            
            
            # In[ ]:
            
            
            
            
            
            # In[56]:
            
            
            def get_tasks_for_er(name):
                
                filters = f'TaskName eq "{name}" and TaskActive ne null and DateCompleted eq null'
                expand = 'TaskGroup'
                
                tasks = []
                
                tasks_1 = pp.get_tasks(filters=filters, expand=expand)
                tasks.extend(tasks_1)
                
                skip_rows = 1000   
                
                while len(tasks_1) == 1000:
                    tasks_1 = pp.get_tasks(filters=filters, expand=expand, skip=skip_rows)
                    skip_rows+=1000
                    tasks.extend(tasks_1)
            
                projids = [task['TaskGroup']['ProjectId'] for task in tasks]
                
                a = [[task['TaskGroup']['ProjectId'],
                      task['Id'],
                      task['TaskName']] for task in tasks]
                
                df1 = pd.DataFrame(a, columns=['projid', 'taskid', 'task_name'])
            
                c = -(-(len(projids)) // 40)
                
                projs = []
                
                for i in range(c):
                    projidsa = projids[i*40:(i+1)*40]
                    filters = ' or '.join([f'ProjectId eq {projid}' for projid in projidsa])
                    expand = 'Plan.Client,Plan.MultipleEmployerPlan,Plan.Status,Plan.PlanType,Plan.PlanCategory,Plan.FilingStatus,Plan.PlanGroup'
                    projsa = pp.get_projects(filters=filters, expand=expand)
                    projs.extend(projsa)
                    
            
                a = [[proj['Id'],
                      proj['Plan']['InternalPlanId'],
                      proj['Plan']['Name'],
                      proj['Plan']['Client']['Id'],
                      proj['Name'],
                      proj['PeriodStart'],
                      proj['PeriodEnd'],
                      proj['Plan']['IsMultipleEmployerPlan'],
                      proj['Plan']['AddedOn'],
                      proj['Plan']['EffectiveOn'],
                      proj['Plan']['TerminatedOn'],
                      proj['Plan']['IRSPlanNumber'],
                      proj['Plan']['Status']['DisplayName'],
                      proj['Plan']['PlanCategory']['DisplayName'],
                      proj['Plan']['PlanType']['DisplayName'],
                      proj['Plan']['FilingStatus']['DisplayName'],
                      f"{proj['Plan']['MonthEnd']}/{proj['Plan']['DayEnd']}",
                      proj['Plan']['PlanGroup']['DisplayName']] for proj in projs]       
            
                cols = ['projid', 'planid', 'plan_name', 'client_id', 'proj_name', 'period_start', 'period_end', 'mep_status', 
                        'added_on','effective_on','terminated_on','irs_number','plan_status', 'plan_category', 
                        'plan_type', 'form5500', 'plan_end', 'plan_group']
                
                ## finish adding part to look up client name. do this by collating all client IDs and doing a filtered get_clients
                ## query, expanding information that we need
                
                df2 = pd.DataFrame(a, columns=cols)
            
                dfw = df1.merge(df2, on='projid')
                
                return dfw
            
            
            # In[ ]:
            
            
            
            
            
            # In[57]:
            
            
            # Get the worktray 
            df_auto = pp.get_worktray('Compare Contributions')
            # df_auto = get_tasks_for_er('Record-keeper/Loan Review')
            df_auto.reset_index(inplace=True, drop=True)
            
            
            # In[58]:
            
            
            df_auto
            
            
            # In[64]:
            
            
            # get list of all plans that have output in this folder, to exclude them from the ASC target list
            os.chdir('Y:/ASC/Exported Reports/Employer Contribution Check')
            
            all_output_folder = os.listdir()
            plans_with_asc_output = [file for file in all_output_folder if "_EmployerContribCheck.txt" in file]
            print(len(plans_with_asc_output))
            
            
            # In[65]:
            
            
            # create two dataframes 
            # first one will continue below
            # second one will be used here to generate a target list for the ASC script to get the output
            
            # lists for rows in dataframe to be split based on file match or no
            indices = []
            not_indices = []
            
            for i in df_auto.index:
                planid = df_auto.at[i,'planid']
                period_end = df_auto.at[i,'period_end']
                try:
                    period_end_dt = datetime.strptime(period_end,'%m/%d/%Y')
                except:
                    period_end_dt = datetime.strptime(period_end,'%m/%d/%Y %H:%M:%S %p')
                
                matching_file_found = False
                
                for file in plans_with_asc_output:
                    file_planid = file.split("_")[0]
                    file_periodend = file.split("_")[1]
                    file_periodend_dt = datetime.strptime(file_periodend,'%m%d%Y')
                        
                        
                    if file_planid == planid:
                        if file_periodend_dt != period_end_dt:            
                            file_periodend_dt = file_periodend_dt + timedelta(hours=12) #match file datetime to pensionpro dt
                            if file_periodend_dt == period_end_dt: 
                                matching_file_found = True                    
            
                        if file_periodend_dt == period_end_dt:
                            matching_file_found = True
                            
                if matching_file_found is True:
                    indices.append(i)
            
                else:
                    not_indices.append(i)
            
            df_erpull = df_auto.loc[indices]
            df_asc_target = df_auto.loc[not_indices]
            df_erpull0 = df_erpull[df_erpull['proj_name'] == 'DC Annual Governmental Forms - Audit']
            df_erpull0.reset_index(drop=True, inplace=True)
            
            
            # In[66]:
            
            
            df_erpull0
            
            
            # In[67]:
            
            
            len(df_erpull0), len(df_asc_target)
            
            
            # In[68]:
            
            
            # this next part generates a target list for ASC (same code as the testing target script)
            
            # NOTE THIS ALSO WILL NEED TO BE UPDATED FOR 2023, SINCE CURRENTLY THE ASCVal wizard is coded for 2022
            # probably can incorporate the target year into this file below in the future, so ASC grabs the target valdate
            # from the launched project period
            
            asc_target_planids = df_asc_target.planid.tolist()
            
            df_18 = pd.read_table(r'F:\ASC\USER\All ASC Cases - D18.TXT',index_col=None).astype(object)
            df_12 = pd.read_table(r'F:\ASC\USER\All ASC Cases - D12.TXT',index_col=None).astype(object)
            
            def add_header(df=None):
                header = ['$LIBKEY', '$LIBID', '$LIBDESC', '$PLANKEY', 'S:CLIENTNO']
                df = df.copy()
                if df.loc[0].to_list() == header:
                    df = df.drop(index=0)
                df.columns = header
                df = df.reset_index(drop=True)
                return df
            
            df_12 = add_header(df_12)
            df_18 = add_header(df_18)
            
            df_lookup = df_18.append(df_12)
            df_lookup.reset_index(drop=True, inplace=True)
            
            df_lookup['DISKPLAN'] = df_lookup["$LIBID"].astype(str) + ":" + df_lookup["$PLANKEY"].astype(str).str.zfill(4)
            df_lookup['S:CLIENTNO'] = df_lookup['S:CLIENTNO'].astype(str).astype(object)
            
            target_list = []
            plan_id_export = []
            
            for i in df_lookup.index:
                planid_lookup = str(df_lookup.loc[i, 'S:CLIENTNO'])
                diskplan = str(df_lookup.loc[i, 'DISKPLAN'])
                if planid_lookup in asc_target_planids:
                    target_list.append(diskplan)
                    plan_id_export.append(planid_lookup)
                    
            fname = "Y:/ASC/Exported Reports/5500_ER_Balance_Target_PlanList.txt"
            print(len(target_list))
            np.savetxt(fname, target_list, fmt='%s')
            
            
            # In[69]:
            
            
            # advance all the ones that are on the er_pull0 dataframe, then pull the new tasks, 
            # then filter by which new tasks were just advanced
            
            er_pull_targets = df_erpull0['projid'].tolist()
            
            for i in df_erpull0.index[:]:
            
                planid = df_erpull0.at[i,'planid']
                period_start = df_erpull0.at[i,'period_start']
                period_end = df_erpull0.at[i,'period_end']
                taskid = df_erpull0.at[i,'taskid']
                projid = df_erpull0.at[i,'projid']
                
                project_period_end = datetime.strptime(period_end,'%m/%d/%Y')
                
                pp.override_task(taskid)
            
            
            # In[72]:
            
            
            time.sleep(60) # for some reason the above takes time to propogate
            
            df_erpull1 = get_tasks_for_er('Record-keeper/Loan Review')
            df_erpull = df_erpull1[df_erpull1['projid'].isin(er_pull_targets)]
            # df_erpull = df_erpull1
            df_erpull.reset_index(inplace=True, drop=True)
            df_erpull
            
            
            # In[73]:
            
            
            df_erpull['review_needed'] = False
            df_erpull['error'] = False
            
            b=0
            ends_on = None
            
            no_count = 0
            yes_count = 0
            error_count = 0
            
            for i in df_erpull.index[b:ends_on]:
                os.chdir('Y:/ASC/Exported Reports/Employer Contribution Check')
                print(b)
                error = False
                
                planid = df_erpull.at[i,'planid']
                period_start = df_erpull.at[i,'period_start']
                period_end = df_erpull.at[i,'period_end']
                taskid = df_erpull.at[i,'taskid']
                projid = df_erpull.at[i,'projid']
                
                project_period_end = datetime.strptime(period_end,'%m/%d/%Y %H:%M:%S %p')
                
                # import ASC extract information
                asc_extract = [file for file in all_output_folder if "_EmployerContribCheck.txt" in file and file.startswith(f"{planid}_") and file.endswith(".txt")]
                if len(asc_extract) > 0:
                    for file in asc_extract:
                        file_periodend = file.split("_")[1]
                        file_periodend_dt = datetime.strptime(file_periodend,'%m%d%Y')
                        file_periodend_dt = file_periodend_dt + timedelta(hours=12)
                        if file_periodend_dt == project_period_end:
                            df_asc = pd.read_table(file, encoding_errors='ignore')          
                else:
                    error = True
                    
                if error != True:
                    prior_year_ps =  df_asc['Prior Year PS Contrib'].sum()
                    prior_year_match = df_asc['Prior Year Match Contrib'].sum()
                    current_year_ps_contrib = df_asc['Current Year PS Contrib'].sum()
                    current_year_match_contrib = df_asc['Current Year Match Contrib'].sum()
                    current_year_ps_value = df_asc['Current PS Value'].sum()
                    current_year_match_value = df_asc['Current Match Value'].sum()
            
                    er_contrib_check_required = False
            
                    # Plan has prior year PS contrib, and no current year PS value
                    if prior_year_ps != 0 and current_year_ps_value == 0:
                        er_contrib_check_required = True
            
                    # Plan has no prior year PS contrib, and current year PS value
                    if prior_year_ps == 0 and current_year_ps_value != 0:
                        er_contrib_check_required = True
            
                    # Plan has no current year match contrib but current year match value
                    if current_year_match_contrib == 0 and current_year_match_value != 0:
                        er_contrib_check_required = True
            
                    # Plan has current year match contrib but no current year match value
                    if current_year_match_contrib != 0 and current_year_match_value == 0:
                        er_contrib_check_required = True
            
                    df_erpull.at[i,'review_needed'] = er_contrib_check_required
                    
                else:
                    prior_year_ps =  "Error"
                    prior_year_match = "Error"
                    current_year_ps_contrib = "Error"
                    current_year_match_contrib = "Error"
                    current_year_ps_value = "Error"
                    current_year_match_value = "Error"
                    df_erpull.at[i,'review_needed'] = True
                    df_erpull.at[i,'error'] = True
                    error = True
                    er_contrib_check_required = True
            
                if error == True:
                    error_count += 1
                    
                if er_contrib_check_required == True:
                    populate_value = "Yes"
                    yes_count += 1
                    
                else:
                    populate_value = "No"
                    no_count += 1
                
                note_text_values = f"""This plan was identified as needing a review of employer contributions:<br>
                <br>
                    Prior Year PS Contrib: {prior_year_ps}<br>
                    Prior Year Match Contrib: {prior_year_match}<br>
                    Current Year PS Contrib: {current_year_ps_contrib}<br>
                    Current Year Match Contrib: {current_year_match_contrib}<br>
                    Current PS Value: {current_year_ps_value}<br>
                    Current Match Value: {current_year_match_value}<br>
                    """
                
                task_items = pp.get_taskitems_by_taskid(taskid)
                for task_item in task_items:
                    if task_item['ShortName'] == 'Potential Missing ER Contrib':
                        task_item.update({'Value':populate_value})
                        pp.put_taskitem(task_item)
            #             if populate_value == "No":
            #                 pp.override_task(taskid) # specs were unclear about this, they said "close out" but they
                            # apparently only meant the other task, super great very happy about this
                            
                        print(i, planid, task_item['Value'], er_contrib_check_required, error)
                        
                if populate_value == "Yes":
                    payload2 = {
                            "ProjectID": f'{projid}', 
                            "NoteText": note_text_values,
                            "ShowOnPSL": False
                                }
            
                    y = pp.add_note(payload2)
                b+=1
                
            print("yes count:", yes_count, "no count:", no_count, "error count:", error_count)
            
            
            # In[ ]:
            
            
            
            
            
            # In[74]:
            
            
            now = datetime.now()
            
            today = now.strftime('%Y-%m-%d_%H.%M.%S')
            
            # write logfile:
            
            df_erpull.to_excel(f'Y:/ASC/Exported Reports/Employer Contribution Check/Logging/{today}_EmployerContibCheck.xlsx')
            
            
            # In[43]:
            
            
            # df_rollback = pd.read_excel(r'Y:\Automation\Temp\Rollback.xlsx')
            # df_rollback
            
            
            # In[44]:
            
            
            # for i in df_rollback.index[1:]:
            #     planid = df_rollback.at[i,'TPA Plan ID']
            #     projects = pp.get_projects_by_planid(planid, filters="Name eq 'DC Annual Governmental Forms - Audit' and PeriodEnd eq '12/31/2023'", expand='TaskGroups.Tasks')
            #     if len(projects) > 0:
            #         project = projects[0]
            #     else:
            #         continue
                    
            #     for taskgroup in project['TaskGroups']:
            #         for task in taskgroup['Tasks']:
            #             if task['TaskName'] == 'Record-keeper/Loan Review':
            #                 pp.uncomplete_task(task['Id'])
            #                 print(task['Id'])
            
            
            # In[ ]:
            
            
            now = datetime.now()
            
            today = now.strftime('%Y-%m-%d_%H.%M.%S')
            
            pickle_dir = 'Y:/ASC/Exported Reports/Employer Contribution Check/Pickle'
            
            pickle_file = f'{pickle_dir}/{today}_ASCExport.pkl'
            
            asc_targets = df_asc_target.projid.tolist()
            
            with open(pickle_file, 'wb') as f:
                pickle.dump(asc_targets,f)
                
            last_pickles = os.listdir(pickle_dir)
            last_pickles.reverse()
            
            # if a project has been missing for three checks in a row, we advance.
            
            last_three_pickles = last_pickles[:3]
            
            pickle_check = []
            
            for file in last_three_pickles:
                with open(f'{pickle_dir}/{file}','rb') as f:
                    single_pkl = pickle.load(f)
                if pickle_check == []:
                    pickle_check = single_pkl       
                else:
                    pickle_check = [project for project in pickle_check if project in single_pkl]
                    
            print(len(pickle_check))
            
            # advance and mark ones that were unable to be pulled for three days in a row
            
            df_close_out = df_asc_target[df_asc_target['projid'].isin(pickle_check)]
                                         
            for i in df_close_out.index[:]:
            
                planid = df_close_out.at[i,'planid']
                period_start = df_close_out.at[i,'period_start']
                period_end = df_close_out.at[i,'period_end']
                taskid = df_close_out.at[i,'taskid']
                projid = df_close_out.at[i,'projid']                   
                
                pp.override_task(taskid)
                
            # Get the just-advanced tasks to mark as yes and put notes
            if len(df_close_out) > 0:
                df_pickle_mark = get_tasks_for_er('Record-keeper/Loan Review')
                df_pickle_mark = df_pickle_mark[df_pickle_mark['projid'].isin(pickle_check)]
                df_pickle_mark.reset_index(inplace=True, drop=True)
                # advance and mark ones that were unable to be pulled for three days in a row
            
                note_text1 = "Unable to pull ASC information to do an employer balance check per task item in 'Record-keeper/Loan Review' task- please complete manually"
            
                df_pickle_mark = df_pickle_mark[df_pickle_mark['projid'].isin(pickle_check)]
            
                for i in df_pickle_mark.index[:]:
            
                    planid = df_pickle_mark.at[i,'planid']
                    period_start = df_pickle_mark.at[i,'period_start']
                    period_end = df_pickle_mark.at[i,'period_end']
                    taskid = df_pickle_mark.at[i,'taskid']
                    projid = df_pickle_mark.at[i,'projid']                   
            
                    task_items = pp.get_task_items_by_taskid(taskid)
            
                    for task_item in task_items:
                        if task_item['ShortName'] == 'Potential Missing ER Contrib':
                            task_item.update({'Value':'Yes'})
                            pp.put_taskitem(task_item)
                            print(i, planid, task_item['Value'])
            
                    payload1 = {
                                "ProjectID": f'{projid}', 
                                "NoteText": note_text1,
                                "ShowOnPSL": False
                                    }
            
                    x = pp.add_note(payload1)
            
                    print(planid)
            

    schedule_logger('Finished', pyfile= __file__)

except SystemExit:
    schedule_logger('Finished', pyfile= __file__)
    raise
    
except Exception as e:
    email_error_to_automation(e, __file__)
    schedule_logger('Error', pyfile= __file__)

            