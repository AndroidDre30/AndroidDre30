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
            
            from selenium import webdriver
            from selenium.webdriver.common.by import By
            from selenium.common.exceptions import NoSuchElementException
            from selenium.webdriver.support.ui import WebDriverWait
            from selenium.webdriver.support import expected_conditions as ec
            from webdriver_manager.chrome import ChromeDriverManager
            
            from webdriver_manager.chrome import ChromeDriverManager
            
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
            
            from pypdf import PdfReader 
            from fuzzywuzzy import fuzz, process
            
            
            # In[ ]:
            
            
            
            
            
            # In[2]:
            
            
            now = datetime.now()
            previous_year = str(int(now.strftime("%Y"))-1)
            
            today = now.strftime('%Y-%m-%d_%H.%M.%S')
            
            
            # In[3]:
            
            
            def get_worktray_for_sf(name):
                
                team = pp.get_teams(filters=f"Name eq '{name}'")[0]
                teamid = team['Id']
                
                filters = f'TeamId eq {teamid} and TaskActive ne null and DateCompleted eq null'
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
                    expand = 'Plan.Client,Plan.MultipleEmployerPlan,Plan.Status,Plan.PlanType,Plan.PlanCategory,Plan.FilingStatus,Plan.PlanGroup,Plan.ComboPlan'
                    projsa = pp.get_projects(filters=filters, expand=expand)
                    projs.extend(projsa)
                    
            
                a = [[proj['Id'],
                      proj['Plan']['InternalPlanId'],
                      proj['Plan']['Name'],
                      proj['Plan']['Client']['Id'],
                      proj['Plan']['Id'],
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
                      proj['Plan']['ComboPlan']['DisplayName'],
                      f"{proj['Plan']['MonthEnd']}/{proj['Plan']['DayEnd']}",
                      proj['Plan']['PlanGroup']['DisplayName']] for proj in projs]       
            
                cols = ['projid', 'planid', 'plan_name', 'client_id', 'pp_planid', 'proj_name', 'period_start', 'period_end', 'mep_status', 
                        'added_on','effective_on','terminated_on','irs_number','plan_status', 'plan_category', 
                        'plan_type', 'form5500', 'combo_plan','plan_end', 'plan_group']
                
                ## finish adding part to look up client name. do this by collating all client IDs and doing a filtered get_clients
                ## query, expanding information that we need
                
                df2 = pd.DataFrame(a, columns=cols)
            
                dfw = df1.merge(df2, on='projid')
                
                return dfw
            
            def convert_tuple(tup, di):
                for a, b in tup:
                    di.setdefault(a, []).append(b)
                return di
            
            def convert_TF_to_digit(x):
                if x == True:
                    y = 1
                elif x == False:
                    y = 0
                return y
                    
            def convert_TF_to_one_two(x):
                if x == True:
                    y = 1
                elif x == False:
                    y = 2
                    
                return y
            
            def convert_TF_to_YN(x):
                if x == False:
                    y = "No"
                    
                elif x == True:
                    y = "Yes"
                    
                return y
            
            def split_address(address1, address2, delimiter):
                shortened_address_elements = address1.rsplit(f'{delimiter}', 1)
                address1 = shortened_address_elements[0]
                address2 = shortened_address_elements[1] + " " + address2
                return address1.strip(), address2.strip()
            
            
            # In[33]:
            
            
            # initiate logging stuff
            
            pp_logging_columns = ['planid',
                'period_start',
                'period_end',
                'mep_status',
                'added_on',
                'effective_on',
                'terminated_on',
                'irs_number',
                'taskid',
                'projid',
                'task_name',
                'plan_name',
                'client_id',
                'pp_planid',
                'proj_name',
                'plan_status',
                'plan_category',
                'plan_type',
                'form5500',
                'combo_plan',
                'plan_end',
                'plan_group',
                'error',
                'client_name',
                'boy_participants',
                'eoy_participants',
                'eoy_participants_w_acct',
                'boy_active_participants',
                'eoy_active_participants',
                'term_unvested_participants',
                'eoy_assets',
                'employer_contrib',
                'part_contrib',
                'other_income,',
                'benefits_paid',
                'deemed_or_corrective_dist',
                'salaries_fees_commissions',
                'loan_amount',
                'first_year_return',
                'final_return',
                'amended_filing',
                'short_plan_year',
                'plan_effective_date',
                'address_available',
                'address',
                'city',
                'state',
                'zipcode',
                'ein_available',
                'ein',
                'phone_available',
                'phone_number',
                'business_code_available',
                'business_code',
                'late_contrib_available',
                'late_contrib',
                'late_contrib_amount',
                'char_2r',
                'fidelity_bond_available',
                'has_fidelity_bond',
                'fidelity_bond_amount',
                'blackout_period',
                'char_2a',
                'char_2c',
                'char_2e',
                'char_2f',
                'char_2g',
                'char_2h',
                'char_2j',
                'char_2k',
                'char_2m',
                'char_2l',
                'char_2s',
                'char_2t',
                'char_3b',
                'char_3d',
                'char_3h',
                'cc_string',
                'relius_doc_available',
                'asc_extract_available',
                'algodocs_available',
                'asc_extract_read_success',
                'missing_commission_info',
                'missing_irs_num',
                'missing_naic_code',
                'collectivelybargained',
                'boy_participants_w_acct',
                'plansatisfytests',
                'plan401kdesignbased',
                'plan401kprioryear',
                'plan401kcurrentyear',
                'plan401kNA',
                'opinion_letter_date',
                'opinion_letter_serial',
                'signer_name',
                'signer_email',
                'signer_cc',
                'regex_rule_violation',]
            
            
            # In[5]:
            
            
            # Get the worktray 
            
            df = get_worktray_for_sf('5500 Automation')
            
            # pull worktray, which includes EZ forms
            df_auto = df[df['task_name'] == 'Automation Work']
            
            len(df_auto)
            
            
            # In[6]:
            
            
            df_auto
            
            
            # In[7]:
            
            
            # ## Revert plans to automation step
            
            # df = get_worktray_for_sf('5500 Preparation')
            # df_auto = df[df['task_name'] == 'Specialist Review of Form 5500']
            
            # df_auto = df_auto[df_auto['period_end'] != '12/31/2022 12:00:00 AM']
            # for i in df_auto.index[:]:
            #     projid = df_auto.at[i,'projid']
            #     project = pp.get_project_by_projectid(projid, expand="TaskGroups.Tasks")
            #     idstoclose = []
            #     for taskgroup in project['TaskGroups']:
            #         for task in taskgroup['Tasks']:
            #             if task['TaskName'] == 'Specialist Correction of ASC Data' or task['TaskName'] == 'Automation Work':
            #                 idstoclose.append(task['Id'])
            #     idstoclose.reverse()
            #     for taskid in idstoclose:
            #         pp.uncomplete_task(taskid)
            
            
            # In[ ]:
            
            
            
            
            
            # In[ ]:
            
            
            
            
            
            # In[8]:
            
            
            # Now we need to pull the contents of the ASC script output folder. First we copy any PDFs from the PC output folders
            # to the 'Y:\ASC\Exported Reports\5500 Automation' directory- need to see if this conflicts with any other scripting.
            # We then copy everything in that 5500 Automation directory (except the target list) to both a dated folder in the 
            # "Dated Output" subdirectory, as well as to the "All Output"  folder. 
            
            # Once that is complete, we can check the "All Output" folder for plans that have had 5500-SF stuff output in the past-
            # any plans on the dataframe which have a _5500SFExport_ txt file in that directory are removed from the list
            # of new plans to be pulled into the ASC script target list (since we already have good data for them, apparently)
            now = datetime.now()
            today = now.strftime('%Y-%m-%d_%H.%M.%S')
            # copy any exported PDFs 
            files_copied = False
            
            
            
            for i in range(1,22):
                os.chdir(f"Y:/ASC/Exported Reports/PC{i}")
                pc_contents = os.listdir()
                move_files = [file for file in pc_contents if "_ASC RMD List_" in file or "_InvestSumm_" in file or "_VSTWRKSH_" in file or "_FormSSA_" in file or "_HCEKey55_" in file or "_TopHeavy55_" in file or "_5500SFExport_" in file]
                if len(move_files) > 0:
                    for file in move_files:
            
                        shutil.move(file,f'Y:/ASC/Exported Reports/5500 Automation/{file}')
                        files_copied = True
            
            files_copied = True
            if files_copied is True:
                    
                # Switch active directory to the 5500 Automation folder
                os.chdir('Y:/ASC/Exported Reports/5500 Automation')
                sf_folder = os.listdir()
            
                # files to ignore when moving
                ignore_file_list = ['5500-SF_Target_PlanList.txt','DGEM Import Logs','DGEM Import Files',
                                    'Find Results','All Output','All Output (new)','Dated Output','DGEM Import Files', 
                                    'Put Downloaded PDF Files Here', 'Testing', 'DGEM Validation Files','Pickle', 'All Output (2022 work)',
                                   '1_5500-SF_Target_PlanList.txt','2_5500-SF_Target_PlanList.txt','3_5500-SF_Target_PlanList.txt',
                                   '4_5500-SF_Target_PlanList.txt', 'Signer Import Files']
                
                target_files = [file for file in sf_folder if file not in ignore_file_list]
                target_files = [file for file in target_files if file.endswith('.bat') == False]
                
                # create directory for current date for copy of files
                newdir_name = f'Dated Output/{today}_ASCVal Output'
                os.mkdir(newdir_name)
            
                # THIS PART NEEDS TO BE UPDATED TO DYNAMICALLY CHANGE YEAR FOLDER
                # Also going to be an issue with off-calendar plans, since I don't know if those are consistent
                for file in target_files:
                    #get year from filename
                    file_date = file.split("_")[1]
                    if len(file_date) == 4:
                        file_year = file_date
                    else:
                        file_year = file_date[4:]
                    
            
                    # fix where these two are copied to the next year's folder
                    if "TopHeavy" in file or "HCEKey" in file:
                        year_after = str(int(file_year) + 1)
                        nova.copy_file(file,f'{year_after}/5500')
                    else:
                        nova.copy_file(file,f'{file_year}/5500')
            
            
                    shutil.copy(file,f'{newdir_name}/{file}')
                    shutil.move(file,f'All Output/{file}')
            
                    
            
            
            # In[9]:
            
            
            # get list of all plans that have output in this folder, to exclude them from the ASC target list
            # os.chdir('Y:/ASC/Exported Reports/5500 Automation/All Output')
            os.chdir('Y:/ASC/Exported Reports/5500 Automation/All Output')
            
            all_output_folder = os.listdir()
            plans_with_asc_output = list(set([file for file in all_output_folder if '5500SFExport.txt' in file]))
            print(len(plans_with_asc_output))
            
            
            # In[10]:
            
            
            # create two dataframes from the 5500 Automation, 'Automation Work' task
            # first one will continue below, to pull all the needed sf fields and generate that output
            # second one will be used here to generate a target list for the ASC script to get the SF output
            
            # lists for rows in dataframe to be split based on file match or no
            indices = []
            not_indices = []
            
            for i in df_auto.index:
                planid = df_auto.at[i,'planid']
                period_end = df_auto.at[i,'period_end']
                period_end_dt = datetime.strptime(period_end,'%m/%d/%Y %H:%M:%S %p')
                
                matching_file_found = False
                
                for file in plans_with_asc_output:
                    file_planid = file.split("_")[0]
                    file_periodend = file.split("_")[1]
                    file_periodend_dt = datetime.strptime(file_periodend,'%m%d%Y')
                    file_periodend_dt = file_periodend_dt + timedelta(hours=12) #match file datetime to pensionpro dt
                    
                    if file_planid == planid:
                    
                        if file_periodend_dt == period_end_dt:
                            matching_file_found = True
                            
                if matching_file_found is True:
                    indices.append(i)
            
                else:
                    not_indices.append(i)
            
            df_sfpull = df_auto.loc[indices]
            df_asc_target = df_auto.loc[not_indices]
            df_sfpull = df_sfpull[df_sfpull['proj_name'] == 'DC Annual Governmental Forms - Small Filer (Automated)']
            df_sfpull.reset_index(drop=True, inplace=True)
            
            
            # In[11]:
            
            
            len(df_sfpull), len(df_asc_target)
            
            
            # In[13]:
            
            
            #df_sfpull.loc[df_sfpull['planid']=='90438']
            
            
            # In[12]:
            
            
            # this next part generates a target list for ASC (same code as the testing target script)
            
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
                    
            fname = "Y:/ASC/Exported Reports/SSA_Target_PlanList.txt"
            print(len(target_list))
            np.savetxt(fname, target_list, fmt='%s')
            
            array_split = np.array_split(target_list, 4)
            
            b=1
            for array in array_split: 
                fname = f"Y:/ASC/Exported Reports/5500 Automation/{b}_5500-SF_Target_PlanList.txt"
                np.savetxt(fname, array, fmt='%s')
                b+=1
            
            
            # In[13]:
            
            
            now = datetime.now()
            
            today = now.strftime('%Y-%m-%d_%H.%M.%S')
            
            pickle_dir = 'Y:/ASC/Exported Reports/5500 Automation/Pickle'
            
            pickle_file = f'{pickle_dir}/{today}_ASCExport.pkl'
            
            asc_targets = df_asc_target.projid.tolist()
            
            with open(pickle_file, 'wb') as f:
                pickle.dump(asc_targets,f)
                
            last_pickles = os.listdir(pickle_dir)
            last_pickles.reverse()
            
            
            # In[14]:
            
            
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
                    
            # I commented this out. I ran into a situation where I needed to run this more than once a day and it mistakenly overrode
            # a bunch of tasks. Stale processes will remain as is. -Andrew
            
            
            # In[ ]:
            
            
            
            
            
            # In[23]:
            
            
            # advance plans that haven't been able to have ASC data pulled in three days to specialists
            
            # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
            # Added by Andrew. Delete this block. do not run if you're not Andrew!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
            #!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
            #!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
            
            # note_text1 = "Needs to be completed manually."
            
            # task_names = ['Automation Work','Specialist Correction of ASC Data']
            
            # for projid in pickle_check[:]:
            #     try:
            #         project = pp.get_project_by_projectid(projid, expand="TaskGroups.Tasks,Plan")
            #     except:
            #         continue # Some projects can't be looked up at all? 'Bad Request for url' error.   ¯\_(ツ)_/¯
                
            #     plan_name = project['Plan']['Name']
            #     planid = project['Plan']['InternalPlanId']
                
            #     for taskgroup in project['TaskGroups']:
            #         for task in taskgroup['Tasks'][::-1]:
            #             if task['TaskName'] in task_names and task['DateCompleted'] != None:
            #                 #pp.override_task(task['Id'])
            #                 try:
            #                     pp.uncomplete_task(task["Id"])
            #                     print(f"Task uncompleted for {plan_name}.")
            #                 except:
            #                     print("Failed to uncomplete task")
            #                 print(task['Id'])
                
            #     meow_found_notes = pp.get_notes_by_projid(projid)
            #     for meow_note in meow_found_notes:
            #         if meow_note['NoteText'] == note_text1:    
            #             pp.delete_note(meow_note['Id'])
                    
            # #     payload1 = {
            # #                 "ProjectID": f'{projid}', 
            # #                 "NoteText": note_text1,
            # #                 "ShowOnPSL": False
            # #                     }
            
            # #     x = pp.add_note(payload1)
                
            #     print(planid, plan_name)
            
            
            # In[15]:
            
            
            #advance plans that haven't been able to have ASC data pulled in three days to specialists
            
            note_text1 = "Needs to be completed manually."
            
            task_names = ['Automation Work','Specialist Correction of ASC Data']
            
            for projid in pickle_check[:]:
                try:
                    project = pp.get_project_by_projectid(projid, expand="TaskGroups.Tasks,Plan")
                except:
                    continue # Some projects can't be looked up at all? 'Bad Request for url' error.   ¯\_(ツ)_/¯
                
                plan_name = project['Plan']['Name']
                planid = project['Plan']['InternalPlanId']
                
                for taskgroup in project['TaskGroups']:
                    for task in taskgroup['Tasks']:
                        if task['TaskName'] in task_names and task['DateCompleted'] is None:
                            pp.override_task(task['Id'])
                            print(task['Id'])
            
                payload1 = {
                            "ProjectID": f'{projid}', 
                            "NoteText": note_text1,
                            "ShowOnPSL": False
                                }
            
                x = pp.add_note(payload1)
                
                print(planid, plan_name)
            
            
            # In[16]:
            
            
            # get all client information (40 at a time to prevent the query from erroring)
            clientids = df_sfpull['client_id'].tolist()
            
            c = -(-(len(clientids)) // 40)
                
            all_clients = []
            
            for i in range(c):
                clientidsa = clientids[i*40:(i+1)*40]
                filters = ' or '.join([f'ClientId eq {clientid}' for clientid in clientidsa])
                expand = "CompanyName,EmployerDatas,Addresses.AddressType,Addresses.Address,Numbers.PhoneNumberType,Numbers.PhoneNumber"
                clients = pp.get_clients(filters=filters,expand=expand)['Values']
                all_clients.extend(clients)
            # end get client block
            
            
            # In[17]:
            
            
            # get all plan contact information, a few at a time, and filter by signer categories
            planids = df_sfpull['pp_planid'].tolist()
            
            c = -(-(len(planids)) // 50)
                
            all_plan_contacts = []
            
            for i in range(c):
                planidsa = planids[i*50:(i+1)*50]
                filters = ' or '.join([f'PlanId eq {pp_planid}' for pp_planid in planidsa])
                plan_contacts = pp.get_plan_contact_roles(expand="Contact,RoleType", filters=filters)
                all_plan_contacts.extend(plan_contacts)
            # end get plan contact block
            
            
            # In[18]:
            
            
            ## create lists of contacts that are relevant for signer information
            
            signer_contacts = [contact for contact in all_plan_contacts if contact['RoleType']['DisplayName'] == '5500 Signer']
            signer_cc_contacts = [contact for contact in all_plan_contacts if contact['RoleType']['DisplayName'] == '5500 cc']
            primary_broker_contacts = [contact for contact in all_plan_contacts if contact['RoleType']['DisplayName'] == 'Primary Broker']
            afs_signer_contacts = [contact for contact in all_plan_contacts if contact['RoleType']['DisplayName'] == '3(16) Administrator']
            
            
            # In[19]:
            
            
            ## read in Relius data extract
            relius_extract_path = r'Y:\Automation\Projects\Active\5500 SF Automation\2023\Relius Document Data 20240513.xlsx'
            
            df_relius = pd.read_excel(relius_extract_path)
            
            df_relius.dropna(subset=['EmployerEIN'], inplace=True)
            
            
            # In[20]:
            
            
            
            ## read in algodocs commissions information
            jh_algodocs_path = r'Y:\Automation\Projects\Active\5500 SF Automation\2023\Fees & Commissions\Master Files\JH\JH Commissions Info Master.xlsx'
            voya_algodocs_path = r'Y:\Automation\Projects\Active\5500 SF Automation\2023\Fees & Commissions\Master Files\Voya\Voya Commissions Info Master.xlsx'
            ta_algodocs_path = r'Y:\TED Files\2023\Transamerica\Schedule A\Transamerica Commissions Info Master.xlsx'
            empower_algodocs_path = r'Y:\TED Files\2023\Empower\Empower Commissions Info Master.xlsx'
            
            # add other RK algodocs paths as they become available
            
            # create single dataframe for lookup below
            df_algodocs = pd.read_excel(jh_algodocs_path).append(pd.read_excel(voya_algodocs_path)).append(pd.read_excel(ta_algodocs_path)).append(pd.read_excel(empower_algodocs_path))
            df_algodocs.reset_index(inplace=True)
            
            df_algodocs['plan_id'] = df_algodocs['plan_id'].astype(str)
            
            algodocs_rks = ['Ameritas',
            'AXA Equitable',
            'Empower - Non-Automated Distributions',
            'Empower Retirement',
            'John Hancock',
            'Lincoln',
            'Lincoln - Alliance',
            'Lincoln - Director',
            'Nationwide',
            'Nationwide Pension Prof',
            'Nationwide Regular',
            'Ohio National',
            'OneAmerica (formerly AUL)',
            'OneAmerica Alliance Plus',
            'Principal',
            'Securian',
            'Securian Financial',
            'The Standard',
            'Transamerica',
            'Voya',
            'Voya - ACES',
            'Voya - EASE']
            
            exclude_rks = ['Mass Mutual (Aviator)',
                            'Mass Mutual (Reflex)',
                            'Mass Mutual']
            
            
            # In[21]:
            
            
            #df_pars[df_pars['planid'] == '4932']
            
            
            # In[22]:
            
            
            # read in JH loan and fee information
            jh_pars_path = r'Y:/TED Files/2023/John Hancock'
            os.chdir(jh_pars_path)
            jh_pars_dir = os.listdir()
            
            jh_pars_dir = [file for file in jh_pars_dir if "AlgoDocs-" in file and "~$" not in file]
            
            checknum = 0
            for file in jh_pars_dir:
                if checknum == 0:
                    df_pars = pd.read_excel(file)
                    df_pars = df_pars[df_pars['Total JH Contract Admin Fees'].astype(str) != 'nan']
                    checknum+=1
                    
                else:
                    df_pars0 = pd.read_excel(file)
                    df_pars0 = df_pars0[df_pars0['Total JH Contract Admin Fees'].astype(str) != 'nan']
                    df_pars = df_pars.append(df_pars0)
                    df_pars.reset_index(inplace=True,drop=True)
            
            # split out column with Nova plan IDs
            df_pars['planid'] = df_pars['File name'].apply(lambda x: pd.Series(str(x).split("_")))[0]
            
            df_pars = df_pars.fillna(0)
            
            # create new columns for ease of pulling info below
            df_pars['fee_total'] = df_pars['Total JH Contract Admin Fees'] + df_pars['Total TPA Fees'] + df_pars['Total Redemption Fees'] + df_pars['Total Inv Adv Fees'] + df_pars['JH GIFL Fees']
            df_pars['loans'] = df_pars['Loan Value EOY']
            df_pars['corrections'] = df_pars['Deemed loan distributions'] + df_pars['Corrective Distributions']
            
            
            # In[ ]:
            
            
            
            
            
            # In[23]:
            
            
            # get 2022 EFAST data extract to get EOY balance for escalated fidelity bond amounts
            efast_extract_path = r'Y:\5500\2022\EFAST 5500 Data 2022.xlsx'
            df_efast_sf = pd.read_excel(efast_extract_path, sheet_name="5500 SF", dtype={"SF_BUSINESS_CODE": str})
            df_efast_5500 = pd.read_excel(efast_extract_path, sheet_name="5500")
            df_efast_name = pd.read_excel(efast_extract_path, sheet_name="5500 Name")
            
            
            # In[24]:
            
            
            #df_efast_sf["SF_BUSINESS_CODE"]
            
            
            # In[25]:
            
            
            # get Traveler's Insurance List of purchased bonds to use in fidelity bond field
            fidelity_bond_path = r'T:\Government Filing Forms\Form 5500\Fidelity Bond Information\Travelers\Purchased List.xlsx'
            df_fidelity_bond = pd.read_excel(fidelity_bond_path,sheet_name="PURCHASED",converters={"Client ID":str})
            
            
            # In[ ]:
            
            
            
            
            
            # In[26]:
            
            
            # DGEM upload template
            template_source = r'Y:\5500\2023\Automation\SF Production\DGEM txt template.xlsx'
            df_template = pd.read_excel(template_source)
            template_columns = df_template.columns.tolist()
            
            ## import DGEM template details in order to test output values for form validity and length
            import re
            
            instructions_path = r'Y:\5500\2023\Automation\SF Production\ASC_DGEM Form 5500SF Template_2023.xlsx'
            
            df_dgem_instructions = pd.read_excel(instructions_path)
            
            
            # In[ ]:
            
            
            
            
            
            # In[27]:
            
            
            
            def Add_Note_To_MEP_Project(planid, project_id):
                """
                
                Added by Andrew on 6/3/24
                
                """
                project_id = int(project_id)
                
                # Check if the note already exists. Return Falsy value if found. 
                project_notes = pp.get_project_notes(project_id)
                for note in project_notes:
                    if note['NoteText'].lower() == 'mep attachment needed':
                        print("'MEP attachment needed' note already exists!")
                        return {}
                
                payload = {
                "PlanId": pp.get_sysplanid(planid),
                'NoteText': 'MEP attachment needed', 
                'NoteCategoryId': 3545, # Category: Automation - Production
                'ProjectId': project_id,
                #'IsImportant' : True
                }
                
                add_note_results = pp.add_note(payload)
                
                return add_note_results
            
            
            # In[28]:
            
            
            def CLEAN_PLAN_NAME(input_str):
                """
                Requires re
            
                This erases generic terms such as 401k and 'the' from a given plan name. This is used to compare against a master plan list
                that also had its name cleaned up in a similar manner. 
            
                The main purpose of this is to use the fuzzywuzzy library and compare the two data points. Having more strings than necessary
                will confuse the fuzzywuzzy name matching algorithm. 
                
                Example usage:
            
                plan_name = CLEAN_PLAN_NAME(plan_name)
                fuzzywuzzy.process.extract(plan_name, ALL_PLANS["Name"].astype(str).str.lower().to_list(), limit=3, scorer=fuzz.WRatio)
            
                """
            
                input_str = re.sub(r'401\s?\(k\)[^\n]*?plan|401\s?k[^\n]*?plan', '', input_str, flags=re.IGNORECASE)
                input_str = re.sub(r'401\s?\(k\)|401\s?k', '', input_str, flags=re.IGNORECASE)
                input_str = re.sub(r'401\s?\(a\)[^\n]*?plan|401\s?a[^\n]*?plan', '', input_str, flags=re.IGNORECASE)
                input_str = re.sub(r'401\s?\(a\)|401\s?a', '', input_str, flags=re.IGNORECASE)
                input_str = re.sub(r'(?i)^the\s+', '', input_str, flags=re.IGNORECASE) # Erase 'the' in front of string.
                input_str = input_str.strip()
                
                return input_str 
            
            def Measure_Name_Similarity(name_source_1, name_source_2):
                """
                
                Requires fuzzywuzzy library.
                
                Returns the confidence level when matching between 2 names. 100% is a perfect match.
                Anything between 100 and 90 probably means there were minor spelling variations but otherwise
                should be considered the same name. Anything less than 90 is probably a significant name change. 
                
                """
                
                # If invalid data is passed down, return 100 to bypass filters checking for < 90. 
                if name_source_1 == None or name_source_2 == None:
                    return 100
                
                name_source_1 = name_source_1.upper()
                name_source_2 = name_source_2.upper()
                
                # Clean up generic terms such as '401(k)'. Keeping generic words in place may erroneously raise the confidence level during comparison.  
                #name_source_1 = CLEAN_PLAN_NAME(name_source_1)
                #name_source_2 = CLEAN_PLAN_NAME(name_source_2)
                
                print(name_source_1)
                print(name_source_2)
                
                match_results = process.extract(name_source_1, [name_source_2], limit=1, scorer=fuzz.ratio)[0]
                confidence_level = match_results[1]
                
                return confidence_level
            
            
            # In[32]:
            
            
            dgem_dataframe_values = []
            pp_update_data = []
            
            os.chdir('Y:/ASC/Exported Reports/5500 Automation/All Output')
            
            
            # In[34]:
            
            
            # mega loop 1
            
            b=0
            ends_on = None
            
            for i in df_sfpull.index[b:ends_on]:
                os.chdir('Y:/ASC/Exported Reports/5500 Automation/All Output')
            #     try:
            
                error = False
             
                planid = df_sfpull.at[i,'planid']
                period_start = df_sfpull.at[i,'period_start']
                period_end = df_sfpull.at[i,'period_end']
                mep_status = df_sfpull.at[i,'mep_status']
                added_on = df_sfpull.at[i,'added_on']
                effective_on = df_sfpull.at[i,'effective_on']
                terminated_on = df_sfpull.at[i,'terminated_on']
                irs_number = df_sfpull.at[i,'irs_number']
                taskid = df_sfpull.at[i,'taskid']
                projid = df_sfpull.at[i,'projid']
                task_name = df_sfpull.at[i,'task_name']
                plan_name = df_sfpull.at[i,'plan_name']
                client_id = df_sfpull.at[i, 'client_id']
                pp_planid = df_sfpull.at[i, 'pp_planid']
                proj_name = df_sfpull.at[i,'proj_name']
                plan_status = df_sfpull.at[i,'plan_status']
                plan_category = df_sfpull.at[i,'plan_category']
                plan_type = df_sfpull.at[i,'plan_type']
                form5500 = df_sfpull.at[i,'form5500']
                combo_plan = df_sfpull.at[i,'combo_plan']
                plan_end = df_sfpull.at[i,'plan_end']
                plan_group = df_sfpull.at[i,'plan_group']
                
                print(b, planid, plan_name)
                
                regex_rule_violation = "" # New category for pp_ouput to record regex violations.
                
                period_year = period_end.split("/")[2].split(" ")[0]
                year_after_period_year = str(int(period_year) + 1)
                
                # create dt objects for time comparisons
                project_period_end = datetime.strptime(period_end,'%m/%d/%Y %H:%M:%S %p')
                project_period_start = datetime.strptime(period_start,'%m/%d/%Y %H:%M:%S %p')
                date_plan_added = datetime.strptime(added_on,'%m/%d/%Y %H:%M:%S %p')
                date_effective_on = datetime.strptime(effective_on,'%m/%d/%Y %H:%M:%S %p')
                
                matching_file_found = False
                
                # create dataframe of relius information
                df_doc = df_relius[df_relius['TPA Plan ID'] == planid]
                df_doc.reset_index(inplace=True)
                if len(df_doc) == 0:
                    relius_doc_available = False
                    error = True
            
                else:
                    relius_doc_available = True
                
                # import ASC extract information
                # we should still be pointed at the "All Output" dir
                # for 2023 forms, we added logic to ensure we're pulling the right PYE
                asc_extract_available = False
                asc_extract = [file for file in all_output_folder if "_5500SFExport" in file and file.startswith(f"{planid}_") and file.endswith(".txt")]
                if len(asc_extract) > 0:
                    for file in asc_extract:
                        file_periodend = file.split("_")[1]
                        file_periodend_dt = datetime.strptime(file_periodend,'%m%d%Y')
                        file_periodend_dt = file_periodend_dt + timedelta(hours=12)
                        if file_periodend_dt == project_period_end:
                            try:
                                df_asc = pd.read_table(file,header=None)
                                asc_extract_available = True
                                break
                            except:
                                asc_extract_available = False
                                break 
                                
                    if asc_extract_available == False:
                        asc_extract_available = False
                        error = True                
                else:
                    asc_extract_available = False
                    error = True
            
                # import status grid to check for missing dates
                blank_date_found = False
                status_grids = [file for file in all_output_folder if file.endswith("_Status Grid.txt") and file.startswith(f"{planid}_")]
                if len(status_grids) > 0:
                    for file in status_grids:
                        file_periodend = file.split("_")[1]
                        file_periodend_dt = datetime.strptime(file_periodend,'%m%d%Y')
                        file_periodend_dt = file_periodend_dt + timedelta(hours=12)
                        if file_periodend_dt == project_period_end:
                            df_status_grid = pd.read_table(file)
            
                            df_status_grid.replace({pd.NaT: 0}, inplace=True)
            
                            for i in df_status_grid.index:
                                birth_date = df_status_grid.at[i,'Birth Date']
                                hire_date = df_status_grid.at[i,'Hire Date']
                                if birth_date == 0 or hire_date == 0:
                                    blank_date_found = True
                  
                # import JH PARS
                df_algodocs_pars = df_pars[df_pars['planid'] == planid]
                df_algodocs_pars.reset_index(inplace=True)
            
                if len(df_algodocs_pars) == 0:
                    jh_pars_available = False
            
                else:
                    jh_pars_available = True
            
                # import EFAST data for fido bonds
                df_efast_plan = df_efast_sf[df_efast_sf['TPA Plan ID'] == planid]
                df_efast_plan.reset_index(inplace=True)
                efast_available = False
            
                # check if the plan is available on sf list
                if len(df_efast_plan) != 0:
                    efast_available = "sf_available"
            
                # if no, check out if it's available in the long filer extract
                elif len(df_efast_plan) == 0:
                    df_efast_plan = df_efast_5500[df_efast_5500['TPA Plan ID'] == planid]
                    df_efast_plan.reset_index(inplace=True)
            
                    if len(df_efast_plan) != 0:
                        efast_available = "5500_available"
            
                    else:
                        efast_available = False
            
                else:
                    efast_available = False
            
                # determine if plan is a takeover or not
                takeover_plan = False
                if date_effective_on >= project_period_start and date_effective_on <= project_period_end:
                    if date_plan_added < project_period_start:
                        takeover_plan = True
            
                # reformat pp dates to comply with DGEM standards
                period_start_dgem = datetime.strftime(project_period_start,'%Y-%m-%d')
                period_end_dgem = datetime.strftime(project_period_end,'%Y-%m-%d')
                
                # Tries to grab the liquidation project field from the termination project to check if
                # the plan is actually terminating or just leaving Nova
                project_fields = pp.get_project_fields_by_planid(planid, filters="FieldName eq 'Date Final Assets Liquidated'")
                if len(project_fields) == 0:
                    terminated_on = None
                else:
                    terminated_on = project_fields[0]['FieldValue']
                    if terminated_on:
                        date_plan_terminated = datetime.strptime(terminated_on,'%m/%d/%Y')        
            
                # isolate client information
                client_info = [client for client in all_clients if client['Id'] == client_id][0]
            
                # get client name
                client_name = client_info['CompanyName']['DisplayName']
                
                asc_extract_read_success = None
                
                # ASC information
                if asc_extract_available is True:
            
                    boy_participants = int(df_asc.at[0,12])
            
                    eoy_participants_w_acct = int(df_asc.at[0,18])
                    boy_active_participants = int(df_asc.at[0,13])
                    eoy_active_participants = int(df_asc.at[0,14])
                    term_unvested_participants = int(df_asc.at[0,19])
                    boy_participants_w_acct = int(df_asc.at[0,20])
            
                    eoy_participants = int(eoy_active_participants + df_asc.at[0,15] + df_asc.at[0,16] + df_asc.at[0,17])
            
                    eoy_assets = int(df_asc.at[2,4])
                    employer_contrib = int(df_asc.at[2,5])
                    part_contrib = int(df_asc.at[2,6])
                    other_contrib = int(df_asc.at[2,7])
                    other_income = int(df_asc.at[2,8])
                    benefits_paid = int(df_asc.at[2,9])
                    deemed_or_corrective_dist = int(df_asc.at[2,10])
                    salaries_fees_commissions = int(df_asc.at[2,11])
            
                    loan_amount = int(df_asc.at[2,12])
                    asc_extract_read_success = True
            
                else:
                    boy_participants = None
                    eoy_participants = None
                    eoy_participants_w_acct = None
                    boy_active_participants = None
                    eoy_active_participants = None
                    term_unvested_participants = None
                    boy_participants_w_acct = None
            
                    eoy_assets = None
                    employer_contrib = None
                    part_contrib = None
                    other_income = None
                    benefits_paid = None
                    deemed_or_corrective_dist = None
                    salaries_fees_commissions = None    
            
                    loan_amount = None
            
                    error = True
                    asc_extract_read_success = False
                
                ## Determine whether the plan is obviously union or no:
                collectivelybargained = False
            
                if "Union" in plan_name or "Collectively Bargained" in plan_name:
                    if "Non-Union" not in plan_name and "Credit Union" not in plan_name:
                        collectivelybargained = True
            
                ## get Opinion Letter Information
                opinion_letter_available = False
                
                preapproved_items = None
                opinion_letter_date = None
                opinion_letter_serial = None
            
                plan_project_fields = pp.get_projects_by_planid(planid, filters="Name eq 'PreApproved Serial Number Update'",expand="TaskGroups.Tasks")
                
                if len(plan_project_fields) > 0:
            
                    for project in plan_project_fields:
                        for taskgroup in project['TaskGroups']:
                            for task in taskgroup['Tasks']:
                                if task['TaskName'] == 'Serial Number':
                                    task_items = pp.get_task_items_by_taskid(task['Id'])
            
                    for task_item in task_items:
                        if task_item['ShortName'] == 'Preapproved Document Serial Number':
                            preapproved_items = task_item['Value']
                #         if combo_plan != 'N/A':
                #             preapproved_items = task_item['Value']
                else:
                    error = True
                    preapproved_items = None
                    
                if combo_plan != 'N/A' and combo_plan != 'No':
            
            
                    for plan in client_info['Plans']:
                        additional_eoy_value = None
                        if plan['InternalPlanId'] == planid:
                            print(plan['InternalPlanId'])
                            continue
            
                        else:
                            additional_eoy_value = get_valuation_file(plan['InternalPlanId'], period_year)
                            print("combo plan!", plan['InternalPlanId'], additional_eoy_value, eoy_assets)
            
                            if str(additional_eoy_value) != "None":
                                eoy_assets += additional_eoy_value
            
                            else:
                                missing_valuation_report = True
            
                            #revert directory back to ASC files
                            os.chdir(All_Output_Directory)
                        
                if preapproved_items is not None:
                    opinion_letter_date = preapproved_items.split("; ")[1]
                    opinion_letter_serial = preapproved_items.split("; ")[0]
                    opinion_letter_available = True
                    
                if opinion_letter_available == False:
                    error = True
                    
                ## Find plan G drive directory to pull fidelity bond and late contrib info from there
            
                period_year = period_end.split("/")[2].split(" ")[0]
                plan_dir = nova.get_fold(planid)
                plan_directory_attempt = f"G:/{plan_dir}/{period_year}/Testing"
                empower_directory_attempt = f"G:/{plan_dir}/{period_year}/Empower"
                
                df_questionnaire = pd.DataFrame()
                df_questionnaire_plan = pd.DataFrame()
                df_empower_par = pd.DataFrame()
                
                # variable to track if we find relevant
                other_plan_410b = False
                
                # get Empower directory
                if os.path.exists(empower_directory_attempt):
                    os.chdir(empower_directory_attempt)
                    g_drive_empower_dir = os.listdir()
                    
                    for file in g_drive_empower_dir:
                        os.chdir(empower_directory_attempt)
            
                        # get empower par file
                        if file.startswith(f"{planid}_AdjustedPAR"):
                            df_empower_par = pd.read_excel(file)
              
                    if len(df_empower_par) == 0:
                        for file in g_drive_empower_dir:
                            os.chdir(empower_directory_attempt)
            
                            # get non-adjusted empower par file if the adjusted one doesn't exist or was renamed
                            if file.endswith(f"_acr.pas2.xlsx"):
                                df_empower_par = pd.read_excel(file)  
                                
                if os.path.exists(plan_directory_attempt):
                    os.chdir(plan_directory_attempt)
                    g_drive_testing_dir = os.listdir()
                    
                    for file in g_drive_testing_dir:
                        os.chdir(plan_directory_attempt)
            
                        # get questionnaire file
                        if file.startswith("ProQuestionnaire"):
                            df_questionnaire = pd.read_excel(file,sheet_name="5500",skiprows=1)
                            df_questionnaire_plan = pd.read_excel(file,sheet_name="Plan",skiprows=1)
                            
                        # get 410b PDF files, read them, and see if the 410b was aggregated across multiple plans
                        
                        if "Temp" in file and "DNU" not in file:
                            if os.path.isdir(file):
                                os.chdir(file)
                                temp_list = os.listdir()
                                for temp_file in temp_list:
                                    if temp_file.startswith(f"{planid}_410b") and temp_file.endswith('.pdf'):
                                        reader = PdfReader(temp_file) 
                                        for page in reader.pages: 
                                            text = page.extract_text()
                                            if 'Includes data aggregated from other plans sponsored by same Employer' in text:
                                                other_plan_410b = True
                                                break
                                                
                # one of the questionnaire types doesn't ask these apparently
            
                late_contributions_questionnaire = None
                fidelity_amount_questionnaire = None
                fidelity_auto_questionnaire = None
                signer_name_questionnaire = None
                signer_email_questionnaire = None
                    
                if len(df_questionnaire) > 4:
                    late_contributions_questionnaire = df_questionnaire.at[0,"Answer"]
                    fidelity_amount_questionnaire = df_questionnaire.at[1,"Answer"]
                    fidelity_auto_questionnaire = df_questionnaire.at[2,"Answer"]
            
                if len(df_questionnaire) == 5:
                    signer_name_questionnaire = df_questionnaire.at[3,"Answer"]
                    signer_email_questionnaire = df_questionnaire.at[4,"Answer"]
                
                if len(df_questionnaire_plan) == 11:
                    multi_plan_questionnaire = df_questionnaire_plan.at[8,"Answer"]
                    
                if len(df_questionnaire_plan) == 17:
                    multi_plan_questionnaire = df_questionnaire_plan.at[12,"Answer"]        
            
                elif len(df_questionnaire_plan) == 25:
                    multi_plan_questionnaire = df_questionnaire_plan.at[18,"Answer"]
                    
                elif len(df_questionnaire_plan) > 25:
                    multi_plan_questionnaire = df_questionnaire_plan.at[20,"Answer"]
                else:
                    multi_plan_questionnaire = None
                
                ## Parse signer information for later import
                
                # init variables
                signer_name = None
                signer_email = None
                signer_cc = None
                
                # strip out any garbage signer information from questionnaire
                # Added a check to see if signer_name_questionnaire is float after finding nan value. - Andrew 6/14/24
                if signer_email_questionnaire != None and type(signer_name_questionnaire) != float and not isinstance(signer_name_questionnaire, np.floating):
                    
                    
                    if "SAME AS" in str(signer_name_questionnaire.upper()):
                        signer_name_questionnaire = None
                        signer_email_questionnaire = None
            
                    if "@" not in str(signer_email_questionnaire):
                        signer_name_questionnaire = None
                        signer_email_questionnaire = None
            
                    if str(signer_name_questionnaire).startswith("AFS ") or str(signer_name_questionnaire).startswith("AMP "):
                        signer_name_questionnaire = None
                        signer_email_questionnaire = None    
            
                    if len(str(signer_name_questionnaire)) > 50:
                        signer_name_questionnaire = None
                        signer_email_questionnaire = None          
            
                    if str(signer_name_questionnaire) != 'None' and str(signer_email_questionnaire) != 'None':
                        signer_name = signer_name_questionnaire.upper().strip()
                        signer_email = signer_email_questionnaire.strip()
                    
                # get contacts for individual plan from contact object list grabbed before loop
                plan_signer_contact = [contact for contact in signer_contacts if contact['PlanId'] == pp_planid]
                plan_signer_cc_contacts = [contact for contact in signer_cc_contacts if contact['PlanId'] == pp_planid]
                plan_primary_broker_contacts = [contact for contact in primary_broker_contacts if contact['PlanId'] == pp_planid]
                plan_afs_signer_contact = [contact for contact in afs_signer_contacts if contact['PlanId'] == pp_planid]
            
                ## logic for determining which contact information to use for signer and CCs
                
                # first, if they are an AFS Partner level plan, let's ignore questionnaire answer and use AFS contact
                if "Partner" in plan_group:
                    if len(plan_afs_signer_contact) > 0:
                        plan_afs_signer_contact = plan_afs_signer_contact[0]
                    
                        first_name_signer = plan_afs_signer_contact['Contact']['FirstName'].strip()
                        last_name_signer = plan_afs_signer_contact['Contact']['LastName'].strip()
                        signer_name = first_name_signer + " " + last_name_signer
                        signer_email = plan_afs_signer_contact['Contact']['Email'].strip()
                
                # otherwise if the questionnaire answer is blank (or was blanked) we try to pull info from Pro
                else:
                    if signer_name == None and len(plan_signer_contact) > 0:
                        plan_signer_contact = plan_signer_contact[0]
                        
                        if plan_signer_contact['Contact']['Email'] != None:
                        
                            first_name_signer = plan_signer_contact['Contact']['FirstName'].strip()
                            last_name_signer = plan_signer_contact['Contact']['LastName'].strip()
            
                            signer_name = first_name_signer + " " + last_name_signer
                            signer_email = plan_signer_contact['Contact']['Email'].strip()
                        
                # finally, we assemble the cc string
                
                signer_cc_string = ""
                
                # first we add emails from explicit 5500 cc contacts
                if len(plan_signer_cc_contacts) > 0:
                    for cc_contact in plan_signer_cc_contacts:
                        if cc_contact['Contact']['Email'] != None:
                            cc_contact_email = cc_contact['Contact']['Email'].strip() + ","
                            signer_cc_string+=cc_contact_email
                        
                # then we add primary broker emails
                if len(plan_primary_broker_contacts) > 0:
                    for broker_contact in plan_primary_broker_contacts:
                        if broker_contact['Contact']['Email'] != None:
                            broker_contact_email = broker_contact['Contact']['Email'].strip() + ","
                            signer_cc_string+=broker_contact_email
                        
                # finally, we add ourselves
                signer_cc_string+="automation@nova401k.com"
                    
                signer_cc = signer_cc_string
             
                # Pull in JH information for fees
                if salaries_fees_commissions == 0 and jh_pars_available == True:
            
                    # pull fee total from algodocs pars dataframe
                    jh_par_fees = df_algodocs_pars.at[0,'fee_total']
            
                    # take absolute value of par fees, since they're expressed as negatives
                    salaries_fees_commissions = int(round(abs(jh_par_fees),0))
            
                    # subtract fees from "other income" (investment gains), since JH rolls the fees in there
                    other_income = other_income - int(round(jh_par_fees,0))
            
                # Pull in JH information for corrective distributions
                if deemed_or_corrective_dist == 0 and jh_pars_available == True:
                    
                    # pull fee total from algodocs pars dataframe
                    jh_par_corrections = df_algodocs_pars.at[0,'corrections']
            
                    # take absolute value, since they're expressed as negatives
                    deemed_or_corrective_dist = int(round(abs(jh_par_corrections),0))
            
                    # subtract corrective dist from "other income" (investment gains), since JH rolls the fees in there
                    other_income = other_income - int(round(jh_par_corrections,0))
            
                # Pull in JH information for loans
                if loan_amount == 0 and jh_pars_available == True:
            
                    # pull loan total from algodocs pars dataframe
                    jh_loans = df_algodocs_pars.at[0,'loans']
                    loan_amount = int(round(jh_loans,0))
                    
                # Pull in Empower information for loans, if we were able to detect the PAR in the G drive
                if len(df_empower_par) > 0:
                    empower_loan_sum = 0
            
                    for column in df_empower_par:
                        if "End Prin" in column or "End Int" in column:
                            empower_loan_sum += df_empower_par[f'{column}'].sum()
                            
                    if empower_loan_sum > 0:
                        loan_amount = int(round(empower_loan_sum,0))
                    
                ## blackout date block
                blackout_period = False
            
                term_in_target_year = False
                start_in_target_year = False
            
                plan_info = pp.get_plan_by_planid(planid,expand='InvestmentProviderLinks.InvestmentProvider')
            
                investment_provider_blackout_candidates = [provider for provider in plan_info['InvestmentProviderLinks'] if provider['InvestmentProvider']['DisplayName'] not in exclude_rks]
            
                for provider in investment_provider_blackout_candidates:
                    try:
                        provider_start_date = datetime.strptime(provider['EffectiveOn'],'%m/%d/%Y %H:%M:%S %p')
                    except:
                        provider_start_date = None
            
                    if provider_start_date is not None:
                        if project_period_start <= provider_start_date and  project_period_end>= provider_start_date:
                            start_in_target_year = True
            
                    try:
                        provider_end_date = datetime.strptime(provider['TerminatedOn'],'%m/%d/%Y %H:%M:%S %p')
                    except:
                        provider_end_date = None
            
                    if provider_end_date is not None:
                        if project_period_start <= provider_end_date and  project_period_end>= provider_end_date:
                            term_in_target_year = True        
            
                if term_in_target_year is True and start_in_target_year is True:
                    blackout_period = True
            
                ## end blackout date block
                
                # import algodocs info
                df_algodocs_info = df_algodocs[df_algodocs['plan_id'] == planid]
                df_algodocs_info.reset_index(inplace=True)
                
                missing_commission_info = False
                
                # if algodocs isn't available, we check to see if the RK doesn't have algodocs output
                if len(df_algodocs_info) == 0:
                    
                    primary_investment_provider = [provider for provider in plan_info['InvestmentProviderLinks'] if provider['IsPrimary'] is True and provider['InvestmentProvider']['DisplayName'] not in algodocs_rks]
                    
                    if len(primary_investment_provider) == 0:
                        algodocs_available = False
                        missing_commission_info = True
                        
                    elif len(primary_investment_provider) > 0:
                        algodocs_available = False
            
                else:
                    algodocs_available = True
                    
                # pull in algodocs information for schedule As
                if algodocs_available == True:
            
                    # pull fees from algodocs dataframe
                    schedulea = df_algodocs_info.at[0,'sum']
                    broker_fees = int(round(schedulea,0))
            
                if algodocs_available == False:
                    broker_fees = 0
            
                # First return
                first_year_return = False
                if added_on is not None:
                    if date_plan_added >= project_period_start and date_plan_added <= project_period_end:
                        first_year_return = True
            
                # final return
                final_return = False
                if terminated_on is not None:
                    if date_plan_terminated >= project_period_start and date_plan_terminated <= project_period_end:
                        final_return = True
            
                # amended filing
                amended_filing = False
                if "Amended" in proj_name:
                    amended_filing = True
            
                # short plan year
                short_plan_year = False
                if int(str(project_period_end - project_period_start).split(" ")[0]) < 360:
                    short_plan_year = True
            
                # effective date logic
                if relius_doc_available is True:
                    relius_effective_date_raw = df_doc.at[0,"InitialEffDate"]
                    relius_effective_date_raw = relius_effective_date_raw.replace('1st','01')
                    relius_effective_dt = datetime.strptime(relius_effective_date_raw,'%B %d, %Y')
                    relius_effective_date = datetime.strftime(relius_effective_dt,'%Y-%m-%d')
                    plan_effective_date = relius_effective_date
            
                else:
                    pp_effective_dt = datetime.strptime(added_on,'%m/%d/%Y %H:%M:%S %p')
                    pp_effective_date = datetime.strftime(pp_effective_dt,'%Y-%m-%d')
                    plan_effective_date = pp_effective_date
            
                # Get address information
                # Get all addresses for a plan, filter anything but Physical or Mailing, prioritize Mailing
            
                addresses = client_info['Addresses']
            
                addresses = [address for address in addresses if (address['AddressType']['DisplayName'] == "Physical Address" or address['AddressType']['DisplayName'] == "Mailing Address" or address['AddressType']['DisplayName'] == "Billing Address")]
            
                target_address = None
            
                address_available = False
                
                for address_all in addresses:
                    
                    # check for PO boxes and skip, since they aren't acceptable for 5500
                    if target_address is not None:
                        po_check = target_address['Address']['Address1']
                        if "PO Box" or "P.O. Box" in po_check:
                            continue
                    
                    if address_all['AddressType']['DisplayName'] == "Mailing Address":
                        target_address = address_all
                        address_available = True
                        break
            
                    elif address_all['AddressType']['DisplayName'] == "Physical Address":
                        target_address = address_all
                        address_available = True
                        continue
            
                    elif address_all['AddressType']['DisplayName'] == "Billing Address":
                        target_address = address_all
                        address_available = True
                        continue
            
                if target_address is not None:
                    address1 = target_address['Address']['Address1']
                    address2 = target_address['Address']['Address2']
                    if address2 is None:
                        address2 = ""
                    city = target_address['Address']['City']
                    state = target_address['Address']['State']
                    zipcode = target_address['Address']['Zip']
            
                    address = f"{address1} {address2}".strip()
            
                else:
                    if relius_doc_available is True:
                        address = df_doc.at[0,"EmployerStreet"]
                        address1 = address
                        address2 = ""
                        city = df_doc.at[0,"EmployerCity"]
                        state = df_doc.at[0,"EmployerState"]
                        zipcode = df_doc.at[0,"EmployerZip"]
                        address_available = True
            
                    else:
                        address = None
                        address1 = None
                        address2 = None
                        city = None
                        state = None
                        zipcode = None
                        address_available = False  
                        
                if address1 is not None:
                    if len(address1) > 35 and address1 is not None:
                        if ", " in address1:
                            address1, address2 = split_address(address1, address2, ", ")
            
                        elif " - " in address1:
                            address1, address2 = split_address(address1, address2, " - ")
            
                        elif " SUITE" in address1:
                            shortened_address_elements = address1.rsplit(' SUITE', 1)
                            address1 = shortened_address_elements[0]
                            address2 = 'SUITE ' + shortened_address_elements[1] + " " + address2
            
                        elif " STE" in address1:
                            shortened_address_elements = address1.rsplit(' STE', 1)
                            address1 = shortened_address_elements[0]
                            address2 = 'STE ' + shortened_address_elements[1] + " " + address2
            
                        else:
                            while len(address1) > 35:
                                shortened_address_elements = address1.rsplit(' ', 1)
                                address1 = shortened_address_elements[0]
                                address2 = shortened_address_elements[1] + " " + address2
            
                if address_available == False:
                    error = True
            
                # End of address block
            
                # Get EIN
                ein_available = False
                plan_cycle = [plan_cycle for plan_cycle in client_info['EmployerDatas'] if plan_cycle['PeriodStart'] == period_start]
                if len(plan_cycle) > 0:
                    ein = plan_cycle[0]['EIN']
                    ein_available = True
            
                else:
                    if relius_doc_available is True:
                        ein_relius = df_doc.at[0,"EmployerEIN"]
                        ein = ein_relius.replace("-", "").strip()
                        ein_available = True
            
                    else:
                        ein = None
                        ein_available = False
            
                if ein_available == False:
                    error = True
            
                # end EIN block
            
                # Get phone number: primary if available, "Phone Number" if not available
                phone_available = False
            
                phone_number = [phone['PhoneNumber']['Number'] for phone in client_info['Numbers'] if phone['IsPrimary'] is True]
            
                if len(phone_number) == 0:
                    phone_number = [phone['PhoneNumber']['Number'] for phone in client_info['Numbers'] if phone['PhoneNumberType']['DisplayName'] == "Phone Number"]
            
                if len(phone_number) == 0:
                    if relius_doc_available is True:
                        phone_number_relius = df_doc.at[0,"EmployerPhone"]
                        phone_number = [phone_number_relius.replace("(","").replace(") ","").replace("-","")]
            
                if len(phone_number) != 0:
                    phone_number = phone_number[0]
                    phone_available = True
            
                else:
                    phone_number = None
                    phone_available = False
            
                if phone_available == False:
                    error = True
            
                ## end phone block
            
                ## begin business code block
                
                # loop through employer data info old to new, capturing NAIC codes
                business_code_available = False
                business_code = None
                
                # Per Michael, we are going to look up the previous year's business code because its currently rejecting
                # some of the code found in pension pro. Use pension pro data only as a backup. - Andrew 6/10/24
                prior_year_business_code = df_efast_sf.loc[df_efast_sf["TPA Plan ID"] == planid].dropna(subset = 'SF_BUSINESS_CODE')
                if not prior_year_business_code.empty:
                    business_code = prior_year_business_code['SF_BUSINESS_CODE'].iloc[0]
                    business_code_available = True
                else:    
                    for employer_data in client_info["EmployerDatas"]:
                        employer_period_end = employer_data['PeriodEnd']
                        employer_period_end_dt = datetime.strptime(employer_period_end,'%m/%d/%Y %H:%M:%S %p')
            
                        if period_end_dt == employer_period_end_dt:
                            if employer_data["NAICCode"] is not None:
                                business_code = employer_data["NAICCode"]
                                business_code_available = True
            
                    if business_code_available == False:
            
                        if employer_data["NAICCode"] is not None:
                            business_code = employer_data["NAICCode"]
                            business_code_available = True
            
                if business_code_available == False:
                    error=True
            
                ## end business code block
            
                ## begin late contrib block
                late_contrib_available = False
                late_contrib = False
                late_contrib_amount = None
            
                # variable to ensure late contributions are advanced to specialist when no amount is available
                late_contrib_review = False
            
                # check for extant 5500 confirmation project
                # these are launched for plans that may have had late contributions
                extant_confirmation_proj = False
            
                confirmation_projects = pp.get_projects_by_planid(planid, filters=f"Name eq 'Form 5500 Confirmations' and PeriodEnd eq '{period_end}'",expand="TaskGroups.Tasks")
            
                confirmed_late_deposit = None
                confirmed_late_deposit_amount = None
            
                if len(confirmation_projects) > 0:
                    extant_confirmation_proj = True
                    confirmation_project = confirmation_projects[0]
            
                    completed_confirm_project = False
            
                    if confirmation_project['CompletedOn'] is not None:
                        completed_confirm_project = True
            
                    for taskgroup in confirmation_project["TaskGroups"]:
                        for task in taskgroup["Tasks"]:
                            if task["TaskName"] == "Missing 5500 Data":
                                late_contrib_items = pp.get_task_items_by_taskid(task["Id"])
            
                    for item in late_contrib_items:
                        if item["ShortName"] == 'Late Deposit Confirmation 1':
                            confirmed_late_deposit = item['Value']
                        if item["ShortName"] == 'Late Deposit Amount 1':
                            confirmed_late_deposit_amount = item['Value']
            
                # We can say there is no late contrib needed if account manager has completed
                # the confirmations project without marking one as needed
                if extant_confirmation_proj is True:
                    if confirmed_late_deposit != "Yes" and completed_confirm_project is True:
                        late_contrib_available = True
                        
                    if confirmed_late_deposit == "No":
                        late_contrib_available = True
                        
                    if confirmed_late_deposit == "Yes" and confirmed_late_deposit_amount is not None:
                        late_contrib = True
                        late_contrib_amount = float(confirmed_late_deposit_amount)
                        late_contrib_available = True
                    
                    # plan said "yes" for late contrib, and confirm project is still open with no amounts (goes to review)
                    elif confirmed_late_deposit == "Yes" and confirmed_late_deposit_amount == None and completed_confirm_project is False:
                        late_contrib = True
                        late_contrib_available = True
                        late_contrib_review = True
                    # plan said "yes" for late contrib, but confirm project is closed with no amounts. per template we report nothing in this case   
                    elif confirmed_late_deposit == "Yes" and confirmed_late_deposit_amount == None and completed_confirm_project is True:
                        late_contrib_available = True
                        
                # if the above project hasn't been launched, check for correction project
                if extant_confirmation_proj == False:
                    late_contrib_project_available = False
                    late_contrib_project_for_year = False
            
                    # pull late contribution projects, if there are any
                    late_contrib_project_name = 'SPT - Late Deposit of Deferrals, Loan Repayments, or Matching Contributions'
                    late_contrib_projects = pp.get_projects_by_planid(planid, filters=f"Name eq '{late_contrib_project_name}'",expand="TaskGroups.Tasks")
            
                    # limit it to special projects launched in the period in question or after
                    late_contrib_projects = [project for project in late_contrib_projects if project['AddedOn'].split("/")[2].split(" ")[0] == period_year or project['AddedOn'].split("/")[2].split(" ")[0] == year_after_period_year]
            
                    if len(late_contrib_projects) > 0:
                        # we want to be able to sum up values in multiple projects if they all apply to this period
                        late_contrib_sum = 0
                        late_contrib_amt = None
                        
                        for project in late_contrib_projects:
                            for taskgroup in project['TaskGroups']:
                                for task in taskgroup['Tasks']:
                                    if taskgroup['Name'] == 'Lost Earnings Calculation' and task['TaskName'] == 'Review':
                                        task_items = pp.get_task_items_by_taskid(task['Id'])
                            # get year values. they aren't project fields so I need to just pull all dates
                            task_item_values = [item['Value'] for item in task_items if item['Value'] is not None]
                            years_affected = [item.split("/")[2] for item in task_item_values if "/" in item and item != 'N/A']
            
                            # if the project pertains to the year in question, we pull the amount of the deposits and add it to the 
                            # sum variable above. Then we mark that there is a relevant project of this type
                            if period_year in years_affected:
                                late_contrib_project_available = True
                                late_contrib_project_for_year = True
                                late_contrib_amts = [item['Value'] for item in task_items if item['ShortName'] == '5500 - Compliance Questions']
                                if len(late_contrib_amts) > 0 :
                                    late_contrib_amt = late_contrib_amts[0]
                                if late_contrib_amt is not None:
                                    late_contrib_sum+=float(late_contrib_amt)
                                    late_contrib_available = True
            
                        # this should trigger if there are no values available in the SPT project, and will get the task moved to specialist for review      
                        if late_contrib_project_available == True and late_contrib_available == False:
                            late_contrib_available = True
                            late_contrib_review = True
                            
                        # this should trigger if there are SPT project, but none for the target period      
                        if late_contrib_project_for_year == False and late_contrib_available == False:
                            late_contrib_available = True
            
                    # this should trigger when there are no confirmation projects and no late contrib corrections 
                    # it will prevent an error and keep the late contrib fields blank
                    else:
                        late_contrib_available = True
            
                # only time this should trigger is if there is an incomplete confirmations project
                # or if there is an incomplete corrections project
                if late_contrib_available == False:
                    error=True
                ## end late contrib block
            
                ## begin SDBA block (2R char code)
            
                char_2r = False
            
                previous_year_projects = pp.get_projects_by_planid(planid, filters=f"ActiveOn gt '1/1/{period_year}'")
            
                target_projects = [project for project in previous_year_projects if 'Annual Valuation' in project['Name'] and project['PeriodEnd'] == period_end]
            
                for project in target_projects:
                    if project['Name'].startswith("Annual Valuation"):
                        char_2r = True
            
                ## end sdba block
            
                ## begin Fidelity Bond block
            
                fidelity_bond_available = False
            
                has_fidelity_bond = False
            
                fidelity_bond_amount = None
                fidelity_bond_values = [] #initialize list of bond values from different sources
                ## fidelity bond information from questionnaire
                    # fidelity_amount_questionnaire
                    # fidelity_auto_questionnaire
                
                ## fidelity bond information from spreadsheet
                # see if Nova purchased a fidelity bond for the plan
                df_fidelity_plan = df_fidelity_bond[df_fidelity_bond['Client ID'] == planid]
                
                
                # if so, grab that bond amount
                if len(df_fidelity_plan) > 0:
                    nova_fidelity_bond = max(df_fidelity_plan['Bond amount'].tolist())
                    fidelity_bond_values.append(nova_fidelity_bond)
                
                # check which of the purchased or reported bonds is bigger, use that value
                if str(fidelity_amount_questionnaire) != 'nan' and str(fidelity_amount_questionnaire) != 'None':
                    try:
                        fidelity_bond_values.append(float(fidelity_amount_questionnaire))
                    except:
                        try:
                            fidelity_bond_values.append(float(fidelity_amount_questionnaire.replace(",","")))
                        except:
                            pass #value is a string, useless to us for now
                
                if len(fidelity_bond_values) > 0:
                    if max(fidelity_bond_values) > 0:
                        fidelity_bond_amount = max(fidelity_bond_values)
                        fidelity_bond_available = True
                        has_fidelity_bond = True
            
                    elif max(fidelity_bond_values) == 0:
                        fidelity_bond_available = True
                        has_fidelity_bond = False
                        fidelity_bond_amount = 0    
            
                # Modify fidelity bond amount if it's an obvious auto-escalating bond
                # OR if the client indicated that it's an auto-escalating bond
                
                #     # check if fidelity bond amount is a multiple of 1000. if it is, do nothing
                #     if fidelity_bond_amount % 1000 == 0:
                #         pass
                # pull fidelity bond amount from prior form
                if fidelity_bond_amount is None and (efast_available == "5500_available" or efast_available == "sf_available"):
                    fido_amount_from_efast = None
                    if efast_available == "sf_available":
                        fido_ind_from_efast = int(df_efast_plan.at[0,"SF_PLAN_INS_FDLTY_BOND_IND"])
                        if fido_ind_from_efast == 2:
                            fidelity_bond_amount = 0
                            fidelity_bond_available = True
                        elif fido_ind_from_efast == 1:
                            fido_amount_from_efast = int(df_efast_plan.at[0,"SF_PLAN_INS_FDLTY_BOND_AMT"]) 
            
                    if efast_available == "5500_available":
                        fido_ind_from_efast = int(df_efast_plan.at[0,"PLAN_INS_FDLTY_BOND_IND"])
                        if fido_ind_from_efast == 2:
                            fidelity_bond_amount = 0
                            fidelity_bond_available = True
                        elif fido_ind_from_efast == 1:
                            fido_amount_from_efast = int(df_efast_plan.at[0,'PLAN_INS_FDLTY_BOND_AMT'])
                        
                    if fido_amount_from_efast != None:
                        fidelity_bond_amount = fido_amount_from_efast
                        fidelity_bond_available = True
                        has_fidelity_bond = True
                        
                if fidelity_bond_amount is None:
                    fidelity_bond_amount = 0
                
                # else if it's not an even multiple or if they indicated auto-escalate and we have prior year data, calculate the amount
                if (fidelity_bond_amount % 1000 != 0 or fidelity_auto_questionnaire == 'Complete') and efast_available is not False:
                    if efast_available == "sf_available":
                        eoy_amount_from_efast = int(df_efast_plan.at[0,"SF_NET_ASSETS_EOY_AMT"]) # get EOY net assets as integer (sf)
            
                    if efast_available == "5500_available":
                        eoy_amount_from_efast = int(df_efast_plan.at[0,'TOT_ASSETS_EOY_AMT']) # get EOY net assets as integer (5500)
            
                    # formula to calculate bond amount: divide ending balance by 10, take minimum of that or 500,000,
                    # take maximum of that or 20,000, round to the nearest integer, store as integer instead of float
                    fidelity_bond_amount = int(round(max(20000,min(500000,eoy_amount_from_efast/10)),0))
                    has_fidelity_bond = True
                    fidelity_bond_available = True
                    print("recalculated fidelity bond amount:", fidelity_bond_amount)
                
                if fidelity_bond_amount is not None:   
                    if fidelity_bond_amount > 500000:
                        fidelity_bond_amount = 500000
                        
                    if fidelity_bond_amount < 20000 and fidelity_bond_amount > 0:
                        fidelity_bond_amount = 20000
                        
                    fidelity_bond_amount = int(fidelity_bond_amount)
                    print("fidelity bond amount:", fidelity_bond_amount)
            
            # removing this so these proceed- we're picking this up on the specialist review step
            #     if fidelity_bond_available == False:
            #         error=True
                                             
                ## end Fidelity Bond block
            
                ## check for prior year EIN
            
                ein_from_efast = None
                ein_mismatch = False
            
                if efast_available == "sf_available":
                    ein_from_efast = int(df_efast_plan.at[0,"SF_SPONS_EIN"])
            
                if efast_available == "5500_available":
                    ein_from_efast = int(df_efast_plan.at[0,"SCH_H_EIN"])
            
                if ein_from_efast is not None:
                    ein_from_efast = str(ein_from_efast).zfill(9)
                    if ein_from_efast != ein:
                        ein_mismatch = True
            
                ## end prior year EIN check
                                             
                ## begin first irs compliance block
                
                # defaults are False
                plansatisfytests = False
                plan401kdesignbased = False
                plan401kprioryear = False
                plan401kcurrentyear = False
                plan401kNA = False  
                                                              
                # first, if the plan is a MEP, 14a and 14b are left blank, so we only check these for non-MEPs
                if mep_status == False:                       
            
                    # answer to aggregation question is Yes if client answered Yes in Questionnaire
                    if multi_plan_questionnaire == "Yes":
                        plansatisfytests = True
                    
                    # also yes if combo plan
                    if combo_plan == "Yes, Other TPA" or combo_plan == "Yes - DB/DC Combo Plan":
                        plansatisfytests = True
                    
                    # also yes if 410b pdf read earlier says so
                    if other_plan_410b is True:
                        plansatisfytests = True
                elif mep_status:
                    Add_Note_To_MEP_Project(planid,projid)
                                            
                ## end first irs compliance block
                
                # initialize characteristic codes
                
                char_2a = False
                char_2c = False
                char_2e = False
                char_2f = False
                char_2g = False
                char_2h = False
                char_2j = False
                char_2k = False
                char_2l = False  
                char_2m = False
                char_2s = False
                char_3b = False
                # 3D: Pre-approved doc (all plans with the relius extract have this), hardcoded
                char_3d = True
                char_3h = False
                # 2T: Default investment, hardcoded
                char_2t = True 
                
                cc_string = ""
                
                ### These next parts extract information from the document extract to populate
                if relius_doc_available is True:
            
                    # 2A: new comp
                    if df_doc.at[0,"ERDiscrPSNonSafeHarbAlloc"] == "x":
                        char_2a = True
            
                    # 2C: MPP
                    if df_doc.at[0,"ProdCyc3MPPPlan"] == "x":
                        char_2c = True       
            
                    # 2E: Profit Sharing
                    if df_doc.at[0,"ContrTypeERNonElect"] == "x":
                        char_2e = True   
            
                    # 2F: Participant-directed investment
            
                    if df_doc.at[0,"DirInvAcc"] == "x":
                        char_2f = True       
            
                    # 2G: Total participant-directed investment
            
            
                    if df_doc.at[0,"DirInvAccAll"] == "x":
                        char_2g = True    
            
                    # 2H: Partial participant-directed investment
            
            
                    partial_participant_direction_relius_list = ['DirInvAccSpecifyAccounts',
                                                                 'DirInvAccElectDef',
                                                                 'DirInvAccRothElectDef',
                                                                 'DirInvAccQMCERMC',
                                                                 'DirInvAccNE',
                                                                 'DirInvAccQNEC',
                                                                 'DirInvAccRoll',
                                                                 'DirInvAccTransfer',
                                                                 'DirInvAccVol',
                                                                 'DirInvAccOther']
            
                    for provision in partial_participant_direction_relius_list:
                        if df_doc.at[0,f"{provision}"] == "x":
                            char_2h = True
            
                    # 2J: Allows deferrals
                    
            
                    if df_doc.at[0,"ContrTypeElectDef"] == "x":
                        char_2j = True  
            
                    # 2K: Allows match
            
                    if df_doc.at[0,"ContrTypeSafeHarbor"] == "x" or df_doc.at[0,"ContrTypeERMatchContr"] == "x":
                        char_2k = True  
                    
                    # 2L and 2M: for 403b plans
            
                    
                    if plan_type == 'ERISA 403(b)':
                        char_2l = True
                        char_2m = True
                    
                    # 2S: Auto-enrollment
                    
            
                    if df_doc.at[0,"AutoDeferProvisionYes"] == "x":
                        char_2s = True
            
                    # 3B: Self-employed
            
                    if df_doc.at[0,"LLCPartnerSoleProp"] == "x" or df_doc.at[0,"SoleProp"] == "x" or df_doc.at[0,"Partnership"] == "x":
                        char_3b = True        
            
                    # 3H: Controlled group    
            
                    if df_doc.at[0,"YAffilEmployer"] == "x" or df_doc.at[0,"YControlGrp"] == "x":
                        char_3h = True  
            
                    # 2U_MEP_ASSN, 2V_PEO_MEP, 2X_MEP_OTHER, no directive for these
            
                    # generate characteristic_code_string:
            
                    if char_2a is True:
                        cc_string = cc_string+",2A"
                    if char_2c is True:
                        cc_string = cc_string+",2C"
                    if char_2e is True:
                        cc_string = cc_string+",2E"
                    if char_2f is True:
                        cc_string = cc_string+",2F"
                    if char_2g is True:
                        cc_string = cc_string+",2G"
                    if char_2h is True:
                        cc_string = cc_string+",2H"
                    if char_2j is True:
                        cc_string = cc_string+",2J"
                    if char_2k is True:
                        cc_string = cc_string+",2K"
                    if char_2l is True:
                        cc_string = cc_string+",2L"
                    if char_2m is True:
                        cc_string = cc_string+",2M"
                    if char_2s is True:
                        cc_string = cc_string+",2S"
                    if char_2t is True:
                        cc_string = cc_string+",2T"
                    if char_2r is True:
                        cc_string = cc_string+",2R"
                    if char_3b is True:
                        cc_string = cc_string+",3B"
                    if char_3d is True:
                        cc_string = cc_string+",3D"            
                    if char_3h is True:
                        cc_string = cc_string+",3H"
            
                    # remove leading comma
                    cc_string = cc_string[1:]
                    
                    ## begin second irs compliance block
                    # these depend on relius info
                    
                    # first we skip all this if plan is a MEP:
                    if mep_status == False:   
                        # design based safe harbor is "yes" if marked as such in plan doc
                        if df_doc.at[0,"ContrTypeSafeHarbor"] == "x":                       
                            plan401kdesignbased = True
            
                        # prior year ADP testing check
                        if df_doc.at[0,"NHCEPriorYR"] == "x":                                                      
                            plan401kprioryear = True
            
                        # current year ADP testing check (non-SH plans only)
                        if df_doc.at[0,"NHCEPriorYR"] != "x" and df_doc.at[0,"ContrTypeSafeHarbor"] != "x":                                                                                      
                            plan401kcurrentyear = True
                                             
                        # N/A response (we can't pull ADP info), so just pulling no deferrals
                        if df_doc.at[0,"ContrTypeElectDef"] != "x":              
                            plan401kNA = True  
              
                    ## end first irs compliance block      
                    
                #Check for missing NAIC codes or IRS Plan ID
                missing_irs_num = False
                missing_naic_code = False
                
                if irs_number == "" or irs_number is None:
                    missing_irs_num = True
                    
                    error = True
                    
                if business_code == "" or business_code is None:
                    missing_naic_code = True
                    error = True
                    
                if phone_number == "" or phone_number is None:
                    error = True
                
            
                
                # Debugging break
            #     if error == True:
            #         break
                if error == False:
                    ## Dump information into DGEM text import file
                    # dgem_dictionary = dict.fromkeys(template_columns,"")
                    
                    print(planid, "printing to output")
            
                    plan_year = period_end.split("-")[0]
            
                    mep = 1
            
                    if mep_status is True:
                        mep = 2
            
                    initial_filing = convert_TF_to_digit(first_year_return)
                    amended = convert_TF_to_digit(amended_filing)
                    final = convert_TF_to_digit(final_return)
                    short = convert_TF_to_digit(short_plan_year)
                    late = convert_TF_to_one_two(late_contrib)
                    fidelity = convert_TF_to_one_two(has_fidelity_bond)
                    
            #         ## BOND DEBUG
            #         if fidelity == 1 and fidelity_bond_amount == 0:
            #             raise Exception(f'{b}, {planid}, BAD FIDO!')
                    
                    if broker_fees > 0:
                        fee_yn = 1
                    else:
                        fee_yn = 2
                        broker_fees = ""
            
                    blackout = convert_TF_to_one_two(blackout_period)
            
                    if blackout == 1:
                        blackout_notice = 1
                    else:
                        blackout_notice = ""
            
                    loan_yn = 2
            
                    if loan_amount > 0:
                        loan_yn = 1
                        loan_amt_formatted = loan_amount
            
                    else:
                        loan_amt_formatted = ""
                        
            #         ## LOAN DEBUG
            #         if loan_yn == 1 and loan_amt_formatted == "":
            #             raise Exception(f'{b}, {planid}, BAD LOAN!')
                    
                    if " DBA " in client_name:
                        ps_name = client_name.split(" DBA ")[0]
                        dba = client_name.split(" DBA ")[1]
            
                    elif " dba " in client_name:
                        ps_name = client_name.split(" dba ")[0]
                        dba = client_name.split(" dba ")[1]
            
                    else: 
                        ps_name = client_name
                        dba = ""
                        
                    while len(ps_name) > 70:
                        ps_name = ps_name[:ps_name.rfind(', ')]
                                             
                    union_yn = convert_TF_to_digit(collectivelybargained)
                    plansatisfytests_yn = convert_TF_to_one_two(plansatisfytests)
                    plan401kdesignbased_yn = convert_TF_to_digit(plan401kdesignbased)
                    plan401kprioryear_yn = convert_TF_to_digit(plan401kprioryear)
                    plan401kcurrentyear_yn = convert_TF_to_digit(plan401kcurrentyear)
                    plan401kNA_yn = convert_TF_to_digit(plan401kNA)
                                             
                    opinion_letter_dt = datetime.strptime(opinion_letter_date,'%m/%d/%Y')
                    opinion_letter_date_dgem = datetime.strftime(opinion_letter_dt,'%Y-%m-%d')
                    
            
                    dgem_dictionary = {'SponsorEIN': ein,
                     'SponsPlanNum': f"{irs_number}",
                     'PlanYear': plan_year.split("/")[2].split(" ")[0],
                     'PlanYearBeginDate': period_start_dgem,
                     'PlanYearEndDate': period_end_dgem,
                     'TypePlanEntityCd': mep,
                     'InitialFilingInd': initial_filing,
                     'AmendedInd': amended,
                     'FinalFilingInd': final,
                     'ShortPlanYrInd': short,
                     'Form5558ApplicationFiledInd': '',
                     'ExtAutomaticInd': '',
                     'DFVCProgramInd': '',
                     'ExtSpecialInd': '',
                     'ExtSpecialText': '',
                     'AdoptedPlanSECUREAct': '',
                     'PlanName': plan_name.upper().strip(),
                     'SponsorPlanNum': irs_number,
                     'PlanEffDate': plan_effective_date,
                     'SponsorName': ps_name.upper().strip(),
                     'SponsorDbaName': dba.upper().strip(),
                     'SponsorCareOfName': '',
                     'SponsorUSAddressAddressLine1': address1.upper().strip(),
                     'SponsorUSAddressAddressLine2': address2.upper().strip(),
                     'SponsorUSAddressCity': city.upper().strip(),
                     'SponsorUSAddressState': state.upper().strip(),
                     'SponsorUSAddressZipCode': zipcode.strip(),
                     'SponsorForeignAddressAddressLine1': '',
                     'SponsorForeignAddressAddressLine2': '',
                     'SponsorForeignAddressCity': '',
                     'SponsorForeignAddressProvinceOrState': '',
                     'SponsorForeignAddressCountry': '',
                     'SponsorForeignAddressPostalCode': '',
                     'SponsorUSLocationAddressAddressLine1': '',
                     'SponsorUSLocationAddressAddressLine2': '',
                     'SponsorUSLocationAddressCity': '',
                     'SponsorUSLocationAddressState': '',
                     'SponsorUSLocationAddressZipCode': '',
                     'SponsorForeignLocationAddressAddressLine1': '',
                     'SponsorForeignLocationAddressAddressLine2': '',
                     'SponsorForeignLocationAddressCity': '',
                     'SponsorForeignLocationAddressProvinceOrState': '',
                     'SponsorForeignLocationAddressCountry': '',
                     'SponsorForeignLocationAddressPostalCode': '',
                     'SponsorPhoneNum': phone_number,
                     'SponsorForeignPhoneNum': '',
                     'BusinessCode': business_code,
                     'AdminNameSameAsSponsorInd': 1,
                     'AdminName': '',
                     'AdminCareOfName': '',
                     'AdminUSAddressAddressLine1': '',
                     'AdminUSAddressAddressLine2': '',
                     'AdminUSAddressCity': '',
                     'AdminUSAddressState': '',
                     'AdminUSAddressZipCode': '',
                     'AdminForeignAddressAddressLine1': '',
                     'AdminForeignAddressAddressLine2': '',
                     'AdminForeignAddressCity': '',
                     'AdminForeignAddressProvinceOrState': '',
                     'AdminForeignAddressCountry': '',
                     'AdminForeignAddressPostalCode': '',
                     'AdminEIN': '',
                     'AdminPhoneNum': '',
                     'AdminPhoneNumForeignPhoneNum': '',
                     'LastRptPlanName': '',
                     'LastRptSponsName': '',
                     'LastRptSponsEIN': '',
                     'LastRptPlanNum': '',
                     'TotPartcpBoyCnt': boy_participants,
                     'TotActRtdSepBenefCnt': eoy_participants,
                     'PartcpAccountBalCnt': eoy_participants_w_acct,
                     'TotActPartcpBoyCnt': boy_active_participants,
                     'TotActPartcpEoyCnt': eoy_active_participants,
                     'SepPartcpPartlVstdCnt': term_unvested_participants,
                     'EligibleAssetsInd': 1,
                     'IQPAWaiverInd': 1,
                     'CoveredPBGCInsuranceInd': '',
                     'PremiumFilingConfirmationNum': '',
                     'TotAssetsBoyAmt': '',
                     'TotLiabilitiesBoyAmt': '',
                     'NetAssetsBoyAmt': '',
                     'TotAssetsEoyAmt': eoy_assets,
                     'TotLiabilitiesEoyAmt': '',
                     'NetAssetsEoyAmt': '',
                     'EmplrContribIncomeAmt': employer_contrib,
                     'ParticipantContribIncomeAmt': part_contrib,
                     'OthContribRcvdAmt': other_contrib,
                     'OtherIncomeAmt': other_income,
                     'TotIncomeAmt': '',
                     'TotDistribBnftAmt': benefits_paid,
                     'CorrectiveDeemedDistribAmt': deemed_or_corrective_dist,
                     'AdminSrvcProvidersAmt': salaries_fees_commissions,
                     'OthExpensesAmt': '',
                     'TotExpensesAmt': '',
                     'NetIncomeAmt': '',
                     'TotPlanTransfersAmt': '',
                     'TypePensionBnftCode': cc_string,
                     'TypeWelfareBnftCode': '',
                     'FailTransmitContribInd': late,
                     'FailTransmitContribAmt': late_contrib_amount,
                     'PartyInIntNotRptdInd': 2,
                     'PartyInIntNotRptdAmt': '',
                     'PlanInsFdltyBondInd': fidelity,
                     'PlanInsFdltyBondAmt': fidelity_bond_amount,
                     'LossDiscvDurYearInd': 2,
                     'LossDiscvDurYearAmt': '',
                     'BrokerFeesPaidInd': fee_yn,
                     'BrokerFeesPaidAmt': broker_fees,
                     'FailProvideBenefitDueInd': 2,
                     'FailProvideBenefitDueAmt': '',
                     'PartcpLoansInd': loan_yn,
                     'PartcpLoansEoyAmt': loan_amt_formatted,
                     'PlanBlackoutPeriodInd': blackout,
                     'ComplyBlackoutNoticeInd': blackout_notice,
                     'DbPlanFundingReqdInd': '',
                     'UnpaidMinContribCurrYrTotAmt': '',
                     'PBGCNotifiedCd': '',
                     'PBGCNotifiedExplanationText': '',
                     'DcPlanFundingReqdInd': 2,
                     'RulingLetterGrantDate': '',
                     'Sec412ReqContribAmt': '',
                     'EmplrContribPaidAmt': '',
                     'FundingDeficiencyAmt': '',
                     'FundingDeadlineInd': '',
                     'ResTermPlanAdptInd': '',
                     'ResTermPlanAdptAmt': '',
                     'AllPlanAstDistribInd': '',
                     'CollectivelyBargained': union_yn,
                     'PartcpAccountBalCntBoy': int(boy_participants_w_acct),
                     'PlanSatisfyTestsInd': plansatisfytests_yn,
                     'Plan401kDesignBasedInd': plan401kdesignbased_yn,
                     'Plan401kPriorYearADPTestInd': plan401kprioryear_yn,
                     'Plan401kCurrentYearADPTestInd': plan401kcurrentyear_yn,
                     'Plan401kNAInd': plan401kNA_yn,
                     'OpinLtrDate': opinion_letter_date_dgem,
                     'OpinSerialNum': opinion_letter_serial,
                     'AdminSignature': '',                
                     'TPA Plan ID': planid}        
                                
                    regex_or_length_issue = False
                    
                    for i in df_dgem_instructions.index:
                        tag_name = df_dgem_instructions.at[i,"Tag Name"]
                        tag_regex = df_dgem_instructions.at[i,"Regular Expressions"]
                        try:
                            max_length = int(df_dgem_instructions.at[i,"Max Length"])
            
                        except:
                            max_length = int(df_dgem_instructions.at[i,"Max Length"].split(" ")[-1])
            
                        # test regex string to see if it's valid. if no, continue
                        test_value = dgem_dictionary.get(tag_name)
                        
                        if test_value != "":
                            if re.match(f'{tag_regex}', str(test_value)):
                                pass
                            else:
                                print("REGEX VIOLATION:", i, planid, f'{tag_regex}', str(test_value))
                                regex_rule_violation += f"Format Violation: {tag_name} - {test_value}; "
                                regex_or_length_issue = True
                                
                        if len(str(test_value)) > max_length:
                            print("max length violation:", i, planid, f'{max_length}', str(test_value))
                            regex_rule_violation += f"Length Violation: {tag_name} - {test_value}; "
                            regex_or_length_issue = True
                            
                    if regex_or_length_issue is False:
                        dgem_dictionary_values = list(dgem_dictionary.values())
            
                        dgem_dictionary_values = [item.replace('\t', ' ') if type(item) == str and '\t' in item else item for item in dgem_dictionary_values]
            
                        plan_dgem_output = dgem_dictionary_values
                        dgem_dataframe_values.append(plan_dgem_output)
            
                        
                
                
                ## collate information above into new line to put into a dataframe:
                pp_output = [planid,
                period_start,
                period_end,
                mep_status,
                added_on,
                effective_on,
                terminated_on,
                irs_number,
                taskid,
                projid,
                task_name,
                plan_name,
                client_id,
                pp_planid,
                proj_name,
                plan_status,
                plan_category,
                plan_type,
                form5500,
                combo_plan,
                plan_end,
                plan_group,
                error,
                client_name,
                boy_participants,
                eoy_participants,
                eoy_participants_w_acct,
                boy_active_participants,
                eoy_active_participants,
                term_unvested_participants,
                eoy_assets,
                employer_contrib,
                part_contrib,
                other_income,
                benefits_paid,
                deemed_or_corrective_dist,
                salaries_fees_commissions,
                loan_amount,
                first_year_return,
                final_return,
                amended_filing,
                short_plan_year,
                plan_effective_date,
                address_available,
                address,
                city,
                state,
                zipcode,
                ein_available,
                ein,
                phone_available,
                phone_number,
                business_code_available,
                business_code,
                late_contrib_available,
                late_contrib,
                late_contrib_amount,
                char_2r,
                fidelity_bond_available,
                has_fidelity_bond,
                fidelity_bond_amount,
                blackout_period,
                char_2a,
                char_2c,
                char_2e,
                char_2f,
                char_2g,
                char_2h,
                char_2j,
                char_2k,
                char_2l,
                char_2m,
                char_2s,
                char_2t,
                char_3b,
                char_3d,
                char_3h,
                cc_string,
                relius_doc_available,
                asc_extract_available,
                algodocs_available,
                asc_extract_read_success,
                missing_commission_info,
                missing_irs_num,
                missing_naic_code,
                collectivelybargained,
                boy_participants_w_acct,
                plansatisfytests,
                plan401kdesignbased,
                plan401kprioryear,
                plan401kcurrentyear,
                plan401kNA,
                opinion_letter_date,
                opinion_letter_serial,
                signer_name,
                signer_email,
                signer_cc,
                regex_rule_violation,
                ]
                    
                pp_update_data.append(pp_output)
                
                b+=1
                
            print('Done!')
            
            
            # In[36]:
            
            
            # from IPython.display import display, HTML
            # display(HTML("<style>.container { width:100% !important; }</style>"))
            
            
            # In[37]:
            
            
            template_columns = df_template.columns.tolist()
            template_columns.append("TPA Plan ID")
            
            
            # In[38]:
            
            
            # # restore backup df values
            # dgem_dataframe_values = dgem_dataframe_values0
            
            dfk = pd.DataFrame(dgem_dataframe_values, columns=template_columns)
            # dfk['einlu'] = dfk['SponsPlanNum'] + dfk['SponsorEIN']
            # dfk
            
            
            # In[39]:
            
            
            dfk
            
            
            # In[40]:
            
            
            
            
            dfk['NetAssetsEoyAmt'] = dfk['TotAssetsEoyAmt'].replace('', 0) - dfk['TotLiabilitiesEoyAmt'].replace('', 0)
            dfk['TotIncomeAmt'] = dfk[['EmplrContribIncomeAmt', 'ParticipantContribIncomeAmt', 'OthContribRcvdAmt', 'OtherIncomeAmt']].sum(axis=1)
            dfk['TotExpensesAmt'] = dfk[['TotDistribBnftAmt', 'CorrectiveDeemedDistribAmt', 'AdminSrvcProvidersAmt', 'OthExpensesAmt']].sum(axis=1)
            dfk['NetIncomeAmt'] = dfk['TotIncomeAmt'].replace('', 0) - dfk['TotExpensesAmt'].replace('', 0)
            dfk['ResTermPlanAdptInd'] = dfk['ResTermPlanAdptInd'].replace('', 2)
            dfk['AllPlanAstDistribInd'] = dfk['AllPlanAstDistribInd'].replace('', 2)
            
            # format late contribution column
            dfk['FailTransmitContribAmt'] = dfk['FailTransmitContribAmt'].fillna(0)
            dfk['FailTransmitContribAmt'] = dfk['FailTransmitContribAmt'].round().astype(int)
            dfk['FailTransmitContribAmt'] = dfk['FailTransmitContribAmt'].replace(0, '')
            
            
            # In[ ]:
            
            
            
            
            
            # In[41]:
            
            
            df_efast_sf = pd.read_excel(efast_extract_path, sheet_name="5500 SF", dtype={'EIN Lookup':str})
            df_efast_5500 = pd.read_excel(efast_extract_path, sheet_name="5500", dtype={'EIN Lookup':str})
            
            # cols5500 = ['EIN Lookup', 'Form Identifier', 'TotAssetsEoyAmt (H)', 'ACK_ID', 'PARTCP_LOANS_EOY_AMT']
            cols5500 = ['EIN Lookup', 'Form Identifier', 'TOT_ASSETS_EOY_AMT', 'ACK_ID', 'PARTCP_LOANS_EOY_AMT']
            
            colssf = ['EIN Lookup', 'Form Identifier', 'SF_NET_ASSETS_EOY_AMT', 'ACK_ID', 'SF_PARTCP_LOANS_IND', 'SF_PARTCP_LOANS_EOY_AMT']
            
            ef5500 = df_efast_5500[cols5500].copy()
            ef5500.insert(4, 'lyn', 2)
            efsf = df_efast_sf[colssf].copy()
            
            efcols = ['einlu', 'planid', 'tot', 'date', 'lyn', 'lamt']
            
            ef5500.columns = efcols
            efsf.columns = efcols
            
            
            ef = pd.concat([ef5500, efsf])
            ef = ef.sort_values(['planid', 'date']).drop_duplicates(subset=['planid'], keep='last').drop(columns=['date']).copy()
            
            
            len(ef5500), len(efsf), len(ef)
            
            
            # In[42]:
            
            
            # dff = dfk.merge(ef, how='left', on='planid').fillna({'tot':0})
            dff = dfk.merge(ef, how='left', right_on='planid', left_on="TPA Plan ID").fillna({'tot':0})
            
            dff.tot = dff.tot.astype(int)
            dff.lyn = dff.lyn.fillna(pd.NA)
            dff.lamt = dff.lamt.fillna(pd.NA)
            # dff.lyn = dff.lyn.fillna(2).astype(int)
            # dff.lamt = dff.lamt.fillna(0).astype(int)
            
            
            # In[43]:
            
            
            # dff[['einlu', 'planid', 'PartcpLoansInd', 'PartcpLoansEoyAmt', 'lyn', 'lamt']]
            
            
            # In[44]:
            
            
            # dff[dff.lamt.notna()].PartcpLoansInd.value_counts()
            
            
            # In[45]:
            
            
            # causing issues this year, commenting out since the loan stuff should be handled by the other part sufficiently well
            
            # dff['PartcpLoansInd'] = np.where(dff['PartcpLoansEoyAmt'] == '', np.where(dff.lamt.notna(), 1, 2), dff['PartcpLoansInd'])
            
            
            # In[46]:
            
            
            # dff[dff.lamt.notna()].PartcpLoansInd.value_counts()
            
            
            # In[47]:
            
            
            dif3 = (dff['TotAssetsEoyAmt'] - (dff.tot + dff['EmplrContribIncomeAmt'] + dff['ParticipantContribIncomeAmt'] + dff['OthContribRcvdAmt'] + dff['OtherIncomeAmt'] - dff['TotDistribBnftAmt'] - dff['CorrectiveDeemedDistribAmt'] - dff['AdminSrvcProvidersAmt'] - dff['OthExpensesAmt'].replace('', 0) + dff['TotPlanTransfersAmt'].replace('', 0)))
            dff['original_balance'] = (dff['TotAssetsEoyAmt'] - (dff.tot + dff['EmplrContribIncomeAmt'] + dff['ParticipantContribIncomeAmt'] + dff['OthContribRcvdAmt'] + dff['OtherIncomeAmt'] - dff['TotDistribBnftAmt'] - dff['CorrectiveDeemedDistribAmt'] - dff['AdminSrvcProvidersAmt'] - dff['OthExpensesAmt'].replace('', 0) + dff['TotPlanTransfersAmt'].replace('', 0)))
            
            cond3 = abs(dif3) < 3
            
            
            # In[48]:
            
            
            dff['OtherIncomeAmt'] = dff['OtherIncomeAmt'].mask(cond3, dff['OtherIncomeAmt'] + dif3)
            
            
            # In[49]:
            
            
            # re-calculate the totincomeamt
            dff['TotIncomeAmt'] = dff[['EmplrContribIncomeAmt', 'ParticipantContribIncomeAmt', 'OthContribRcvdAmt', 'OtherIncomeAmt']].sum(axis=1)
            dff['NetIncomeAmt'] = dff['TotIncomeAmt'].replace('', 0) - dff['TotExpensesAmt'].replace('', 0)
            
            
            # In[50]:
            
            
            dff['SponsorUSAddressZipCode'] = dff['SponsorUSAddressZipCode'].str.replace('-', '')
            
            
            # In[51]:
            
            
            dgem_dataframe_values0 = dgem_dataframe_values
            
            
            # In[52]:
            
            
            
            
            dgem_dataframe_values = dff.iloc[:, :-6].values.tolist()
            df_report = dff.iloc[:, :].values.tolist()
            
            
            # In[ ]:
            
            
            
            
            
            # In[53]:
            
            
            # len(dgem_dataframe_values), len(dgem_dataframe_values0)
            
            
            # In[54]:
            
            
            # df_report
            
            
            # In[ ]:
            
            
            
            
            
            # In[55]:
            
            
            # create dgem export file, plus xlsx version
            
            if len(dgem_dataframe_values) > 0:
                df_dgem_import = pd.DataFrame(dgem_dataframe_values, columns=template_columns,dtype=str)
                
                df_obj = df_dgem_import.select_dtypes(['object'])
            
                df_dgem_import[df_obj.columns] = df_obj.apply(lambda x: x.str.strip())
                generated_import_file = f'Y:/ASC/Exported Reports/5500 Automation/DGEM Import Files/{today}_DGEM_SF_Import.txt'    
                df_dgem_import.to_csv(generated_import_file, sep ='\t', index=False, encoding='utf-8',doublequote=False, escapechar='\\')
                
                dff.to_excel(f'Y:/ASC/Exported Reports/5500 Automation/DGEM Import Files/{today}_DGEM_Import.xlsx', index=None)
                
            else: 
                df_dgem_import = pd.DataFrame()
                generated_import_file = "No import"
            
            
            # In[56]:
            
            
            df_dgem_import
            
            
            # In[57]:
            
            
            # final log write
            
            if len(pp_update_data) > 0:
                df_pp_output = pd.DataFrame(pp_update_data, columns=pp_logging_columns)
                
            else:
                df_pp_output = pd.DataFrame()
            
            writer = pd.ExcelWriter(f'Y:/5500/2023/Automation/SF Production/Logging/{today}_5500 SF Production Log.xlsx', engine='xlsxwriter')
            
            # Write each dataframe to a different worksheet.
            df_pp_output.to_excel(writer, sheet_name='PP Data Output')
            df_dgem_import.to_excel(writer, sheet_name='DGEM Import File')
            
            # Close the Pandas Excel writer and output the Excel file.
            writer.save()
            
            # Save and release handle
            writer.close()
            writer.handles = None
            
            
            # In[ ]:
            
            
            
            
            
            # In[58]:
            
            
            # browser.quit()
            
            
            # In[59]:
            
            
            now = datetime.now()
            previous_year = str(int(now.strftime("%Y"))-1)
            
            my_username = Username('DGEM')
            my_pw = Password('DGEM')
            
            o365_directory = Path(r'C:\Users\Public\WPy64-39100\notebooks\scheduler')
            
            user = os.getlogin()
            
            download_path = f'C:/Users/{user}/Downloads'
            
            def get_latest_file(directory):
                ## Reads in the find results. This will let us use the names as selenium lookups
            
                fr_files = [os.path.join(directory, file) for file in os.listdir(directory) if os.path.isfile(os.path.join(directory, file))]
            
                # Sort the files based on modification time (latest first)
                sorted_fr_files = sorted(fr_files, key=lambda x: os.path.getmtime(x), reverse=True)
            
                # Get the latest modified file
                latest_find_file = sorted_fr_files[0] if sorted_fr_files else ""
            
                # Convert the latest_file path to string
                return str(latest_find_file)
            
            if generated_import_file != "No import":
            
                browser = webdriver.Chrome(ChromeDriverManager().install())
                # Login-----------------------------------------------------------------------
                browser.get('https://dgem.asc-net.com/ascidoc/login.aspx')
            
                tpakey = browser.find_element_by_name('tbTPA')
                tpakey.send_keys('asc438')
            
                username = browser.find_element_by_name('tbUsername')
                username.send_keys(my_username)               
            
                WebDriverWait(browser,10).until(ec.visibility_of_element_located((By.NAME,'tbPassword')))
                password = browser.find_element_by_name('tbPassword')
                password.send_keys(my_pw)
            
                loginbutton = browser.find_element_by_name('btnLogin')
                loginbutton.click()
                
                
                try:
                    # Click to send verification code via "Email"
                    WebDriverWait(browser,10).until(ec.visibility_of_element_located((By.XPATH,'//*[@id="MFAUC_rbEmail"]')))
                    browser.find_element_by_xpath('//*[@id="MFAUC_rbEmail"]').click()
            
                    # Click SEND
                    browser.find_element_by_xpath('//*[@id="MFAUC_btnSend"]').click()
                    
                    if generated_import_file != "No import":
                        from O365 import Account, FileSystemTokenBackend
            
                        credentials = OAuth('o365_client_id'), OAuth('o365_secret_value')
            
                        token_backend = FileSystemTokenBackend(token_path=o365_directory,
                                                               token_filename='oauth_token.txt')
                        account = Account(credentials,
                                          token_backend=token_backend,
                                          scopes = ['basic','message_all','mailbox'])
            
                        if not account.is_authenticated:
                            account.authenticate()
            
                        time.sleep(60)
            
                        mailbox = account.mailbox(resource='automation@nova401k.com')
            
                        inbox = mailbox.inbox_folder().get_messages()
            
                        for message in inbox:
                            msg_sender = str(message.sender)
                            if (msg_sender == 'ASC (no-reply@pension-plan-emails.com)'
                                and (msg_subject := message.subject) == 'Verification Code'):
                                msg_body = message.body
                                regex = re.search(r'(?<=>)(?P<code>\d{6})(?=<)',msg_body) # find 6-digit num if between < and >
                                verification_code = regex.group('code')
                                message.mark_as_read()
                                break
            
                        # Enter Verification code
                        browser.find_element_by_xpath('//*[@id="MFAUC_tbCode"]').send_keys(verification_code)
            
                        # Click verify
                        browser.find_element_by_xpath('//*[@id="MFAUC_btnVerify"]').click()
            
                        # Click Yes
                        WebDriverWait(browser,10).until(ec.visibility_of_element_located((By.XPATH,'//*[@id="MFAUC_rbYes"]')))
                        browser.find_element_by_xpath('//*[@id="MFAUC_rbYes"]').click()
            
                        # Click "Continue"
                        browser.find_element_by_xpath('//*[@id="MFAUC_btnContinue"]').click()
            
                    else:
                        exit()
                except:
                    # no MFA required
                    pass
            
            
            # In[ ]:
            
            
            
            
            
            # In[ ]:
            
            
            
            
            
            # In[ ]:
            
            
            
            
            
            # In[60]:
            
            
            time.sleep(15)
            
            browser.get('https://dgem.asc-net.com/ascidoc/efast2/wizards/Import_3rdParty_txt.aspx')
            
            time.sleep(15)
            
            dropdown_upload = browser.find_element(by='id',value='ddlFormType').send_keys("Form 5500 SF")
            upload_path = browser.find_element(by='id',value='fileSelect').send_keys(generated_import_file)
            
            email_box = browser.find_element(by='id',value='tbEmail').send_keys("automation@nova401k.com")
            email_next = browser.find_element(by='id',value='ibtnUpload').click()
            
            time.sleep(60)
            
            
            # In[61]:
            
            
            email_found=False
            timeout = time.time() + 60*15   # timeout 15 minutes from now
            
            from O365 import Account, FileSystemTokenBackend
            
            credentials = OAuth('o365_client_id'), OAuth('o365_secret_value')
            
            token_backend = FileSystemTokenBackend(token_path=o365_directory,
                                                   token_filename='oauth_token.txt')
            account = Account(credentials,
                              token_backend=token_backend,
                              scopes = ['basic','message_all','mailbox'])
            
            if not account.is_authenticated:
                account.authenticate()
                
            mailbox = account.mailbox(resource='automation@nova401k.com')
            while email_found is False:
                time.sleep(10)
                inbox = mailbox.inbox_folder().get_messages()
                if time.time() > timeout:
                    print("timeout")
                    break
                for message in inbox:
                    msg_sender = str(message.sender)
                    msg_time_sent = message.sent #get time message was sent
                    msg_time_sent = msg_time_sent.replace(tzinfo=None)
                    recent_email = msg_time_sent > datetime.now() - timedelta(minutes=10)
                    if (msg_sender == 'Import results (support@pension-plan-emails.com)'
                        and (msg_subject := message.subject) == 'Completed importing files'
                       and recent_email is True):
                        msg_body = message.body
                        regex = re.search(r'<a class="fill-div" href="([^"]+)"',msg_body) # find sendgrid url
                        import_report_download = regex.group(1)
                        message.mark_as_read()
                        email_found=True
                        break
                        
            if email_found is True:       
                browser.get(import_report_download)
            
            else:
                raise Exception("No email found")
                
            time.sleep(30)
            
            
            # In[62]:
            
            
            ## Reads in the import results
            latest_import_result = get_latest_file(download_path)
            import_directory = r'Y:\ASC\Exported Reports\5500 Automation\DGEM Import Logs'
            
            df_import_results = pd.read_table(latest_import_result, converters={"EIN":str,"Plan Number":str})
            
            df_import_results['Lookup'] = df_import_results['Plan Number'] + df_import_results['EIN']
            
            just_import_file = latest_import_result.split('\\')[1]
            shutil.copy(latest_import_result, f"{import_directory}/{just_import_file}")
            os.remove(latest_import_result)
            
            
            # In[63]:
            
            
            df_import_results[df_import_results['Result'] != "Fail"]
            
            
            # In[64]:
            
            
            df_import_results
            
            
            # In[65]:
            
            
            dff['Lookup'] = dff['SponsPlanNum'] + dff['SponsorEIN']
            dff
            
            
            # In[66]:
            
            
            df_import_concat = dff.merge(df_import_results, how='left', on="Lookup")
            df_successful_upload = df_import_concat[df_import_concat['Result'] == "Success"]
            
            df_failed_upload = df_import_concat[df_import_concat['Result'] != "Success"]
            
            
            # In[67]:
            
            
            df_failed_upload
            
            
            # In[68]:
            
            
            # Advances the projects that had DGEM upload issues to specialists for correction
            
            note_text1 = "Needs to be completed manually."
            
            task_names = ['Automation Work','Specialist Correction of ASC Data']
            
            for i in df_failed_upload.index[:]:
                planid = df_failed_upload.at[i,'TPA Plan ID']
                period_end = df_failed_upload.at[i,'PlanYearEndDate']
                error_message = df_failed_upload.at[i,'Message']
            
                note_text2 = "DGEM Upload error - " + error_message
                
                projects = pp.get_projects_by_planid(planid,filters=f"PeriodEnd eq '{period_end}' and Name eq 'DC Annual Governmental Forms - Small Filer (Automated)'", expand="TaskGroups.Tasks")
                
                try:
                    project = projects[0]
                    
                except:
                    continue
                    
                projid = project['Id']
                
                for taskgroup in project['TaskGroups']:
                    for task in taskgroup['Tasks']:
                        if task['TaskName'] in task_names and task['DateCompleted'] is None:
                            pp.override_task(task['Id'])
                            print(task['Id'])
                
                payload1 = {
                            "ProjectID": projid, 
                            "NoteText": f"{note_text1}",
                            "ShowOnPSL": False
                                }
                payload2 = {
                            "ProjectID": projid, 
                            "NoteText": f"{note_text2}",
                            "ShowOnPSL": False
                                }
            
                x = pp.add_note(payload1)
                y = pp.add_note(payload2)
                
                print(planid,period_end,error_message)
            
            
            # In[69]:
            
            
            df_sfpull
            
            
            # In[70]:
            
            
            # # Advances all sf projects left
            
            # note_text1 = "Needs to be completed manually."
            
            # task_names = ['Automation Work','Specialist Correction of ASC Data']
            
            # for i in df_sfpull.index[1:]:
            #     planid = df_sfpull.at[i,'planid']
            #     projid = df_sfpull.at[i,'projid']
            
            #     note_text2 = "Miscellaneous issue preventing automated work"
                
            #     project = pp.get_project_by_projectid(projid, expand="TaskGroups.Tasks")
                    
            #     projid = project['Id']
                
            #     for taskgroup in project['TaskGroups']:
            #         for task in taskgroup['Tasks']:
            #             if task['TaskName'] in task_names and task['DateCompleted'] is None:
            #                 pp.override_task(task['Id'])
            #                 print(task['Id'])
                
            #     payload1 = {
            #                 "ProjectID": projid, 
            #                 "NoteText": f"{note_text1}",
            #                 "ShowOnPSL": False
            #                     }
            #     payload2 = {
            #                 "ProjectID": projid, 
            #                 "NoteText": f"{note_text2}",
            #                 "ShowOnPSL": False
            #                     }
            
            #     x = pp.add_note(payload1)
            #     y = pp.add_note(payload2)
                
            #     print(planid)
            
            
            # In[ ]:
            
            
            
            
            
            # In[71]:
            
            
            # get plans that weren't able to be put onto import spreadsheet, to advance for manual work
            df_failed_generation = df_pp_output[df_pp_output['error'].astype(str) == 'True']
            
            # get string for unsupported form year- in future iteration we may be able to support this
            unsupported_form_year =  int(previous_year)-1
            
            note_text1 = "Needs to be completed manually."
            
            for i in df_failed_generation.index[:]:
                
                advance = False
                note_text2 = ""
                
                planid = df_failed_generation.at[i,'planid']
                projid = df_failed_generation.at[i,'projid']
                short_plan_year = str(df_failed_generation.at[i,'short_plan_year'])
                period_start = df_failed_generation.at[i,'period_start']
                period_end = df_failed_generation.at[i,'period_end']
                ein_available = str(df_failed_generation.at[i,'ein_available'])
                relius_doc_available = str(df_failed_generation.at[i,'relius_doc_available'])
                phone_available = str(df_failed_generation.at[i,'phone_available'])
                business_code_available = str(df_failed_generation.at[i,'business_code_available'])
                fidelity_bond_available = str(df_failed_generation.at[i,'fidelity_bond_available'])
                asc_extract_available = str(df_failed_generation.at[i,'asc_extract_available'])
                
            
                period_end_year = int(period_end_dgem.split("-")[0])
                
                unsupported_form_date = False
                
                if fidelity_bond_available == "False":
                    note_text_to_add0 = "Fidelity bond information unavailable. Get from prior year form or contact account manager for more information <br>"
                    note_text2 += note_text_to_add0
                    advance = True
                    
                if period_end_year - unsupported_form_year != 1:
                    unsupported_form_date = True
                    note_text_to_add1 = "Unsupported plan year: " + str(period_end_year) + " <br>"
                    note_text2 += note_text_to_add1
                    advance = True
                
                if short_plan_year == "True":
                    note_text_to_add2 = "Process does not yet support short plan years. <br>"
                    note_text2 += note_text_to_add2
                    advance = True
                                      
                if ein_available == "False":
                    note_text_to_add3 = "Plan EIN is not available for the relevant period end. <br>"
                    note_text2 += note_text_to_add3
                    advance = True
            
                if relius_doc_available == "False":
                    note_text_to_add4 = "Relius plan document extract is not available. <br>"
                    note_text2 += note_text_to_add4
                    advance = True
                                      
                if phone_available == "False":
                    note_text_to_add5 = "There is no phone number on file for the plan. <br>"
                    note_text2 += note_text_to_add5
                    advance = True
                                      
                if business_code_available == "False":
                    note_text_to_add6 = "Business code is not available or not valid. <br>"
                    note_text2 += note_text_to_add6
                    advance = True
                    
                if asc_extract_available == "False":
                    note_text_to_add7 = "Issue reading ASC extract, must import into DGEM manually"
                    note_text2 += note_text_to_add7
                    advance = True
                    
                if advance is False:
                    print("not being advanced",period_end_year, unsupported_form_date, short_plan_year)
                    continue
                    
                if advance is True:
            
                        project = pp.get_project_by_projectid(projid, expand="TaskGroups.Tasks")
            
                        for taskgroup in project['TaskGroups']:
                            for task in taskgroup['Tasks']:
                                if task['TaskName'] in task_names and task['DateCompleted'] is None:
                                    pp.override_task(task['Id'])
                                    print(task['Id'])
            
                        payload1 = {
                                    "ProjectID": f'{projid}', 
                                    "NoteText": note_text1,
                                    "ShowOnPSL": False
                                        }
                        
                        payload2 = {
                                    "ProjectID": f'{projid}', 
                                    "NoteText": note_text2,
                                    "ShowOnPSL": False
                                        }
            
                        pp.add_note(payload1)
                        pp.add_note(payload2)
            
                        print(planid,period_end,note_text2)        
                
                print(period_end_year, unsupported_form_date, short_plan_year)
            
            
            # In[72]:
            
            
            df_failed_generation
            
            
            # In[73]:
            
            
            # download signer list after import, then update signers where necessary
            
            browser.get('https://dgem.asc-net.com/ascidoc/efast2/wizards/importSignerData.aspx')
            
            time.sleep(5)
            
            browser.find_element_by_css_selector('#rbWithData').click()
            browser.find_element_by_css_selector('#btnCreateTemplate').click()
            
            template_link_found = False
            
            while template_link_found == False:
                time.sleep(5)
                if "Download your template" in browser.page_source:
                    browser.find_element_by_link_text("here").click()
                    template_link_found = True
                    
            time.sleep(15)
              
            
            
            # In[74]:
            
            
            
            latest_signer_form = get_latest_file(download_path)
            signer_import_directory = r'Y:\ASC\Exported Reports\5500 Automation\Signer Import Files'
            
            df_signer_results = pd.read_excel(latest_signer_form, converters={'EIN':str,'PN':str,'Year':str}, na_values='')
            
            df_signer_results['Lookup'] = df_signer_results['PN'] + df_signer_results['EIN']
            
            just_signer_file = latest_signer_form.split('\\')[1]
            shutil.copy(latest_signer_form, f"{signer_import_directory}/{just_signer_file}")
            os.remove(latest_signer_form)
            
            
            # In[ ]:
            
            
            
            
            
            # In[75]:
            
            
            target_signer_plans = dff['Lookup'].tolist()
            
            df_signer_lookup = df_pp_output[df_pp_output['signer_email'].astype(str) != 'None']
            df_signer_lookup['Lookup'] = df_signer_lookup['irs_number'] + df_signer_lookup['ein']
            df_signer_lookup = df_signer_lookup[df_signer_lookup['Lookup'].isin(target_signer_plans)]
            
            
            # In[76]:
            
            
            df_signer_processing = df_signer_results[df_signer_results['Lookup'].isin(target_signer_plans)]
            
            
            # In[77]:
            
            
            now = datetime.now()
            
            today = now.strftime('%Y-%m-%d_%H.%M.%S')
            
            
            # In[78]:
            
            
            for i in df_signer_processing.index:
                signer_lookup = df_signer_processing.at[i,'Lookup']
                
                df_signer_lookup_ind = df_signer_lookup[df_signer_lookup['Lookup'] == signer_lookup]
                df_signer_lookup_ind.reset_index(inplace=True)
                if len(df_signer_lookup_ind) > 0:
                    lookup_signer_name = df_signer_lookup_ind.at[0,'signer_name']
                    lookup_signer_email = df_signer_lookup_ind.at[0,'signer_email']
                    lookup_signer_cc = df_signer_lookup_ind.at[0,'signer_cc']
                    
                    if "automation@nova401k.com" not in lookup_signer_cc:
                        if lookup_signer_cc == "":   
                            lookup_signer_cc = "automation@nova401k.com"
                        else:    
                            lookup_signer_cc = lookup_signer_cc + ",automation@nova401k.com"
                    
                    df_signer_processing.at[i,'Admin Name'] = lookup_signer_name
                    df_signer_processing.at[i,'Admin Email'] = lookup_signer_email
                    df_signer_processing.at[i,'Admin CC'] = lookup_signer_cc
                    
            df_signer_processing = df_signer_processing.drop('Lookup', axis=1)
            
            df_signer_processing = df_signer_processing.fillna('')
            new_signer_file_name = f"signersData_import_updated_{today}.xlsx"
            df_signer_processing.to_excel(f'{signer_import_directory}/{new_signer_file_name}',index=False)
            
            
            # In[ ]:
            
            
            
            
            
            # In[79]:
            
            
            # re-import updated signer information to DGEM
            
            browser.get('https://dgem.asc-net.com/ascidoc/efast2/wizards/importSignerData.aspx')
            
            time.sleep(5)
            
            id_import_path = browser.find_element(by='id',value='fileSelect').send_keys(f'{signer_import_directory}/{new_signer_file_name}')
            
            import_button = browser.find_element(by='id',value='ibtnImport').click()
            
            signer_import_success = False
            
            while signer_import_success == False:
                time.sleep(5)
                if "Data was successfully imported" in browser.page_source:
                    signer_import_success = True
                    
                    try:
                        browser.find_element_by_link_text("Download log file").click()
                        time.sleep(15)
                        signer_import_log = get_latest_file(download_path)
                        just_signer_import_log = signer_import_log.split('\\')[1]
                        shutil.copy(signer_import_log, f"{signer_import_directory}/{just_signer_import_log}")
                        os.remove(signer_import_log)
                        
                    except:
                        pass
            
            
            # In[ ]:
            
            
            
            
            
            # In[80]:
            
            
            # Download Find results
            time.sleep(10)
            errorlist = []
            
            browser.get('https://dgem.asc-net.com/ascidoc/Find.aspx')
            
            time.sleep(10)
            checkbox = browser.find_element(by='name',value='cb5500StatusChangedAfter').click()
            
            time.sleep(5)
            checkbox = browser.find_element(by='id',value='lbtnNext_5500').click()
            
            time.sleep(20)
            
            dropdown_menu = browser.find_element(by='name',value='lbAction5500').send_keys("Export Find Results")
            
            find_next = browser.find_element(by='id',value='lbtnClientNext5500').click()
            
            time.sleep(10)
            
            link_check = False
            
            while link_check is False:
                if "Export is complete." in browser.page_source:
                    time.sleep(1)
                    browser.find_element(by='link text',value='here').click()
                    link_check = True
                    
            time.sleep(20)
            
            ## Reads in the find results. This will let us use the names as selenium lookups
            latest_find_result = get_latest_file(download_path)
            
            results = pd.read_html(latest_find_result, converters={"EIN":str,"PlanNumber":str})
            df_results = results[0]
            
            df_results['Lookup'] = df_results['PlanNumber'] + df_results['EIN']
            
            os.remove(latest_find_result)
            
            
            # In[ ]:
            
            
            
            
            
            # In[81]:
            
            
            df_successful_upload['Plan Name'] = df_successful_upload['Plan Name'].apply(lambda x: saxutils.unescape(x))
            
            df_dl_vl_targets = df_results.merge(df_successful_upload, on='Lookup')
            
            
            # In[82]:
            
            
            try:
                planlist = list(set(df_dl_vl_targets['PlanName'].tolist()))
                
            except:
                planlist = list(set(df_dl_vl_targets['PlanName_x'].tolist()))
            
            # planlist = df_dl_vl_targets['Lookup'].tolist()
              
            # planlist = [saxutils.unescape(plan) for plan in planlist]
            print(len(planlist))
            
            
            # In[83]:
            
            
            # # time.sleep(15)
            
            # browser.get('https://dgem.asc-net.com/ascidoc/efast2/wizards/ImportIdentifiers.aspx')
            
            # time.sleep(5)
            
            # id_import_path = browser.find_element(by='id',value='fileSelect').send_keys(identifier_import_name)
            
            # import_button = browser.find_element(by='id',value='ibtnImport').click()
            
            
            # In[ ]:
            
            
            
            
            
            # In[84]:
            
            
            planlist
            
            
            # In[85]:
            
            
            # Request PDFs to be emailed to automation@nova401k.com
            
            
            
            browser.get('https://dgem.asc-net.com/ascidoc/Find.aspx')
            
            checkbox = browser.find_element(by='name',value='cb5500StatusChangedAfter').click()
            time.sleep(5)
            checkbox = browser.find_element(by='id',value='lbtnNext_5500').click()
            
            
            # In[86]:
            
            
            b=0
            errorlist = []
            for plan in planlist[b:]:
                try:
                    browser.find_element(by='xpath',value=f'//td[contains(text(),"{plan}")]/ancestor::tr[1]//input[@type = "checkbox"]').click()
                    b+=1
                    print(plan, b, planlist.index(plan))
            
                except:
                    errorlist.append(plan)
                    continue
            
            dropdown_menu = browser.find_element(by='name',value='lbAction5500').send_keys("Export PDF (5500VS Batch)")
            
            
            # In[ ]:
            
            
            
            
            
            # In[ ]:
            
            
            
            
            
            # In[87]:
            
            
            find_next = browser.find_element(by='id',value='lbtnClientNext5500').click()
            
            time.sleep(10)
            
            email_box = browser.find_element(by='id',value='tbEmail').send_keys("automation@nova401k.com")
            email_next = browser.find_element(by='id',value='btnEmailInputNext').click()
            
            time.sleep(60)
            email_found=False
            timeout = time.time() + 60*15   # timeout 15 minutes from now
            
            while email_found is False:
                time.sleep(10)
                inbox = mailbox.inbox_folder().get_messages()
                if time.time() > timeout:
                    print("timeout")
                    break
                for message in inbox:
                    msg_sender = str(message.sender)
                    msg_time_sent = message.sent #get time message was sent
                    msg_time_sent = msg_time_sent.replace(tzinfo=None)
                    recent_email = msg_time_sent > datetime.now() - timedelta(minutes=5)
                    if (msg_sender == 'Form 5500 PDFs (support@pension-plan-emails.com)'
                        and (msg_subject := message.subject) == 'Completed Batch PDF creation for 5500 forms'
                       and recent_email is True):
                        msg_body = message.body
                        regex = re.search(r'<a class="fill-div" href="([^"]+)"',msg_body) # find sendgrid url
                        pdf_download = regex.group(1)
                        message.mark_as_read()
                        email_found=True
                        break
                        
            if email_found is True:       
                browser.get(pdf_download)
            
            else:
                raise Exception("No email found")
            
            
            time.sleep(30)
            
            
            # In[88]:
            
            
            ## Extract the zip files
            
            # Get latest file in dl directory
            latest_file_string = get_latest_file(download_path)
            
            try:
                dirname = latest_file_string.split(".zip")[0]
                
            except:
                raise Exception("No zip file found")
                
            ret = subprocess.check_output([path_7zip, "e", "-y", "-aou", latest_file_string, f"-o{dirname}"])
            time.sleep(20)
            os.remove(latest_file_string)
            unzipped_pdfs = os.listdir(dirname)
            for file in unzipped_pdfs:
                shutil.copy(f"{dirname}/{file}", f'Y:/ASC/Exported Reports/5500 Automation/Put Downloaded PDF Files Here/{file}')
                os.remove(f"{dirname}/{file}")
                            
            os.removedirs(dirname)
            
            
            # In[ ]:
            
            
            
            
            
            # In[89]:
            
            
            ## create folder for validation files()
            
            validation_directory = 'Y:/ASC/Exported Reports/5500 Automation/DGEM Validation Files'
            
            new_validation_directory = f'{validation_directory}/{today}_Validation Logs'
            
            os.mkdir(new_validation_directory)
            
            
            # In[ ]:
            
            
            
            
            
            # In[90]:
            
            
            ## Pre-validate and get validation file
            
            errorlist = []
            
            b=0
            
            # gotta do these in batches of 100
            c = -(-(len(planlist)) // 100)
            
            for i in range(c):
                planlista = planlist[i*100:(i+1)*100]
                browser.get('https://dgem.asc-net.com/ascidoc/Find.aspx')
            
                checkbox = browser.find_element(by='name',value='cb5500StatusChangedAfter').click()
            
            
                time.sleep(10)
                checkbox = browser.find_element(by='id',value='lbtnNext_5500').click()
                time.sleep(20)    
                for plan in planlista[:]:
            
                    try:
                        browser.find_element(by='xpath',value='//td[contains(text(),"{}")]/ancestor::tr[1]//input[@type = "checkbox"]'.format(plan)).click()
                        b+=1
                        print(plan, b, planlist.index(plan))
            
                    except:
                        errorlist.append(plan)
                        continue
            
                dropdown_menu = browser.find_element(by='name',value='lbAction5500').send_keys("Pre-validate (5500VS Batch)")
            
                find_next = browser.find_element(by='id',value='lbtnClientNext5500').click()
            
                link_check = False
            
                while link_check is False:
                    if "Pre-Validate completed" in browser.page_source:
                        time.sleep(10)
                        browser.find_element(by='link text',value='Click here').click()
                        link_check = True
            
                time.sleep(15)
            
            
            
                # Get downloaded file
                latest_file_string = get_latest_file(download_path)
            
                just_file = latest_file_string.split('\\')[1]
                shutil.copy(latest_file_string, f"{new_validation_directory}/{just_file}")
                os.remove(latest_file_string)
            
            
            # In[91]:
            
            
            pdf_root = 'Y:/5500/2023/Automation/SF Production/PDF Downloads'
            
            # Switch active directory to the folder where the PDFs are placed after download
            os.chdir('Y:/ASC/Exported Reports/5500 Automation/Put Downloaded PDF Files Here')
            pdf_folder = os.listdir()
            pdf_folder = [file for file in pdf_folder if file.endswith(".pdf")]
            
            pdf_folder = ([file for file in pdf_folder if 'Form5500_' in file and not '__' in file and not 'Identifier' in file])
            len(pdf_folder)
            
            if len(pdf_folder) > 0:
            
                # create directory for current date for copy of files
                newdir_name = f'{pdf_root}/Dated Form Downloads/{today} PDF Downloads'
                
                if not os.path.exists(newdir_name):
                
                    os.mkdir(newdir_name)
            
                for file in pdf_folder:
                
                    tpa_planid = file.split("_")[1]
                    if "Form 5500-SF.pdf" not in file:
                        try:
                            os.rename(file, f"{tpa_planid}_Form 5500-SF_{today}.pdf")
            
                        except:
                            pass
                    else:
                        tpa_planid = file.split("_")[0]
                
                
            print('Done!')
            
            # get folder contents again after renaming
            pdf_folder = os.listdir()
            pdf_folder = [file for file in pdf_folder if file.endswith(".pdf")]
            target_files = ([file for file in pdf_folder if not '__' in file and not 'Identifier' in file])
            len(pdf_folder)
            
            len(pdf_folder), len(target_files)
            
            
            
            target_files[0].split('_')[0]
            
            
            
            a = target_files[:]
            a
            
            # THIS PART NEEDS TO BE UPDATED TO DYNAMICALLY CHANGE YEAR FOLDER
            # Also going to be an issue with off-calendar plans, since I don't know if those are consistent
            
            
            # In[ ]:
            
            
            
            
            
            # In[92]:
            
            
            for file in a:
                
                try:
                
                
                    tpa_planid = file.split('_')[0]
            
            
            #     this should work for now since these are the only projects, will need to update this to work off a worktray in the future 
            
                    projects = pp.get_projects_by_planid(tpa_planid, filters="Name eq 'DC Annual Governmental Forms - Small Filer (Automated)'", expand="TaskGroups.Tasks")
                    project = [project for project in projects if project['CompletedOn'] is None]
                    if len(project) > 0:
                        project = project[0]
                    for taskgroup in project['TaskGroups']:
                        for task in taskgroup['Tasks']:
                            if task['TaskName'] == 'Automation Work' or task['TaskName'] == 'Specialist Correction of ASC Data':
                                pp.override_task(task['Id'])
                                print(tpa_planid, task['TaskName'], "overridden")
            
                    nova.copy_file(file,f'2023/5500')
                    shutil.copy(file,f'{newdir_name}/{file}')
                    shutil.move(file,f'{pdf_root}/All Processed PDF Downloads/{file}')
                    
                except Exception as e:
                    
                    print('                         ', tpa_planid, 'error', e)
                    continue
                        
            print('Done!')
            
            ## STEPS BELOW WILL FILL OUT SPECIALIST REVIEW TASKS BASED ON VALIDITY RESULTS
            
            # get list of all plans that have output in this folder, to exclude them from the ASC target list
            # os.chdir('Y:/ASC/Exported Reports/5500 Automation/All Output')
            os.chdir('Y:/ASC/Exported Reports/5500 Automation/All Output')
            
            all_output_folder = os.listdir()
            planids_with_asc_output = list(set([file.split("_")[0] for file in all_output_folder]))
            print(len(planids_with_asc_output))
            
            # ## read in Relius data extract
            # relius_extract_path = r'Y:\Automation\Projects\Active\5500 SF Automation\2022\Relius Document Data 20230530.xlsx'
            
            # df_relius = pd.read_excel(relius_extract_path)
            
            # df_relius.dropna(subset=['EmployerEIN'], inplace=True)
            
            # ## read in algodocs commissions information
            # jh_algodocs_path = r'Y:\Automation\Projects\Active\5500 SF Automation\2022\Fees & Commissions\Master Files\JH\JH Commissions Info Master.xlsx'
            # voya_algodocs_path = r'Y:\Automation\Projects\Active\5500 SF Automation\2022\Fees & Commissions\Master Files\Voya\Voya Commissions Info Master.xlsx'
            
            # add other RK algodocs paths as they become available
            
            # # create single dataframe for lookup below
            # df_algodocs = pd.read_excel(jh_algodocs_path).append(pd.read_excel(voya_algodocs_path))
            # df_algodocs.reset_index(inplace=True)
            
            # algodocs_rks = ['Ameritas',
            # 'AXA Equitable',
            # 'Empower - Non-Automated Distributions',
            # 'Empower Retirement',
            # 'John Hancock',
            # 'Lincoln',
            # 'Lincoln - Alliance',
            # 'Lincoln - Director',
            # 'Nationwide',
            # 'Nationwide Pension Prof',
            # 'Nationwide Regular',
            # 'Ohio National',
            # 'OneAmerica (formerly AUL)',
            # 'OneAmerica Alliance Plus',
            # 'Principal',
            # 'Securian',
            # 'Securian Financial',
            # 'The Standard',
            # 'Transamerica',
            # 'Voya',
            # 'Voya - ACES',
            # 'Voya - EASE']
            
            # # read in JH loan and fee information
            # jh_pars_path = r'Y:\Automation\Karen\JH PARS\ALL_PARS.xlsx'
            # df_pars = pd.read_excel(jh_pars_path)
            
            # split out column with Nova plan IDs
            # df_pars['planid'] = df_pars['File name'].apply(lambda x: pd.Series(str(x).split(" ")))[0]
            # df_pars = df_pars.fillna(0)
            
            # # create new columns for ease of pulling info below
            # df_pars['fee_total'] = df_pars['Total JH Contract Admin Fees'] + df_pars['Total TPA Fees'] + df_pars['Total Redemption Fees'] + df_pars['Total Inv Adv Fees'] + df_pars['JH GIFL Fees']
            # df_pars['loans'] = df_pars['Loan Value EOY']
            # df_pars['corrections'] = df_pars['Deemed loan distributions'] + df_pars['Corrective Distributions']
            
            # # get 2021 EFAST data extract to get EOY balance for escalated fidelity bond amounts
            # efast_extract_path = r'Y:\5500\2021\EFAST 5500 Data 2021.xlsx'
            # df_efast_sf = pd.read_excel(efast_extract_path, sheet_name="5500 SF")
            # df_efast_5500 = pd.read_excel(efast_extract_path, sheet_name="5500")
            
            # # DGEM upload template
            # template_source = r'Y:\5500\2022\Automation\SF Production\DGEM txt template.xlsx'
            # df_template = pd.read_excel(template_source)
            # template_columns = df_template.columns.tolist()
            
            # ## import DGEM template details in order to test output values for form validity and length
            # import re
            
            # instructions_path = r'Y:\5500\2022\Automation\SF Production\ASC_DGEM Form 5500SF Template_2022.xlsx'
            
            # df_dgem_instructions = pd.read_excel(instructions_path)
            
            
            # In[ ]:
            
            
            
            
            
            # In[93]:
            
            
            # df_populate = get_worktray_for_sf('5500 Preparation')
            # df_populate = df_populate[df_populate['task_name'] == 'Specialist Review of Form 5500']
            
            df_auto = get_worktray_for_sf('5500 Preparation')
            df_auto = df_auto[df_auto['task_name'] == 'Specialist Review of Form 5500']
            
            
            # In[ ]:
            
            
            
            
            
            # In[ ]:
            
            
            
            
            
            # In[94]:
            
            
            ## concatenate all the validation files just downloaded
            
            os.chdir(new_validation_directory)
            
            validation_files = os.listdir()
            
            b = 0
            
            if len(validation_files) > 0:
            
                for valfile in validation_files:
                    if b == 0:
                        b+=1
                        try:
                            valdata = pd.read_html(valfile, converters={"EIN":str,"Plan Number":str})
                            dfv = valdata[0]
                            dfv_header = dfv.iloc[0] #grab the first row for the header
                            dfv = dfv[1:] #take the data less the header row
                            dfv.columns = dfv_header #set the header row as the df header    
            
                        except:
                            dfv = pd.read_excel(valfile, converters={"EIN":str,"Plan Number":str})
                    else:
                        try:
                            valdata1 = pd.read_html(valfile, converters={"EIN":str,"Plan Number":str})
                            dfv1 = valdata1[0]
                            dfv_header1 = dfv1.iloc[0] #grab the first row for the header
                            dfv1 = dfv1[1:] #take the data less the header row
                            dfv1.columns = dfv_header1 #set the header row as the df header    
            
                        except:
                            dfv1 = pd.read_excel(valfile, converters={"EIN":str,"Plan Number":str})
                        dfv = dfv.append(dfv1)
                        print(len(dfv))
            
            
            # In[ ]:
            
            
            
            
            
            # In[95]:
            
            
            
            dfv['Plan Number'] = dfv['Plan Number'].str.zfill(3)
            dfv['Lookup'] = dfv['Plan Number'] + dfv['EIN']
            
            ##
            
            df_validation = dfv.merge(df_results, on='Lookup')
            df_validation['TPA Plan ID'] = df_validation['f5500Id']
            
            
            validation_list = df_validation['TPA Plan ID'].tolist()
            validation_list = list(set(validation_list))
            validation_list = [str(value) for value in validation_list if "nan" not in str(value)]
            validation_list = [value.split(".")[0] for value in validation_list]
            
            # df_populate_backup = df_populate
            
            # df_populate = df_auto
            
            df_populate = df_auto[df_auto['planid'].isin(validation_list)]
            df_populate.reset_index(inplace=True)
            
            print(len(df_populate))
            
            
            # In[96]:
            
            
            ## here are the validation warnings we ignore: ie judge that they are valid
            #ignore_val_warnings = ['J-502SF','Z-007','Z-010','Z-005']
            ignore_val_warnings = ['Z-007','Z-005']
            
            
            # In[97]:
            
            
            # first, we pull the ones that fully validated
            df_validated = df_validation[df_validation['Result'] == 'Success']
            
            # then we separate all the ones that weren't
            df_validation_warnings = df_validation[~(df_validation['Result'] == 'Success')]
            
            # then we separate out all the fatal errors and get their filing IDs into a list
            df_validation_fatal_errors = df_validation_warnings[~df_validation['Error Code'].isin(ignore_val_warnings)]
            fatal_errors_list = df_validation_fatal_errors['filingRecordId'].tolist()
            
            # then we pull all the ones that didn't succeed, but which only had one of the non-fatal warnings
            df_validation_ignore_warnings = df_validation_warnings[(df_validation['Error Code'].isin(ignore_val_warnings)) & ~(df_validation['filingRecordId'].isin(fatal_errors_list))]
            # planid_validated = df_validated['TPA Plan ID'].tolist()
            # len(planid_validated)
            
            # finally we concat them together into one df
            df_validated = df_validated.append(df_validation_ignore_warnings)
            
            # get list to use later
            validated_plan_list = df_validated['TPA Plan ID'].tolist()
            validated_plan_list = [str(plan).split(".")[0] for plan in validated_plan_list if "." in str(plan)]
            validated_plan_list
            
            
            # In[98]:
            
            
            os.chdir('Y:/ASC/Exported Reports/5500 Automation/All Output')
            
            
            # In[99]:
            
            
            df_populate
            
            
            # In[100]:
            
            
            # get all client information (40 at a time to prevent the query from erroring)
            clientids = df_populate['client_id'].tolist()
            
            c = -(-(len(clientids)) // 40)
                
            all_clients = []
            
            for i in range(c):
                clientidsa = clientids[i*40:(i+1)*40]
                filters = ' or '.join([f'ClientId eq {clientid}' for clientid in clientidsa])
                expand = "CompanyName,EmployerDatas,Addresses.AddressType,Addresses.Address,Numbers.PhoneNumberType,Numbers.PhoneNumber"
                clients = pp.get_clients(filters=filters,expand=expand)['Values']
                all_clients.extend(clients)
            # end get client block
            
            
            # In[101]:
            
            
            advance_task_counter = 0 # dont move this.
            
            
            # In[102]:
            
            
            print(f"{len(df_populate)} items to work on.")
            
            
            # In[103]:
            
            
            # retroactively populate fields
            
            b=0
            ends_on = None
            
            for i in df_populate.index[b:ends_on]:
                
            #     try:
                print(b)
                error = False
                
                planid = df_populate.at[i,'planid']
                period_start = df_populate.at[i,'period_start']
                period_end = df_populate.at[i,'period_end']
                mep_status = df_populate.at[i,'mep_status']
                added_on = df_populate.at[i,'added_on']
                effective_on = df_populate.at[i,'effective_on']
                terminated_on = df_populate.at[i,'terminated_on']
                irs_number = df_populate.at[i,'irs_number']
                taskid = df_populate.at[i,'taskid']
                projid = df_populate.at[i,'projid']
                task_name = df_populate.at[i,'task_name']
                plan_name = df_populate.at[i,'plan_name']
                client_id = df_populate.at[i, 'client_id']
                proj_name = df_populate.at[i,'proj_name']
                plan_status = df_populate.at[i,'plan_status']
                plan_category = df_populate.at[i,'plan_category']
                plan_type = df_populate.at[i,'plan_type']
                form5500 = df_populate.at[i,'form5500']
                plan_end = df_populate.at[i,'plan_end']
                plan_group = df_populate.at[i,'plan_group']
                
                period_year = period_end.split("/")[2].split(" ")[0]
                year_after_period_year = str(int(period_year) + 1)
                  
                # plan year end back to current year to only capture likely period end refunds
                #previous_year_projects = pp.get_projects_by_planid(planid, filters=f"ActiveOn gt '1/1/{year_after_period_year}'",expand="Plan.MultipleEmployerPlan,Plan.Status,Plan.PlanType,Plan.PlanCategory,Plan.FilingStatus,Plan.PlanGroup,TaskGroups.Tasks")
                previous_year_projects = pp.get_projects_by_planid(planid, filters=f"ActiveOn gt '1/1/{period_year}'",expand="Plan.MultipleEmployerPlan,Plan.Status,Plan.PlanType,Plan.PlanCategory,Plan.FilingStatus,Plan.PlanGroup,TaskGroups.Tasks")
                
                
                # exclude the employer refunds projects
                # get refund projects for field population later
                refund_projects = [project for project in previous_year_projects if ('Refund' in project['Name'] and project['Name'] != 'DC Excess ER Contribution Refunds')]
                if len(refund_projects) > 0:
                    refunds = True
                    
                else:
                    refunds = False
                    
                # create dataframe of relius information
                df_doc = df_relius[df_relius['TPA Plan ID'] == planid]
                df_doc.reset_index(inplace=True)
                if len(df_doc) == 0:
                    relius_doc_available = False
                    error = True
                    print(planid, "error at line x48")
            
                else:
                    relius_doc_available = True
            
                # import ASC extract information
                # we should still be pointed at the "All Output" dir
                # for 2023 forms, we added logic to ensure we're pulling the right PYE
                asc_extract_available = False
                asc_extract = [file for file in all_output_folder if "_5500SFExport" in file and file.startswith(f"{planid}_") and file.endswith(".txt")]
                if len(asc_extract) > 0:
                    for file in asc_extract:
                        file_periodend = file.split("_")[1]
                        file_periodend_dt = datetime.strptime(file_periodend,'%m%d%Y')
                        file_periodend_dt = file_periodend_dt + timedelta(hours=12)
                        if file_periodend_dt == project_period_end:
                            try:
                                df_asc = pd.read_table(file,header=None)
                                asc_extract_available = True
                                break
                            except:
                                asc_extract_available = False
                                break 
                                
                    if asc_extract_available == False:
                        asc_extract_available = False
                        error = True   
                        print(planid, "error at line x76 ")
                else:
                    asc_extract_available = False
                    error = True
                    print(planid, "error at line x79")
            
                # import status grid to check for missing dates
                blank_date_found = False
                status_grids = [file for file in all_output_folder if file.endswith("_Status Grid.txt") and file.startswith(f"{planid}_")]
                if len(status_grids) > 0:
                    for file in status_grids:
                        file_periodend = file.split("_")[1]
                        file_periodend_dt = datetime.strptime(file_periodend,'%m%d%Y')
                        file_periodend_dt = file_periodend_dt + timedelta(hours=12)
                        if file_periodend_dt == project_period_end:
                            df_status_grid = pd.read_table(file)
            
                            df_status_grid.replace({pd.NaT: 0}, inplace=True)
            
                            for i in df_status_grid.index:
                                birth_date = df_status_grid.at[i,'Birth Date']
                                hire_date = df_status_grid.at[i,'Hire Date']
                                if birth_date == 0 or hire_date == 0:
                                    blank_date_found = True
            
                # import JH PARS
                df_algodocs_pars = df_pars[df_pars['planid'] == planid]
                df_algodocs_pars.reset_index(inplace=True)
            
                if len(df_algodocs_pars) == 0:
                    jh_pars_available = False
            
                else:
                    jh_pars_available = True
            
                # import EFAST data for fido bonds
                df_efast_plan = df_efast_sf[df_efast_sf['Form Identifier'] == planid]
                df_efast_plan.reset_index(inplace=True)
                efast_available = False
            
                # check if the plan is available on sf list
                if len(df_efast_plan) != 0:
                    efast_available = "sf_available"
            
                # if no, check out if it's available in the long filer extract
                elif len(df_efast_plan) == 0:
                    df_efast_plan = df_efast_5500[df_efast_5500['Form Identifier'] == planid]
                    df_efast_plan.reset_index(inplace=True)
                    
                    df_efast_name_plan = df_efast_name[df_efast_name['Form Identifier'] == planid]
                    df_efast_name_plan.reset_index(inplace=True)
            
                    if len(df_efast_plan) != 0:
                        efast_available = "5500_available"
            
                    else:
                        efast_available = False
            
                else:
                    efast_available = False
            
                # create dt objects for time comparisons
                project_period_start = datetime.strptime(period_start,'%m/%d/%Y %H:%M:%S %p')
                project_period_end = datetime.strptime(period_end,'%m/%d/%Y %H:%M:%S %p')
                date_plan_added = datetime.strptime(added_on,'%m/%d/%Y %H:%M:%S %p')
                date_effective_on = datetime.strptime(effective_on,'%m/%d/%Y %H:%M:%S %p')
            
                # determine if plan is a takeover or not
                takeover_plan = False
                if date_effective_on >= project_period_start and date_effective_on <= project_period_end:
                    if date_plan_added < project_period_start:
                        takeover_plan = True
            
                # reformat pp dates to comply with DGEM standards
                period_start = datetime.strftime(project_period_start,'%Y-%m-%d')
                period_end = datetime.strftime(project_period_end,'%Y-%m-%d')
            
                if terminated_on is not None:
                    # Tries to grab the "Termination Date" project field from the termination project to check if
                    # the plan is actually terminating or just leaving Nova
                    project_fields = pp.get_project_fields_by_planid(planid, filters="FieldName eq 'Termination Date'")
                    if len(project_fields) == 0:
                        terminated_on = None
                    else:
                        date_plan_terminated = datetime.strptime(terminated_on,'%m/%d/%Y %H:%M:%S %p')
            
            
                # isolate client information
                client_info = [client for client in all_clients if client['Id'] == client_id][0]
            
                # get client name
                client_name = client_info['CompanyName']['DisplayName']
                
                asc_extract_read_success = None
                
                # ASC information
                if asc_extract_available is True:
            
                    boy_participants = int(df_asc.at[0,12])
            
                    eoy_participants_w_acct = int(df_asc.at[0,18])
                    boy_active_participants = int(df_asc.at[0,13])
                    eoy_active_participants = int(df_asc.at[0,14])
                    term_unvested_participants = int(df_asc.at[0,19])
                    boy_participants_w_acct = int(df_asc.at[0,20])
            
                    eoy_participants = int(eoy_active_participants + df_asc.at[0,15] + df_asc.at[0,16] + df_asc.at[0,17])
            
                    eoy_assets = int(df_asc.at[2,4])
                    employer_contrib = int(df_asc.at[2,5])
                    part_contrib = int(df_asc.at[2,6])
                    other_contrib = int(df_asc.at[2,7])
                    other_income = int(df_asc.at[2,8])
                    benefits_paid = int(df_asc.at[2,9])
                    deemed_or_corrective_dist = int(df_asc.at[2,10])
                    salaries_fees_commissions = int(df_asc.at[2,11])
            
                    loan_amount = int(df_asc.at[2,12])
                    asc_extract_read_success = True
            
                else:
                    boy_participants = None
                    eoy_participants = None
                    eoy_participants_w_acct = None
                    boy_active_participants = None
                    eoy_active_participants = None
                    term_unvested_participants = None
                    boy_participants_w_acct = None
            
                    eoy_assets = None
                    employer_contrib = None
                    part_contrib = None
                    other_income = None
                    benefits_paid = None
                    deemed_or_corrective_dist = None
                    salaries_fees_commissions = None    
            
                    loan_amount = None
            
                    error = True
                    print(planid, "error at line x213")
                    asc_extract_read_success = False
            
                # Pull in JH information for fees
                if salaries_fees_commissions == 0 and jh_pars_available == True:
            
                    # pull fee total from algodocs pars dataframe
                    jh_par_fees = df_algodocs_pars.at[0,'fee_total']
            
                    # take absolute value of par fees, since they're expressed as negatives
                    salaries_fees_commissions = int(round(abs(jh_par_fees),0))
            
                    # subtract fees from "other income" (investment gains), since JH rolls the fees in there
                    other_income = other_income - int(round(jh_par_fees,0))
            
                # Pull in JH information for corrective distributions
                if deemed_or_corrective_dist == 0 and jh_pars_available == True:
                    # pull fee total from algodocs pars dataframe
                    jh_par_corrections = df_algodocs_pars.at[0,'corrections']
            
                    # take absolute value, since they're expressed as negatives
                    deemed_or_corrective_dist = int(round(abs(jh_par_corrections),0))
            
                    # subtract corrective dist from "other income" (investment gains), since JH rolls the fees in there
                    other_income = other_income - int(round(jh_par_corrections,0))
            
                # Pull in JH information for loans
                if loan_amount == 0 and jh_pars_available == True:
            
                    # pull loan total from algodocs pars dataframe
                    jh_loans = df_algodocs_pars.at[0,'loans']
                    loan_amount = int(round(jh_loans,0))
                    
                ## blackout date block
                blackout_period = False
            
                term_in_target_year = False
                start_in_target_year = False
            
                plan_info = pp.get_plan_by_planid(planid,expand='InvestmentProviderLinks.InvestmentProvider')
            
                investment_provider_blackout_candidates = [provider for provider in plan_info['InvestmentProviderLinks'] if provider['InvestmentProvider']['DisplayName'] not in exclude_rks]
            
                
                for provider in investment_provider_blackout_candidates:
                    
                    try:
                        provider_start_date = datetime.strptime(provider['EffectiveOn'],'%m/%d/%Y %H:%M:%S %p')
                    except:
                        provider_start_date = None
            
                    if provider_start_date is not None:
                        if project_period_start <= provider_start_date and  project_period_end>= provider_start_date:
                            start_in_target_year = True
            
                    try:
                        provider_end_date = datetime.strptime(provider['TerminatedOn'],'%m/%d/%Y %H:%M:%S %p')
                    except:
                        provider_end_date = None
            
                    if provider_end_date is not None:
                        if project_period_start <= provider_end_date and  project_period_end>= provider_end_date:
                            term_in_target_year = True        
            
                if term_in_target_year is True and start_in_target_year is True:
                    blackout_period = True
            
                ## end blackout date block
                
                ## John Hancock Check Block
                john_hancock_plan = False
                primary_investment_provider_jh = [provider for provider in plan_info['InvestmentProviderLinks'] if provider['IsPrimary'] is True and provider['InvestmentProvider']['DisplayName'] == "John Hancock"]
                if len(primary_investment_provider_jh) > 0:
                    john_hancock_plan = True
                
                # import algodocs info
                df_algodocs_info = df_algodocs[df_algodocs['plan_id'] == planid]
                df_algodocs_info.reset_index(inplace=True)
                
                rk_without_schedule_a = False
                missing_commission_info = False
                
                # if algodocs isn't available, we check to see if the RK doesn't have algodocs output
                if len(df_algodocs_info) == 0:
                    primary_investment_provider = [provider for provider in plan_info['InvestmentProviderLinks'] if provider['IsPrimary'] is True and provider['InvestmentProvider']['DisplayName'] not in algodocs_rks]
                    if len(primary_investment_provider) == 0:
                        algodocs_available = False
                        missing_commission_info = True
                    elif len(primary_investment_provider) > 0:
                        algodocs_available = False
                        rk_without_schedule_a = True
            
                else:
                    algodocs_available = True
                    
                # pull in algodocs information for schedule As
                if algodocs_available == True:
            
                    # pull fees from algodocs dataframe
                    schedulea = df_algodocs_info.at[0,'sum']
                    broker_fees = int(round(schedulea,0))
            
                if algodocs_available == False:
                    broker_fees = 0
            
                # First return
                first_year_return = False
                if added_on is not None:
                    if date_plan_added >= project_period_start and date_plan_added <= project_period_end:
                        first_year_return = True
            
                # final return
                final_return = False
                if terminated_on is not None:
                    if date_plan_terminated >= project_period_start and date_plan_terminated <= project_period_end:
                        final_return = True
            
                # amended filing
                amended_filing = False
                if "Amended" in proj_name:
                    amended_filing = True
            
                # short plan year
                short_plan_year = False
                if int(str(project_period_end - project_period_start).split(" ")[0]) < 360:
                    short_plan_year = True
            
                # effective date logic
                if relius_doc_available is True:
                    relius_effective_date_raw = df_doc.at[0,"InitialEffDate"]
                    relius_effective_date_raw = relius_effective_date_raw.replace('1st','01')
                    relius_effective_dt = datetime.strptime(relius_effective_date_raw,'%B %d, %Y')
                    relius_effective_date = datetime.strftime(relius_effective_dt,'%Y-%m-%d')
                    plan_effective_date = relius_effective_date
            
                else:
                    pp_effective_dt = datetime.strptime(added_on,'%m/%d/%Y %H:%M:%S %p')
                    pp_effective_date = datetime.strftime(pp_effective_dt,'%Y-%m-%d')
                    plan_effective_date = pp_effective_date
            
                # Get address information
                # Get all addresses for a plan, filter anything but Physical or Mailing, prioritize Mailing
            
                addresses = client_info['Addresses']
            
                addresses = [address for address in addresses if (address['AddressType']['DisplayName'] == "Physical Address" or address['AddressType']['DisplayName'] == "Mailing Address" or address['AddressType']['DisplayName'] == "Billing Address")]
            
                target_address = None
            
                address_available = False
                
                for address_all in addresses:
                    
                    # check for PO boxes and skip, since they aren't acceptable for 5500
                    if target_address is not None:
                        po_check = target_address['Address']['Address1']
                        if "PO Box" or "P.O. Box" in po_check:
                            continue
                    
                    if address_all['AddressType']['DisplayName'] == "Mailing Address":
                        target_address = address_all
                        address_available = True
                        break
            
                    elif address_all['AddressType']['DisplayName'] == "Physical Address":
                        target_address = address_all
                        address_available = True
                        continue
            
                    elif address_all['AddressType']['DisplayName'] == "Billing Address":
                        target_address = address_all
                        address_available = True
                        continue
            
                if target_address is not None:
                    address1 = target_address['Address']['Address1']
                    address2 = target_address['Address']['Address2']
                    if address2 is None:
                        address2 = ""
                    city = target_address['Address']['City']
                    state = target_address['Address']['State']
                    zipcode = target_address['Address']['Zip']
            
                    address = f"{address1} {address2}".strip()
            
                else:
                    if relius_doc_available is True:
                        address = df_doc.at[0,"EmployerStreet"]
                        address1 = address
                        address2 = ""
                        city = df_doc.at[0,"EmployerCity"]
                        state = df_doc.at[0,"EmployerState"]
                        zipcode = df_doc.at[0,"EmployerZip"]
                        address_available = True
            
                    else:
                        address = None
                        address1 = None
                        address2 = None
                        city = None
                        state = None
                        zipcode = None
                        address_available = False  
                        
                if address1 is not None:
                    if len(address1) > 35 and address1 is not None:
                        if ", " in address1:
                            address1, address2 = split_address(address1, address2, ", ")
            
                        elif " - " in address1:
                            address1, address2 = split_address(address1, address2, " - ")
            
                        elif " SUITE" in address1:
                            shortened_address_elements = address1.rsplit(' SUITE', 1)
                            address1 = shortened_address_elements[0]
                            address2 = 'SUITE ' + shortened_address_elements[1] + " " + address2
            
                        elif " STE" in address1:
                            shortened_address_elements = address1.rsplit(' STE', 1)
                            address1 = shortened_address_elements[0]
                            address2 = 'STE ' + shortened_address_elements[1] + " " + address2
            
                        else:
                            while len(address1) > 35:
                                shortened_address_elements = address1.rsplit(' ', 1)
                                address1 = shortened_address_elements[0]
                                address2 = shortened_address_elements[1] + " " + address2
            
                if address_available == False:
                    error = True
                    print(planid, "error at line x439")
            
                # End of address block
            
                # End of address block
            
                # Get EIN
                ein_available = False
                plan_cycle = [plan_cycle for plan_cycle in client_info['EmployerDatas'] if plan_cycle['PeriodStart'] == period_start]
                if len(plan_cycle) > 0:
                    ein = plan_cycle[0]['EIN']
                    ein_available = True
            
                else:
                    if relius_doc_available is True:
                        ein_relius = df_doc.at[0,"EmployerEIN"]
                        ein = ein_relius.replace("-", "").strip()
                        ein_available = True
            
                    else:
                        ein = None
                        ein_available = False
            
                if ein_available is False:
                    error = True
                    print(planid, "error at line x463")
            
                # end EIN block
            
                # Get phone number: primary if available, "Phone Number" if not available
                phone_available = False
            
                phone_number = [phone['PhoneNumber']['Number'] for phone in client_info['Numbers'] if phone['IsPrimary'] is True]
            
                if len(phone_number) == 0:
                    phone_number = [phone['PhoneNumber']['Number'] for phone in client_info['Numbers'] if phone['PhoneNumberType']['DisplayName'] == "Phone Number"]
            
                if len(phone_number) == 0:
                    if relius_doc_available is True:
                        phone_number_relius = df_doc.at[0,"EmployerPhone"]
                        phone_number = [phone_number_relius.replace("(","").replace(") ","").replace("-","")]
            
                if len(phone_number) != 0:
                    phone_number = phone_number[0]
                    phone_available = True
            
                else:
                    phone_number = None
                    phone_available = False
            
                if phone_available is False:
                    error = True
                    print(planid, "error at line x490")
            
                ## end phone block
            
                ## begin business code block
                
                # loop through employer data info old to new, capturing NAIC codes
                business_code_available = False
                business_code = None
                for employer_data in client_info["EmployerDatas"]:
                    if employer_data["NAICCode"] is not None:
                        business_code = employer_data["NAICCode"]
                        business_code_available = True
                    
                if business_code_available == False:
                    error=True
                    print(planid, "error at line x506")
            
                ## end business code block
            
                ## begin late contrib block
                late_contrib_available = False
                late_contrib = False
                late_contrib_amount = None
            
                # variable to ensure late contributions are advanced to specialist when no amount is available
                late_contrib_review = False
            
                # check for extant 5500 confirmation project
                # these are launched for plans that may have had late contributions
                extant_confirmation_proj = False
            
                confirmation_projects = pp.get_projects_by_planid(planid, filters=f"Name eq 'Form 5500 Confirmations' and PeriodEnd eq '{period_end}'",expand="TaskGroups.Tasks")
            
                confirmed_late_deposit = None
                confirmed_late_deposit_amount = None
            
                if len(confirmation_projects) > 0:
                    extant_confirmation_proj = True
                    confirmation_project = confirmation_projects[0]
            
                    completed_confirm_project = False
            
                    if confirmation_project['CompletedOn'] is not None:
                        completed_confirm_project = True
            
                    for taskgroup in confirmation_project["TaskGroups"]:
                        for task in taskgroup["Tasks"]:
                            if task["TaskName"] == "Missing 5500 Data":
                                late_contrib_items = pp.get_task_items_by_taskid(task["Id"])
            
                    for item in late_contrib_items:
                        if item["ShortName"] == 'Late Deposit Confirmation 1':
                            confirmed_late_deposit = item['Value']
                        if item["ShortName"] == 'Late Deposit Amount 1':
                            confirmed_late_deposit_amount = item['Value']
            
                # We can say there is no late contrib needed if account manager has completed
                # the confirmations project without marking one as needed
                if extant_confirmation_proj is True:
                    if confirmed_late_deposit != "Yes" and completed_confirm_project is True:
                        late_contrib_available = True
                        
                    if confirmed_late_deposit == "No":
                        late_contrib_available = True
                        
                    if confirmed_late_deposit == "Yes" and confirmed_late_deposit_amount is not None:
                        late_contrib = True
                        late_contrib_amount = float(confirmed_late_deposit_amount)
                        late_contrib_available = True
                    
                    # plan said "yes" for late contrib, and confirm project is still open with no amounts (goes to review)
                    elif confirmed_late_deposit == "Yes" and confirmed_late_deposit_amount == None and completed_confirm_project is False:
                        late_contrib = True
                        late_contrib_available = True
                        late_contrib_review = True
                    # plan said "yes" for late contrib, but confirm project is closed with no amounts. per template we report nothing in this case   
                    elif confirmed_late_deposit == "Yes" and confirmed_late_deposit_amount == None and completed_confirm_project is True:
                        late_contrib_available = True
                        
                # if the above project hasn't been launched, check for correction project
                if extant_confirmation_proj == False:
                    late_contrib_project_available = False
                    late_contrib_project_for_year = False
            
                    # pull late contribution projects, if there are any
                    late_contrib_project_name = 'SPT - Late Deposit of Deferrals, Loan Repayments, or Matching Contributions'
                    late_contrib_projects = pp.get_projects_by_planid(planid, filters=f"Name eq '{late_contrib_project_name}'",expand="TaskGroups.Tasks")
            
                    # limit it to special projects launched in the period in question or after
                    late_contrib_projects = [project for project in late_contrib_projects if project['AddedOn'].split("/")[2].split(" ")[0] == period_year or project['AddedOn'].split("/")[2].split(" ")[0] == year_after_period_year]
            
                    if len(late_contrib_projects) > 0:
                        # we want to be able to sum up values in multiple projects if they all apply to this period
                        late_contrib_sum = 0
                        late_contrib_amt = None
                        
                        for project in late_contrib_projects:
                            for taskgroup in project['TaskGroups']:
                                for task in taskgroup['Tasks']:
                                    if taskgroup['Name'] == 'Lost Earnings Calculation' and task['TaskName'] == 'Review':
                                        task_items = pp.get_task_items_by_taskid(task['Id'])
                            # get year values. they aren't project fields so I need to just pull all dates
                            task_item_values = [item['Value'] for item in task_items if item['Value'] is not None]
                            years_affected = [item.split("/")[2] for item in task_item_values if "/" in item and item != 'N/A']
            
                            # if the project pertains to the year in question, we pull the amount of the deposits and add it to the 
                            # sum variable above. Then we mark that there is a relevant project of this type
                            if period_year in years_affected:
                                late_contrib_project_available = True
                                late_contrib_project_for_year = True
                                late_contrib_amts = [item['Value'] for item in task_items if item['ShortName'] == '5500 - Compliance Questions']
                                if len(late_contrib_amts) > 0 :
                                    late_contrib_amt = late_contrib_amts[0]
                                if late_contrib_amt is not None:
                                    late_contrib_sum+=float(late_contrib_amt)
                                    late_contrib_available = True
            
                        # this should trigger if there are no values available in the SPT project, and will get the task moved to specialist for review      
                        if late_contrib_project_available == True and late_contrib_available == False:
                            late_contrib_available = True
                            late_contrib_review = True
                            
                        # this should trigger if there are SPT project, but none for the target period      
                        if late_contrib_project_for_year == False and late_contrib_available == False:
                            late_contrib_available = True
            
                    # this should trigger when there are no confirmation projects and no late contrib corrections 
                    # it will prevent an error and keep the late contrib fields blank
                    else:
                        late_contrib_available = True
            
                # only time this should trigger is if there is an incomplete confirmations project
                # or if there is an incomplete corrections project
                if late_contrib_available == False:
                    error=True
                ## end late contrib block
            
                ## begin SDBA block (2R char code)
            
                char_2r = False
            
                previous_year_projects = pp.get_projects_by_planid(planid, filters=f"ActiveOn gt '1/1/{previous_year}'")
            
                target_projects = [project for project in previous_year_projects if 'Annual Valuation' in project['Name'] and project['PeriodEnd'] == period_end]
            
                for project in target_projects:
                    if project['Name'].startswith("Annual Valuation"):
                        char_2r = True
            
                ## end sdba block
            
                ## begin Fidelity Bond block
            
                fidelity_bond_available = False
            
                has_fidelity_bond = False
            
                fidelity_bond_amount = None
                fidelity_bond_values = [] #initialize list of bond values from different sources
                ## fidelity bond information from questionnaire
                    # fidelity_amount_questionnaire
                    # fidelity_auto_questionnaire
                
            #     ## fidelity bond information from spreadsheet
            #     # see if Nova purchased a fidelity bond for the plan
            #     df_fidelity_plan = df_fidelity_bond[df_fidelity_bond['Client ID'] == planid]
                
                
            #     # if so, grab that bond amount
            #     if len(df_fidelity_plan) > 0:
            #         nova_fidelity_bond = max(df_fidelity_plan['Bond amount'].tolist())
            #         fidelity_bond_values.append(nova_fidelity_bond)
                
            #     # check which of the purchased or reported bonds is bigger, use that value
            #     if str(fidelity_amount_questionnaire) != 'nan' and str(fidelity_amount_questionnaire) != 'None':
            #         try:
            #             fidelity_bond_values.append(float(fidelity_amount_questionnaire))
            #         except:
            #             try:
            #                 fidelity_bond_values.append(float(fidelity_amount_questionnaire.replace(",","")))
            #             except:
            #                 pass #value is a string, useless to us for now
                
            #     if len(fidelity_bond_values) > 0:
            #         if max(fidelity_bond_values) > 0:
            #             fidelity_bond_amount = max(fidelity_bond_values)
            #             fidelity_bond_available = True
            #             has_fidelity_bond = True
            
            #         elif max(fidelity_bond_values) == 0:
            #             fidelity_bond_available = True
            #             has_fidelity_bond = False
            #             fidelity_bond_amount = 0    
            
            #     # Modify fidelity bond amount if it's an obvious auto-escalating bond
            #     # OR if the client indicated that it's an auto-escalating bond
                
            #     #     # check if fidelity bond amount is a multiple of 1000. if it is, do nothing
            #     #     if fidelity_bond_amount % 1000 == 0:
            #     #         pass
                
            #     if fidelity_bond_amount is None:
            #         fidelity_bond_amount = 0
                
            #     # else if it's not an even multiple or if they indicated auto-escalate and we have prior year data, calculate the amount
            #     if (fidelity_bond_amount % 1000 != 0 or fidelity_auto_questionnaire == 'Complete') and efast_available is not False:
            #         if efast_available == "sf_available":
            #             eoy_amount_from_efast = int(df_efast_plan.at[0,"SF_NET_ASSETS_EOY_AMT"]) # get EOY net assets as integer (sf)
            
            #         if efast_available == "5500_available":
            #             eoy_amount_from_efast = int(df_efast_plan.at[0,'TOT_ASSETS_EOY_AMT']) # get EOY net assets as integer (5500)
            
            #         # formula to calculate bond amount: divide ending balance by 10, take minimum of that or 500,000,
            #         # take maximum of that or 20,000, round to the nearest integer, store as integer instead of float
            #         fidelity_bond_amount = int(round(max(20000,min(500000,eoy_amount_from_efast/10)),0))
            #         has_fidelity_bond = True
            #         fidelity_bond_available = True
            #         print("recalculated fidelity bond amount:", fidelity_bond_amount)
                                            
            #     if fidelity_bond_available == False:
            #         error=True
            #         print(planid, "error at line x687")
                                             
                ## end Fidelity Bond block
            
                ## check for prior year EIN and plan name
             
                ein_from_efast = None
                ein_mismatch = False
                name_from_efast = None
                ps_from_efast = None
                
                
                if efast_available == "sf_available":
                    ein_from_efast = int(df_efast_plan.at[0,"SF_SPONS_EIN"])
                    name_from_efast = df_efast_plan.at[0,"SF_PLAN_NAME"].upper()
                    ps_from_efast = df_efast_plan.at[0, "SF_SPONSOR_NAME"].upper()
                    
                if efast_available == "5500_available":
                    ein_from_efast = int(df_efast_plan.at[0,"SCH_H_EIN"])
                    name_from_efast = df_efast_name_plan.at[0,"PLAN_NAME"].upper()
                    ps_from_efast = df_efast_name_plan.at[0,"SPONSOR_DFE_NAME"]
                    
                if ein_from_efast is not None:
                    ein_from_efast = str(ein_from_efast).zfill(9)
                    if ein_from_efast != ein:
                        ein_mismatch = True ## end prior year EIN check
                  
                  
                # If the confidence level is low because the names aren't similar...
                if not ein_mismatch and Measure_Name_Similarity(plan_name, name_from_efast) < 90:
                    ein_mismatch = True  
                if not ein_mismatch and Measure_Name_Similarity(client_name, ps_from_efast) < 90:
                    ein_mismatch = True 
                    
                ## end prior year EIN check
                
                ## begin first irs compliance block
                
                # defaults are False
                plansatisfytests = False
                plan401kdesignbased = False
                plan401kprioryear = False
                plan401kcurrentyear = False
                plan401kNA = False  
                                                              
                # first, if the plan is a MEP, 14a and 14b are left blank, so we only check these for non-MEPs
                if mep_status == False:                       
            
                    # answer to aggregation question is Yes if client answered Yes in Questionnaire
                    if multi_plan_questionnaire == "Yes":
                        plansatisfytests = True
                    
                    # also yes if combo plan
                    if combo_plan == "Yes, Other TPA" or combo_plan == "Yes - DB/DC Combo Plan":
                        plansatisfytests = True
                    
                    # also yes if 410b pdf read earlier says so
                    if other_plan_410b is True:
                        plansatisfytests = True
                                     
                ## end first irs compliance block
                                             
                ### These next parts extract information from the document extract to populate
                if relius_doc_available is True:
            
                    # 2A: new comp
                    char_2a = False
            
                    if df_doc.at[0,"ERDiscrPSNonSafeHarbAlloc"] == "x":
                        char_2a = True
            
                    # 2C: MPP
                    char_2c = False
            
                    if df_doc.at[0,"ProdCyc3MPPPlan"] == "x":
                        char_2c = True       
            
                    # 2E: Profit Sharing
                    char_2e = False
            
                    if df_doc.at[0,"ContrTypeERNonElect"] == "x":
                        char_2e = True   
            
                    # 2F: Participant-directed investment
                    char_2f = False
            
                    if df_doc.at[0,"DirInvAcc"] == "x":
                        char_2f = True       
            
                    # 2G: Total participant-directed investment
                    char_2g = False
            
                    if df_doc.at[0,"DirInvAccAll"] == "x":
                        char_2g = True    
            
                    # 2H: Partial participant-directed investment
                    char_2h = False
            
                    partial_participant_direction_relius_list = ['DirInvAccSpecifyAccounts',
                                                                 'DirInvAccElectDef',
                                                                 'DirInvAccRothElectDef',
                                                                 'DirInvAccQMCERMC',
                                                                 'DirInvAccNE',
                                                                 'DirInvAccQNEC',
                                                                 'DirInvAccRoll',
                                                                 'DirInvAccTransfer',
                                                                 'DirInvAccVol',
                                                                 'DirInvAccOther']
            
                    for provision in partial_participant_direction_relius_list:
                        if df_doc.at[0,f"{provision}"] == "x":
                            char_2h = True
            
                    # 2J: Allows deferrals
                    char_2j = False
            
                    if df_doc.at[0,"ContrTypeElectDef"] == "x":
                        char_2j = True  
            
                    # 2K: Allows match
                    char_2k = False
            
                    if df_doc.at[0,"ContrTypeSafeHarbor"] == "x" or df_doc.at[0,"ContrTypeERMatchContr"] == "x":
                        char_2k = True  
                    
                    # 2L and 2M: for 403b plans
                    char_2l = False
                    
                    char_2m = False
                    
                    if plan_type == 'ERISA 403(b)':
                        char_2l = True
                        char_2m = True
                    
                    # 2S: Auto-enrollment
                    char_2s = False
            
                    if df_doc.at[0,"AutoDeferProvisionYes"] == "x":
                        char_2s = True
            
                    # 3B: Self-employed
                    char_3b = False
            
                    if df_doc.at[0,"LLCPartnerSoleProp"] == "x" or df_doc.at[0,"SoleProp"] == "x" or df_doc.at[0,"Partnership"] == "x":
                        char_3b = True
            
                    # 3D: Pre-approved doc (all plans with the relius extract have this), hardcoded
                    char_3d = True        
            
                    # 3H: Controlled group
                    char_3h = False
            
                    if df_doc.at[0,"YAffilEmployer"] == "x" or df_doc.at[0,"YControlGrp"] == "x":
                        char_3h = True
            
                    # 2T: Default investment, hardcoded
                    char_2t = True    
            
                    # 2U_MEP_ASSN, 2V_PEO_MEP, 2X_MEP_OTHER, no directive for these
            
                    # generate characteristic_code_string:
                    cc_string = ""
            
                    if char_2a is True:
                        cc_string = cc_string+",2A"
                    if char_2c is True:
                        cc_string = cc_string+",2C"
                    if char_2e is True:
                        cc_string = cc_string+",2E"
                    if char_2f is True:
                        cc_string = cc_string+",2F"
                    if char_2g is True:
                        cc_string = cc_string+",2G"
                    if char_2h is True:
                        cc_string = cc_string+",2H"
                    if char_2j is True:
                        cc_string = cc_string+",2J"
                    if char_2k is True:
                        cc_string = cc_string+",2K"
                    if char_2l is True:
                        cc_string = cc_string+",2L"
                    if char_2m is True:
                        cc_string = cc_string+",2M"
                    if char_2s is True:
                        cc_string = cc_string+",2S"
                    if char_2t is True:
                        cc_string = cc_string+",2T"
                    if char_2r is True:
                        cc_string = cc_string+",2R"
                    if char_3b is True:
                        cc_string = cc_string+",3B"
                    if char_3d is True:
                        cc_string = cc_string+",3D"            
                    if char_3h is True:
                        cc_string = cc_string+",3H"
            
                    # remove leading comma
                    cc_string = cc_string[1:]
                    
                    ## begin second irs compliance block
                    # these depend on relius info
                    
                    # first we skip all this if plan is a MEP:
                    if mep_status == False:   
                        # design based safe harbor is "yes" if marked as such in plan doc
                        if df_doc.at[0,"ContrTypeSafeHarbor"] == "x":                       
                            plan401kdesignbased = True
            
                        # prior year ADP testing check
                        if df_doc.at[0,"NHCEPriorYR"] == "x":                                                      
                            plan401kprioryear = True
            
                        # current year ADP testing check (non-SH plans only)
                        if df_doc.at[0,"NHCEPriorYR"] != "x" and df_doc.at[0,"ContrTypeSafeHarbor"] != "x":                                                                                      
                            plan401kcurrentyear = True
                                             
                        # N/A response (we can't pull ADP info), so just pulling no deferrals
                        if df_doc.at[0,"ContrTypeElectDef"] != "x":              
                            plan401kNA = True  
              
                    ## end first irs compliance block      
            
                ## collate information above into new line to put into a dataframe:
                pp_output = [planid,
                period_start,
                period_end,
                mep_status,
                added_on,
                effective_on,
                terminated_on,
                irs_number,
                taskid,
                projid,
                task_name,
                plan_name,
                client_id,
                proj_name,
                plan_status,
                plan_category,
                plan_type,
                form5500,
                combo_plan,
                plan_end,
                plan_group,
                error,
                client_name,
                boy_participants,
                eoy_participants,
                eoy_participants_w_acct,
                boy_active_participants,
                eoy_active_participants,
                term_unvested_participants,
                eoy_assets,
                employer_contrib,
                part_contrib,
                other_income,
                benefits_paid,
                deemed_or_corrective_dist,
                salaries_fees_commissions,
                loan_amount,
                first_year_return,
                final_return,
                amended_filing,
                short_plan_year,
                plan_effective_date,
                address_available,
                address,
                city,
                state,
                zipcode,
                ein_available,
                ein,
                phone_available,
                phone_number,
                business_code_available,
                business_code,
                late_contrib_available,
                late_contrib,
                late_contrib_amount,
                char_2r,
                fidelity_bond_available,
                has_fidelity_bond,
                fidelity_bond_amount,
                blackout_period,
                char_2a,
                char_2c,
                char_2e,
                char_2f,
                char_2g,
                char_2h,
                char_2j,
                char_2k,
                char_2l,
                char_2m,
                char_2s,
                char_2t,
                char_3b,
                char_3d,
                char_3h,
                cc_string,
                relius_doc_available,
                asc_extract_available,
                algodocs_available,
                asc_extract_read_success,
                missing_commission_info,
                missing_irs_num,
                missing_naic_code,
                collectivelybargained,
                boy_participants_w_acct,
                plansatisfytests,
                plan401kdesignbased,
                plan401kprioryear,
                plan401kcurrentyear,
                plan401kNA,
                opinion_letter_date,
                opinion_letter_serial]
                    
                pp_update_data.append(pp_output)
                print(error)
                if error == False:
                    ## Dump information into DGEM text import file
                    # dgem_dictionary = dict.fromkeys(template_columns,"")
                    
                    print(planid, "printing to output")
            
                    plan_year = period_end.split("-")[0]
            
                    mep = 1
            
                    if mep_status is True:
                        mep = 2
            
                    initial_filing = convert_TF_to_digit(first_year_return)
                    amended = convert_TF_to_digit(amended_filing)
                    final = convert_TF_to_digit(final_return)
                    short = convert_TF_to_digit(short_plan_year)
                    late = convert_TF_to_one_two(late_contrib)
                    fidelity = convert_TF_to_one_two(has_fidelity_bond)
            
                    if broker_fees > 0:
                        fee_yn = 1
                    else:
                        fee_yn = 2
                        broker_fees = ""
            
                    blackout = convert_TF_to_one_two(blackout_period)
            
                    if blackout == 1:
                        blackout_notice = 1
                    else:
                        blackout_notice = ""
            
                    loan_yn = 2
            
                    if loan_amount > 0:
                        loan_yn = 1
                        loan_amt_formatted = loan_amount
            
                    else:
                        loan_amt_formatted = ""
            
                    if " DBA " in client_name:
                        ps_name = client_name.split(" DBA ")[0]
                        dba = client_name.split(" DBA ")[1]
            
                    elif " dba " in client_name:
                        ps_name = client_name.split(" dba ")[0]
                        dba = client_name.split(" dba ")[1]
            
                    else: 
                        ps_name = client_name
                        dba = ""
                        
                    while len(ps_name) > 70:
                        ps_name = ps_name[:ps_name.rfind(', ')]
                                             
                    union_yn = convert_TF_to_digit(collectivelybargained)
                    plansatisfytests_yn = convert_TF_to_one_two(plansatisfytests)
                    plan401kdesignbased_yn = convert_TF_to_digit(plan401kdesignbased)
                    plan401kprioryear_yn = convert_TF_to_digit(plan401kprioryear)
                    plan401kcurrentyear_yn = convert_TF_to_digit(plan401kcurrentyear)
                    plan401kNA_yn = convert_TF_to_digit(plan401kNA)
                                             
                    opinion_letter_dt = datetime.strptime(opinion_letter_date,'%m/%d/%Y')
                    opinion_letter_date_dgem = datetime.strftime(project_period_start,'%Y-%m-%d')
                                             
            #         dgem_dictionary = {'SponsorEIN': ein,
            #          'SponsPlanNum': f"{irs_number}",
            #          'PlanYear': plan_year,
            #          'PlanYearBeginDate': period_start_dgem,
            #          'PlanYearEndDate': period_end_dgem,
            #          'TypePlanEntityCd': mep,
            #          'InitialFilingInd': initial_filing,
            #          'AmendedInd': amended,
            #          'FinalFilingInd': final,
            #          'ShortPlanYrInd': short,
            #          'Form5558ApplicationFiledInd': '',
            #          'ExtAutomaticInd': '',
            #          'DFVCProgramInd': '',
            #          'ExtSpecialInd': '',
            #          'ExtSpecialText': '',
            #          'AdoptedPlanSECUREAct': '',
            #          'PlanName': plan_name.upper().strip(),
            #          'SponsorPlanNum': irs_number,
            #          'PlanEffDate': plan_effective_date,
            #          'SponsorName': ps_name.upper().strip(),
            #          'SponsorDbaName': dba.upper().strip(),
            #          'SponsorCareOfName': '',
            #          'SponsorUSAddressAddressLine1': address1.upper().strip(),
            #          'SponsorUSAddressAddressLine2': address2.upper().strip(),
            #          'SponsorUSAddressCity': city.upper().strip(),
            #          'SponsorUSAddressState': state.upper().strip(),
            #          'SponsorUSAddressZipCode': zipcode.strip(),
            #          'SponsorForeignAddressAddressLine1': '',
            #          'SponsorForeignAddressAddressLine2': '',
            #          'SponsorForeignAddressCity': '',
            #          'SponsorForeignAddressProvinceOrState': '',
            #          'SponsorForeignAddressCountry': '',
            #          'SponsorForeignAddressPostalCode': '',
            #          'SponsorUSLocationAddressAddressLine1': '',
            #          'SponsorUSLocationAddressAddressLine2': '',
            #          'SponsorUSLocationAddressCity': '',
            #          'SponsorUSLocationAddressState': '',
            #          'SponsorUSLocationAddressZipCode': '',
            #          'SponsorForeignLocationAddressAddressLine1': '',
            #          'SponsorForeignLocationAddressAddressLine2': '',
            #          'SponsorForeignLocationAddressCity': '',
            #          'SponsorForeignLocationAddressProvinceOrState': '',
            #          'SponsorForeignLocationAddressCountry': '',
            #          'SponsorForeignLocationAddressPostalCode': '',
            #          'SponsorPhoneNum': phone_number,
            #          'SponsorForeignPhoneNum': '',
            #          'BusinessCode': business_code,
            #          'AdminNameSameAsSponsorInd': 1,
            #          'AdminName': '',
            #          'AdminCareOfName': '',
            #          'AdminUSAddressAddressLine1': '',
            #          'AdminUSAddressAddressLine2': '',
            #          'AdminUSAddressCity': '',
            #          'AdminUSAddressState': '',
            #          'AdminUSAddressZipCode': '',
            #          'AdminForeignAddressAddressLine1': '',
            #          'AdminForeignAddressAddressLine2': '',
            #          'AdminForeignAddressCity': '',
            #          'AdminForeignAddressProvinceOrState': '',
            #          'AdminForeignAddressCountry': '',
            #          'AdminForeignAddressPostalCode': '',
            #          'AdminEIN': '',
            #          'AdminPhoneNum': '',
            #          'AdminPhoneNumForeignPhoneNum': '',
            #          'LastRptPlanName': '',
            #          'LastRptSponsName': '',
            #          'LastRptSponsEIN': '',
            #          'LastRptPlanNum': '',
            #          'TotPartcpBoyCnt': boy_participants,
            #          'TotActRtdSepBenefCnt': eoy_participants,
            #          'PartcpAccountBalCnt': eoy_participants_w_acct,
            #          'TotActPartcpBoyCnt': boy_active_participants,
            #          'TotActPartcpEoyCnt': eoy_active_participants,
            #          'SepPartcpPartlVstdCnt': term_unvested_participants,
            #          'EligibleAssetsInd': 1,
            #          'IQPAWaiverInd': 1,
            #          'CoveredPBGCInsuranceInd': '',
            #          'PremiumFilingConfirmationNum': '',
            #          'TotAssetsBoyAmt': '',
            #          'TotLiabilitiesBoyAmt': '',
            #          'NetAssetsBoyAmt': '',
            #          'TotAssetsEoyAmt': eoy_assets,
            #          'TotLiabilitiesEoyAmt': '',
            #          'NetAssetsEoyAmt': '',
            #          'EmplrContribIncomeAmt': employer_contrib,
            #          'ParticipantContribIncomeAmt': part_contrib,
            #          'OthContribRcvdAmt': other_contrib,
            #          'OtherIncomeAmt': other_income,
            #          'TotIncomeAmt': '',
            #          'TotDistribBnftAmt': benefits_paid,
            #          'CorrectiveDeemedDistribAmt': deemed_or_corrective_dist,
            #          'AdminSrvcProvidersAmt': salaries_fees_commissions,
            #          'OthExpensesAmt': '',
            #          'TotExpensesAmt': '',
            #          'NetIncomeAmt': '',
            #          'TotPlanTransfersAmt': '',
            #          'TypePensionBnftCode': cc_string,
            #          'TypeWelfareBnftCode': '',
            #          'FailTransmitContribInd': late,
            #          'FailTransmitContribAmt': late_contrib_amount,
            #          'PartyInIntNotRptdInd': 2,
            #          'PartyInIntNotRptdAmt': '',
            #          'PlanInsFdltyBondInd': fidelity,
            #          'PlanInsFdltyBondAmt': fidelity_bond_amount,
            #          'LossDiscvDurYearInd': 2,
            #          'LossDiscvDurYearAmt': '',
            #          'BrokerFeesPaidInd': fee_yn,
            #          'BrokerFeesPaidAmt': broker_fees,
            #          'FailProvideBenefitDueInd': 2,
            #          'FailProvideBenefitDueAmt': '',
            #          'PartcpLoansInd': loan_yn,
            #          'PartcpLoansEoyAmt': loan_amt_formatted,
            #          'PlanBlackoutPeriodInd': blackout,
            #          'ComplyBlackoutNoticeInd': blackout_notice,
            #          'DbPlanFundingReqdInd': '',
            #          'UnpaidMinContribCurrYrTotAmt': '',
            #          'PBGCNotifiedCd': '',
            #          'PBGCNotifiedExplanationText': '',
            #          'DcPlanFundingReqdInd': '',
            #          'RulingLetterGrantDate': '',
            #          'Sec412ReqContribAmt': '',
            #          'EmplrContribPaidAmt': '',
            #          'FundingDeficiencyAmt': '',
            #          'FundingDeadlineInd': '',
            #          'ResTermPlanAdptInd': '',
            #          'ResTermPlanAdptAmt': '',
            #          'AllPlanAstDistribInd': '',
            #          'CollectivelyBargained': union_yn,
            #          'PartcpAccountBalCntBoy': int(boy_participants_w_acct),
            #          'PlanSatisfyTestsInd': plansatisfytests_yn,
            #          'Plan401kDesignBasedInd': plan401kdesignbased_yn,
            #          'Plan401kPriorYearADPTestInd': plan401kprioryear_yn,
            #          'Plan401kCurrentYearADPTestInd': plan401kcurrentyear_yn,
            #          'Plan401kNAInd': plan401kNA_yn,
            #          'OpinLtrDate': opinion_letter_date_dgem,
            #          'OpinSerialNum': opinion_letter_serial,
            #          'AdminSignature': '',                
            #          'TPA Plan ID': planid}   
            
            #         for i in df_dgem_instructions.index:
            #             tag_name = df_dgem_instructions.at[i,"Tag Name"]
            #             tag_regex = df_dgem_instructions.at[i,"Regular Expressions"]
            #             try:
            #                 max_length = int(df_dgem_instructions.at[i,"Max Length"])
            
            #             except:
            #                 max_length = int(df_dgem_instructions.at[i,"Max Length"].split(" ")[-1])
            
            #             # test regex string to see if it's valid. if no, continue
            #             test_value = dgem_dictionary.get(tag_name)
            
            #             if test_value != "":
            #                 if re.match(f'{tag_regex}', str(test_value)):
            #                     pass
            #                 else:
            #                     print("REGEX VIOLATION:", i, planid, f'{tag_regex}', str(test_value)) 
            
            #             if len(str(test_value)) > max_length:
            #                 print("max length violation:", i, planid, f'{max_length}', str(test_value)) 
            
            #         plan_dgem_output = list(dgem_dictionary.values())
            #         dgem_dataframe_values.append(plan_dgem_output)
            
                    ## Begin block to populate 5500 review tasks
            
                    # notes on each of these, while converting TF to YN
            
                    #1 
                    blank_date_yn = convert_TF_to_YN(blank_date_found) #True or False, if True, populate with "Yes"
            
                    #2
            #         df_validation_individual = df_validation[df_validation['TPA Plan ID'].astype(str).apply(lambda x: x.split('.')[0]) == planid].reset_index()
            #         try:
            #             valid_status = df_validation_individual.at[0,'Result']
                        
            #         except:
            #             valid_status = None
                    if planid in validated_plan_list:
                        valid_status = "Success"
                        valid_yn = "No"
                    else:
                        valid_status = "Fail"
                        valid_yn = "Yes"
                        
                    #3
                    takeover_yn = convert_TF_to_YN(takeover_plan) #True or False, if True, populate with "Yes"
            
                    #4
                    ein_mismatch_yn = convert_TF_to_YN(ein_mismatch)
            
                    #5
                    refunds_yn = convert_TF_to_YN(refunds)
            
                    #6
                    char_2r_yn = convert_TF_to_YN(char_2r)
            
                    #7
                    rk_without_schedule_a_yn = convert_TF_to_YN(missing_commission_info) 
                    #updated this to only mark it "yes" if the RK should have commissions
            
                    #8
                    blackout_period_yn = convert_TF_to_YN(blackout_period)
                    
                    #9 (not a task item, but something we need to put as a note and will prevent from advancing
                    if john_hancock_plan == True:
                        jh_pars_available_yn = convert_TF_to_YN(not jh_pars_available)
                        
                    else:
                        jh_pars_available_yn = "No"
                        
                    #10 (also not a task item, designed to deal with missing late contrib info)
                    if late_contrib_review == True:
                        late_contrib_review_yn = convert_TF_to_YN(late_contrib_review)
                        
                    else:
                        late_contrib_review_yn = "No"
            
            #         # get projects to get task for taskid
            #         project = pp.get_project_by_projectid(projid, expand="TaskGroups.Tasks")
            #         for taskgroup in project['TaskGroups']:
            #             for task in taskgroup['Tasks']:df_pars
            #                 if task['TaskName'] == "Specialist Review of Form 5500":
            #                     taskid = task['Id']
            #                     print(taskid)
                    
                    all_task_items = [blank_date_yn, valid_yn, refunds_yn, takeover_yn, 
                                      ein_mismatch_yn, char_2r_yn, blackout_period_yn,
                                     rk_without_schedule_a_yn, jh_pars_available_yn, late_contrib_review_yn]
                
                    # pull task items, then update them with new values
            #         if valid_yn is not "No":
                    task_items = pp.get_task_items_by_taskid(taskid)
                    for task_item in task_items:
                        if task_item['ShortName'] is None:
                            continue
            
                        if task_item['ShortName'] == '5500-SF Blank Dates':
                            if blank_date_yn == "Yes":
                                blank_date_yn = "Completed"
            
                            elif blank_date_yn == "No":
                                blank_date_yn = "N/A"
            
                            task_item.update({'Value':blank_date_yn})
            
                            try:
                                pp.put_taskitem(task_item)
            
                            except:
                                if blank_date_yn == "Completed":
                                    blank_date_yn = "Yes"
            
                                elif blank_date_yn == "N/A":
                                    blank_date_yn = "No"
                                task_item.update({'Value':blank_date_yn})
                                pp.put_taskitem(task_item)
            
                            print(task_item['ShortName'], "- populated!")
                        if task_item['ShortName'] == '5500-SF Validation Errors':
                            task_item.update({'Value':valid_yn})
                            pp.put_taskitem(task_item)   
                            print(task_item['ShortName'], "- populated!")
                        if task_item['ShortName'] == '5500-SF Corrective Distributions':
                            task_item.update({'Value':refunds_yn})
                            pp.put_taskitem(task_item)    
                            print(task_item['ShortName'], "- populated!")
                        if task_item['ShortName'] == '5500-SF Takeover Plan':
                            task_item.update({'Value':takeover_yn})
                            pp.put_taskitem(task_item)
                            print(task_item['ShortName'], "- populated!")
                        if task_item['ShortName'] == '5500-SF EIN or Name Change':
                            task_item.update({'Value':ein_mismatch_yn})
                            pp.put_taskitem(task_item)
                            print(task_item['ShortName'], "- populated!")
                        if task_item['ShortName'] == '5500-SF SDBA':
                            task_item.update({'Value':char_2r_yn})
                            pp.put_taskitem(task_item)
                            print(task_item['ShortName'], "- populated!")
                        if task_item['ShortName'] == '5500-SF Blackout':
                            task_item.update({'Value':blackout_period_yn})
                            pp.put_taskitem(task_item)
                            print(task_item['ShortName'], "- populated!")
                        if task_item['ShortName'] == '5500-SF Commissions':
                            task_item.update({'Value':rk_without_schedule_a_yn})
                            pp.put_taskitem(task_item)
                            print(task_item['ShortName'], "- populated!")
            
                    advance_task = False
                    if jh_pars_available_yn == "Yes":
                        
                        note_text_jh = """Fees, deemed loans, loans, and corrective distributions unable to be extracted since JH PAR extract missing. 
                        All of that if will need to be pulled from PAR PDF and netted out of appropiately."""
                        
                        payload_jh_par = {
                        "ProjectID": f'{projid}', 
                        "NoteText": note_text_jh,
                        "ShowOnPSL": False
                            }
            
                        pp.add_note(payload_jh_par)
                        
                    if late_contrib_review_yn == "Yes":
                        note_text_late_contrib = """Plan has late contributions missed or corrected in the plan year, 
                        however automation is unable to find the late contribution amount. Please refer to a completed SPT late
                        contribution project or the 5500 Confirmations project for this period for more information"""
                        
                        payload_late_contrib = {
                        "ProjectID": f'{projid}', 
                        "NoteText": note_text_late_contrib,
                        "ShowOnPSL": False
                            }
            
                        pp.add_note(payload_late_contrib)
                                    
                        
                    if "Yes" not in all_task_items and "Completed" not in all_task_items:
                        advance_task = True
            
                    if advance_task is True:
                        pp.override_task(taskid)
                        advance_task_counter += 1
                        print(b, planid, "task advanced")
            
                    else:
                        print(b, planid, "task not advanced")    
            
            
                b+=1
            
            
            # In[106]:
            
            
            advance_task_counter
            
            
            # In[107]:
            
            
            len(df_populate)
            
            
            # In[108]:
            
            
            sys.path.insert(0, "Y:\Automation\Team Scripts\Andrew Kim\my modules")
            
            
            from glob import glob
            
            from IPython.display import display
            
            pd.set_option('display.max_rows',None)
            pd.set_option('display.max_columns',None)
            
            # My custom functions
            
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
            
            
            # In[109]:
            
            
            final_stats_total_count = len(df_validation)
            final_stats_success_count = len(df_validation[df_validation['Result'] == 'Success'])
            final_stats_failed_count = len(df_validation[df_validation['Result'] != 'Success'])
            final_stats_success_rate = int((final_stats_success_count / final_stats_total_count) * 100)
            
            print(f"Total advanced projects: {advance_task_counter}")
            print(f"Total plans: {final_stats_total_count}")
            print(f"Successful plans: {final_stats_success_count}")
            print(f"Failed plans: {final_stats_failed_count}")
            print(f"Success rate: {final_stats_success_rate}%")
            
            
            # In[110]:
            
            
            df_validation[df_validation['Result'] != 'Success']
            
            
            # In[117]:
            
            
            INDEX_DF = df_validation
            
            INDEX_DF["Status"] = "" # This will show the final result of the note upload and/or why the upload failed.
            INDEX_DF["NOTEID"] = "" # In case you need to iterate over them and delete. 
            INDEX_DF["NOTEID"] = INDEX_DF["NOTEID"].astype(object)
            #INDEX_DF["TPA Plan ID"] = INDEX_DF["TPA Plan ID"].fillna(0).astype(int).astype(str)
            
            
            # Added by Andrew. Change #4 6/12/24
            # Fill in missing TPA plan id's. They dont seem to be provided if DGEM is a failure.
            # Also, they are returned as floats from dgem. Dont even want to know the ramifications of that throughout the script...
            INDEX_DF["TPA Plan ID"] = INDEX_DF.apply(lambda row : pp.get_plans(filters = f"contains(Name,'{row['Plan Name']}')")[0]['InternalPlanId'] if pd.isna(row['TPA Plan ID']) else row['TPA Plan ID'], axis = 1)
            INDEX_DF["TPA Plan ID"] = INDEX_DF["TPA Plan ID"].apply(lambda x : str(int(x)) if type(x) == float else x)
            
            
            INDEX_DF["RUN TIME"] = datetime.now().strftime('%m/%d/%y %H:%M:%S') 
            GROUPED_DF = INDEX_DF.groupby('TPA Plan ID')
            
            for name, group in GROUPED_DF:
                
                # Skip plans that sucessfully had notes added.
                # This is in case you need to run the loop manually several times to go over failed rows. 
                if len(group.loc[group['Status'] == 'note added']):
                    continue
                    
                # Only log groups with at least 1 error.
                if len(group.loc[group["Result"] == "Success"]) == len(group):
                    continue
                
                PLANID = name
                
                if INDEX_DF.loc[INDEX_DF["TPA Plan ID"] == PLANID, "NOTEID"].iloc[0] != '': 
                    continue
                
                PYE = group['planYearEndDate'].iloc[0].split(" ")[0]
                try:
                    SMALL_FILER_PROJECT = pp.get_projects_by_planid(planid=PLANID, filters=f"contains(Name, 'DC Annual Governmental Forms - Small Filer') and CompletedOn eq null and PeriodEnd eq '{PYE}'")
                    if SMALL_FILER_PROJECT:
                        SMALL_FILER_PROJECT = SMALL_FILER_PROJECT[-1]
                        print(PLANID)
                #         break
                
                except:
                    print('Cannot locate Small Filer Project')
                    INDEX_DF.loc[INDEX_DF["TPA Plan ID"] == group["TPA Plan ID"].iloc[0], "Status"] = 'Cannot locate Small Filer Project'
                    continue  
                    
                if not SMALL_FILER_PROJECT:
                    print('Cannot locate Small Filer Project')
                    INDEX_DF.loc[INDEX_DF["TPA Plan ID"] == group["TPA Plan ID"].iloc[0], "Status"] = 'Cannot locate Small Filer Project'
                    continue  
                
                # Find the Filing Status task that is active. 
                # Only upload notes for projects with this active. 
                
                FILING_STATUS_TASK = find_task(SMALL_FILER_PROJECT['Id'], "Filing Status", "Filing Status")
                if FILING_STATUS_TASK["DateCompleted"] == None:
                    print("Adding note...")
                    SMALL_FILER_PROJECT['PlanId'] = '' # Erase PlanId so the note is only saved in the project.
                    SMALL_FILER_PROJECT["ProjectId"] = SMALL_FILER_PROJECT["Id"]  # Note payload requires ID
                    
                    # some errors are massive and exceed the 2000 note character limit. Break it down into rows of threes.
                    if len(group) > 2: 
                        print("Big note. Breaking into rows of 2.")
                        for i in range(0, len(group), 2):
                            SMALL_FILER_PROJECT['NoteText'] = group[['EIN_x', 'Plan Number', 'Plan Name', 'Result', 'Severity', 'Error Code','Error Message']].iloc[i : i+2].to_html(index=False)
                            UPLOADED_NOTE = pp.add_note(SMALL_FILER_PROJECT)
                    else:
                        SMALL_FILER_PROJECT['NoteText'] = group[['EIN_x', 'Plan Number', 'Plan Name', 'Result', 'Severity', 'Error Code','Error Message']].to_html(index=False)
                        UPLOADED_NOTE = pp.add_note(SMALL_FILER_PROJECT)
                        
                    INDEX_DF.loc[INDEX_DF["TPA Plan ID"] == group["TPA Plan ID"].iloc[0], "Status"] = 'note added'
                    INDEX_DF.loc[INDEX_DF["TPA Plan ID"] == group["TPA Plan ID"].iloc[0], "NOTEID"] = UPLOADED_NOTE["Id"]
                else:
                    INDEX_DF.loc[INDEX_DF["TPA Plan ID"] == group["TPA Plan ID"].iloc[0], "Status"] = 'Filing Status task not active. Note cancelled.'
                    continue
                
                print('\n')
            
            
            dataframe_logger(INDEX_DF, "Y:\\ASC\\Exported Reports\\5500 Automation\\DGEM Validation Files\\20228.xlsx",5000)
            
            failures = INDEX_DF[(INDEX_DF["Status"] != "note added") & (INDEX_DF['Result'] != "Success")]
            if len(failures):
                failures.to_excel(f"Y:\\ASC\\Exported Reports\\5500 Automation\\DGEM Validation Files\\{today}_FAILURES.xlsx", index = False)
            
            
            # In[259]:
            
            
            browser.quit()
            
            
            # In[260]:
            
            
            df_validation
            
            
            # In[261]:
            
            
            failures
            
            
            # In[262]:
            
            
            # # delete old notes
            
            # for i in df_populate.index[:]:
                
            #     planid = df_populate.at[i,'planid']
            #     projid = df_populate.at[i,'projid']
                
            #     notes = pp.get_notes_by_projid(projid)
                
            #     for note in notes:
            #         note_date = datetime.strptime(note['DateAdded'],'%m/%d/%Y %H:%M:%S %p')
            #         target_date = datetime.strptime('5/24/2024 12:00:00 AM','%m/%d/%Y %H:%M:%S %p')
            #         if note_date < target_date:
            #             pp.delete_note(note['Id'])
            
            
            # In[ ]:
            
            
            
            
            
            # In[ ]:
            
            
            
            
            
            # In[ ]:
            
            
            
            
            
            # In[ ]:
            
            
            
            
            
            # In[ ]:
            
            
            
            
            
            # In[ ]:
            
            
            
            
            
            # In[ ]:
            
            
            
            
            
            # In[263]:
            
            
            print('Y:\\ASC\\Exported Reports\\5500 Automation\\DGEM Validation Files')
            
            
            # In[264]:
            
            
            print('Y:\\ASC\\Exported Reports\\5500 Automation\\DGEM Validation Files\\20228.xlsx')
            
            
            # In[ ]:
            
            
            
            
            
            # In[ ]:
            
            
            
            
            
            # In[ ]:
            
            
            
            
            
            # In[118]:
            
            
            df_pp_output
            
            
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

            