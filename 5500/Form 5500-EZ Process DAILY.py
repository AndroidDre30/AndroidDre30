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
            

            
            # In[62]:
            
            
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
            from dateutil.relativedelta import relativedelta
            
            sys.path.insert(0, r'C:\Users\Public\WPy64-39100\notebooks\Lam Hoang')
            
            import lam
            
            import xlwt
            
            import pandas as pd
            import numpy as np
            import pensionpro_nova_jb as pp
            
            from tqdm import tqdm
            from pathlib import Path
            import smtplib
            
            
            # In[63]:
            
            
            now = datetime.now()
            previous_year = str(int(now.strftime("%Y"))-1)
            
            today = now.strftime('%Y-%m-%d_%H.%M.%S')
            
            
            # In[64]:
            
            
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
                if x is False:
                    y = "No"
                    
                elif x is True:
                    y = "Yes"
                    
                return y
            
            def split_address(address1, address2, delimiter):
                shortened_address_elements = address1.rsplit(f'{delimiter}', 1)
                address1 = shortened_address_elements[0]
                address2 = shortened_address_elements[1] + " " + address2
                return address1.strip(), address2.strip()
            
            
            # In[65]:
            
            
            # initiate logging stuff
            
            pp_logging_columns = ['planid',
                'period_start',
                'period_end',
                'added_on',
                'effective_on',
                'terminated_on',
                'irs_number',
                'taskid',
                'projid',
                'task_name',
                'plan_name',
                'client_id',
                'proj_name',
                'plan_status',
                'plan_category',
                'plan_type',
                'form5500',
                'plan_end',
                'plan_group',
                'error',
                'client_name',
                'boy_participants',
                'eoy_participants',
                'boy_active_participants',
                'eoy_active_participants',
                'term_unvested_participants',
                'eoy_assets',
                'employer_contrib',
                'part_contrib',
                'other_income,',
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
                'char_2r',
                'char_2a',
                'char_2c',
                'char_2e',
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
                'asc_extract_read_success',
                'missing_irs_num']
            
            
            # In[66]:
            
            
            # Get the worktray 
            
            df = get_worktray_for_sf('5500 Automation')
            
            df_auto = df[(df['task_name'] == 'Automation Work') & (df['proj_name'] == 'DC Annual Governmental Forms - 5500-EZ, Owner Only Filer (Automated)')]
            
            len(df_auto)
            
            
            # In[71]:
            
            
            
            
            
            # In[ ]:
            
            
            # df = get_worktray_for_sf('5500 Automation')
            
            # df_auto = df[(df['task_name'] == 'Confirm No Filing Required (No non-key employees)')]
            
            # len(df_auto)
            
            
            # In[6]:
            
            
            # Now we need to pull the contents of the ASC script output folder. First we copy any PDFs from the PC output folders
            # to the 'Y:\ASC\Exported Reports\5500 Automation' directory- need to see if this conflicts with any other scripting.
            # We then copy everything in that 5500 Automation directory (except the target list) to both a dated folder in the 
            # "Dated Output" subdirectory, as well as to the "All Output"  folder. 
            
            # Once that is complete, we can check the "All Output" folder for plans that have had 5500-SF stuff output in the past-
            # any plans on the dataframe which have a _5500SFExport_ txt file in that directory are removed from the list
            # of new plans to be pulled into the ASC script target list (since we already have good data for them, apparently)
            
            # THIS PART OF THE PROCESS WILL NEED TO BE UPDATED FOR FUTURE YEARS, OR FOR OFF-CALENDAR PLANS!
            # If it keeps running indefinitely as currently configured, it will pick up previous year output and prevent plans from
            # advancing further.
            
            # copy any exported PDFs 
            files_copied = False
            
            for i in range(1,18):
                os.chdir(f"Y:/ASC/Exported Reports/PC{i}")
                pc_contents = os.listdir()
                move_files = [file for file in pc_contents if "_HCEKey55_" in file or "_TopHeavy55_" in file or "_5500SFExport_" in file]
                if len(move_files) > 0:
                    for file in move_files:
                        shutil.move(file,f'Y:/ASC/Exported Reports/5500 Automation/{file}')
                    files_copied = True
            
            if files_copied is True:
                    
                # Switch active directory to the 5500 Automation folder
                os.chdir('Y:/ASC/Exported Reports/5500 Automation')
                sf_folder = os.listdir()
            
                # files to ignore when moving
                ignore_file_list = ['5500-SF_Target_PlanList.txt','DGEM Import Logs','DGEM Import Files','Find Results','All Output','All Output (new)','Pickle','Dated Output','DGEM Import Files', 'Put Downloaded PDF Files Here', 'Testing', 'DGEM Validation Files']
                target_files = [file for file in sf_folder if file not in ignore_file_list]
            
                # create directory for current date for copy of files
                newdir_name = f'Dated Output/{today}_ASCVal Output'
                os.mkdir(newdir_name)
            
                # THIS PART NEEDS TO BE UPDATED TO DYNAMICALLY CHANGE YEAR FOLDER
                # Also going to be an issue with off-calendar plans, since I don't know if those are consistent
                for file in target_files:
                    nova.copy_file(file,f'2022/5500')
                    shutil.copy(file,f'{newdir_name}/{file}')
                    shutil.move(file,f'All Output (new)/{file}')
            
            
            # In[ ]:
            
            
            
            
            
            # In[7]:
            
            
            # get list of all plans that have output in this folder, to exclude them from the ASC target list
            # os.chdir('Y:/ASC/Exported Reports/5500 Automation/All Output')
            os.chdir('Y:/ASC/Exported Reports/5500 Automation/All Output (new)')
            
            all_output_folder = os.listdir()
            planids_with_asc_output = list(set([file.split("_")[0] for file in all_output_folder]))
            print(len(planids_with_asc_output))
            
            
            # In[8]:
            
            
            # create two dataframes from the 5500 Automation, 'Automation Work' task
            # first one will continue below, to pull all the needed sf fields and generate that output
            # second one will be used here to generate a target list for the ASC script to get the SF output
            
            df_ezpull = df_auto[df_auto['planid'].isin(planids_with_asc_output)]
            df_asc_target = df_auto[~df_auto['planid'].isin(planids_with_asc_output)]
            df_ezpull.reset_index(drop=True, inplace=True)
            
            
            # In[9]:
            
            
            len(df_ezpull), len(df_asc_target)
            
            
            # In[10]:
            
            
            df_asc_target
            
            
            # In[11]:
            
            
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
                    
            fname = "Y:/ASC/Exported Reports/5500 Automation/5500-SF_Target_PlanList.txt"
            print(len(target_list))
            np.savetxt(fname, target_list, fmt='%s')
            
            
            # In[12]:
            
            
            # df_ezpull = df_ezpull[df_ezpull['planid'] == '2147']
            # df_ezpull
            
            
            # In[13]:
            
            
            # get all client information (40 at a time to prevent the query from erroring)
            clientids = df_ezpull['client_id'].tolist()
            
            c = -(-(len(clientids)) // 40)
                
            all_clients = []
            
            for i in range(c):
                clientidsa = clientids[i*40:(i+1)*40]
                filters = ' or '.join([f'ClientId eq {clientid}' for clientid in clientidsa])
                expand = "CompanyName,EmployerDatas,Addresses.AddressType,Addresses.Address,Numbers.PhoneNumberType,Numbers.PhoneNumber"
                clients = pp.get_clients(filters=filters,expand=expand)['Values']
                all_clients.extend(clients)
            # end get client block
            
            
            # In[15]:
            
            
            ## read in Relius data extract
            relius_extract_path = r'Y:\Automation\Projects\Active\5500 SF Automation\2022\Relius Document Data 20230530.xlsx'
            
            df_relius = pd.read_excel(relius_extract_path)
            
            df_relius.dropna(subset=['EmployerEIN'], inplace=True)
            
            
            # In[16]:
            
            
            ## read in algodocs commissions information
            jh_algodocs_path = r'Y:\Automation\Projects\Active\5500 SF Automation\2022\Fees & Commissions\Master Files\JH\JH Commissions Info Master.xlsx'
            voya_algodocs_path = r'Y:\Automation\Projects\Active\5500 SF Automation\2022\Fees & Commissions\Master Files\Voya\Voya Commissions Info Master.xlsx'
            
            # add other RK algodocs paths as they become available
            
            # create single dataframe for lookup below
            df_algodocs = pd.read_excel(jh_algodocs_path).append(pd.read_excel(voya_algodocs_path))
            df_algodocs.reset_index(inplace=True)
            
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
            
            exclude_rks = ['Empower Retirement',
                            'Mass Mutual (Aviator)',
                            'Mass Mutual (Reflex)',
                            'Empower - Non-Automated Distributions',
                            'Mass Mutual']
            
            
            # In[17]:
            
            
            # read in JH loan and fee information
            jh_pars_path = r'Y:\Automation\Karen\JH PARS\ALL_PARS.xlsx'
            df_pars = pd.read_excel(jh_pars_path)
            
            # split out column with Nova plan IDs
            df_pars['planid'] = df_pars['File name'].apply(lambda x: pd.Series(str(x).split(" ")))[0]
            df_pars = df_pars.fillna(0)
            
            # create new columns for ease of pulling info below
            df_pars['fee_total'] = df_pars['Total JH Contract Admin Fees'] + df_pars['Total TPA Fees'] + df_pars['Total Redemption Fees'] + df_pars['Total Inv Adv Fees'] + df_pars['JH GIFL Fees']
            df_pars['loans'] = df_pars['Loan Value EOY']
            df_pars['corrections'] = df_pars['Deemed loan distributions'] + df_pars['Corrective Distributions']
            
            
            # In[ ]:
            
            
            
            
            
            # In[18]:
            
            
            # DGEM upload template
            template_source = r'Y:\5500\2022\Automation\EZ Production\DGEM EZ txt template.xlsx'
            df_template = pd.read_excel(template_source)
            template_columns = df_template.columns.tolist()
            
            ## import DGEM template details in order to test output values for form validity and length
            import re
            
            instructions_path = r'Y:\5500\2022\Automation\EZ Production\ASC_DGEM Form 5500EZ Template_2022.xlsx'
            
            df_dgem_instructions = pd.read_excel(instructions_path)
            
            
            # In[ ]:
            
            
            
            
            
            # In[ ]:
            
            
            
            
            
            # In[19]:
            
            
            set(df_ezpull['plan_type'].tolist())
            
            
            # In[20]:
            
            
            dgem_dataframe_values = []
            pp_update_data = []
            
            b=0
            ends_on = None
            
            for i in df_ezpull.index[b:ends_on]:
                
            #     try:
                print(b)
                error = False
            
                planid = df_ezpull.at[i,'planid']
                period_start = df_ezpull.at[i,'period_start']
                period_end = df_ezpull.at[i,'period_end']
                mep_status = df_ezpull.at[i,'mep_status']
                added_on = df_ezpull.at[i,'added_on']
                effective_on = df_ezpull.at[i,'effective_on']
                terminated_on = df_ezpull.at[i,'terminated_on']
                irs_number = df_ezpull.at[i,'irs_number']
                taskid = df_ezpull.at[i,'taskid']
                projid = df_ezpull.at[i,'projid']
                task_name = df_ezpull.at[i,'task_name']
                plan_name = df_ezpull.at[i,'plan_name']
                client_id = df_ezpull.at[i, 'client_id']
                proj_name = df_ezpull.at[i,'proj_name']
                plan_status = df_ezpull.at[i,'plan_status']
                plan_category = df_ezpull.at[i,'plan_category']
                plan_type = df_ezpull.at[i,'plan_type']
                form5500 = df_ezpull.at[i,'form5500']
                plan_end = df_ezpull.at[i,'plan_end']
                plan_group = df_ezpull.at[i,'plan_group']
            
                # create dataframe of relius information
                df_doc = df_relius[df_relius['TPA Plan ID'] == planid]
                df_doc.reset_index(inplace=True)
                if len(df_doc) == 0:
                    relius_doc_available = False
                    error = True
                    print("RELIUS ISSUE")
            
                else:
                    relius_doc_available = True
            
                # import ASC extract information
                # we should still be pointed at the "All Output" dir
                asc_extract_available = False
                asc_extract = [file for file in all_output_folder if "_5500SFExport_" in file and file.startswith(f"{planid}_") and file.endswith(".txt")]
                if len(asc_extract) > 0:
                    asc_extract_path = asc_extract[0]
                    df_asc = pd.read_table(asc_extract_path,header=None)
                    asc_extract_available = True
            
                else:
                    asc_extract_available = False
                    error = True
                    print("ASC ISSUE")
            
                # import status grid to check for missing dates & get beginning values
                blank_date_found = False
                status_grids = [file for file in all_output_folder if 'Status Grid' in file and file.startswith(f"{planid}_")]
                if len(status_grids) > 0:
                    status_grid = status_grids[0]
                    
                    if status_grid.endswith('xlsx'):
                        df_status_grid = pd.read_excel(status_grid)
                        
                    elif status_grid.endswith('txt'):
                        try:
                            df_status_grid = pd.read_table(status_grid)
                            
                        except:
                            df_status_grid = pd.read_table(status_grid,encoding='ISO-8859-1')
            
                    df_status_grid.replace({pd.NaT: 0}, inplace=True)
            
                    for i in df_status_grid.index:
                        birth_date = df_status_grid.at[i,'Birth Date']
                        hire_date = df_status_grid.at[i,'Hire Date']
                        if birth_date == 0 or hire_date == 0:
                            blank_date_found = True
                            
                    beginning_value = round(df_status_grid['Beginning Value (0; 0)'].sum())
            
                # import JH PARS
                df_algodocs_pars = df_pars[df_pars['planid'] == planid]
                df_algodocs_pars.reset_index(inplace=True)
            
                if len(df_algodocs_pars) == 0:
                    jh_pars_available = False
            
                else:
                    jh_pars_available = True
            
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
                period_start_dgem = datetime.strftime(project_period_start,'%Y-%m-%d')
                period_end_dgem = datetime.strftime(project_period_end,'%Y-%m-%d')
                
                if period_end_dgem != '2022-12-31':
                    error = True
                    
                # Tries to grab the liquidation project field from the termination project to check if
                # the plan is actually terminating or just leaving Nova
                project_fields = pp.get_project_fields_by_planid(planid, filters="FieldName eq 'Date Final Assets Liquidated'")
                if len(project_fields) == 0:
                    terminated_on = None
                else:
                    terminated_on = project_fields[0]['FieldValue']
                    if terminated_on:
                        date_plan_terminated = datetime.strptime(terminated_on,'%m/%d/%Y')        
                    
                ## terminated on date needs to be pulling
            
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
            #         if loan_amount != 0:
            #             print("LOAN!")
            #             break
                    asc_extract_read_success = True
            
                else:
                    boy_participants = None
                    eoy_participants = None
                    eoy_participants_w_acct = None
                    boy_active_participants = None
                    eoy_active_participants = None
                    term_unvested_participants = None
            
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
                    
                # Pull in JH information for fees
                if salaries_fees_commissions == 0 and jh_pars_available == True:
            
                    # pull fee total from algodocs pars dataframe
                    jh_par_fees = df_algodocs_pars.at[0,'fee_total']
            
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
               
                # First return
                first_year_return = False
                if added_on is not None:
                    if date_plan_added >= project_period_start and date_plan_added <= project_period_end:
                        first_year_return = True
            
                # additional EZ first return checks:
                takeover_check = False
                
                previous_period_end = datetime.strptime(period_end,'%m/%d/%Y %H:%M:%S %p')
                previous_period_year = previous_period_end - relativedelta(years=1)
            
                target_period_end = previous_period_year.strftime("%m/%d/%Y %H:%M:%S %p")
            
                ## try to get previous year projects for non-startups and non-takeovers
                if takeover_plan is False:
                    prior_projects = pp.get_projects_by_planid(planid, filters=f"PeriodEnd eq '{target_period_end}'")
                    if len(prior_projects) > 0:
                        prior_projects = [project for project in prior_projects if 
                                          project.startswith("DC Annual Governmental Forms - 5500-EZ, Owner Only Filer") and 
                                          "250,000" not in project]
                        if len(prior_projects) == 0:
                            first_year_return = True
                            
                if takeover_plan is True:
                    if beginning_value is not None:
                        if beginning_value > 249999:
                            first_year_return = False
                            takeover_check = False
                            
                        else:
                            takeover_check = True
                            
                    else:
                        takeover_check = True
                        
                        
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
            
                if address_available is False:
                    error = True
                    print("ADDRESS ISSUE")
            
                # End of address block
            
                # Get EIN
                ein_available = False
                plan_cycle = [plan_cycle for plan_cycle in client_info['EmployerDatas'] if plan_cycle['PeriodStart'] == period_start]
                if len(plan_cycle) > 0:
                    ein = plan_cycle[0]['EIN']
                    ein_available = True
                    print(planid, "EIN, PC")
            
                else:
                    if relius_doc_available is True:
                        ein_relius = df_doc.at[0,"EmployerEIN"]
                        ein = ein_relius.replace("-", "").replace(" ", "").strip()
                        ein_available = True
                        print(planid, "EIN, Relius")
                    else:
                        ein = None
                        ein_available = False
            
                if ein_available is False:
                    error = True
                    print("EIN ISSUE")
                
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
            
                ## end phone block
             
                ## begin business code block
            
                business_code_available = False
                
                plan_cycles = [plan_cycle for plan_cycle in client_info['EmployerDatas']]
            
                if len(plan_cycles) > 0:
                    for plan_cycle in plan_cycles:
                        try:
                            business_code = plan_cycle['NAICCode'].strip().ljust(6, '0')
                            if business_code != "000000" and business_code is not None:
                                break
                        except:
                            business_code = None
                            continue
                    
                    if business_code == "000000" or business_code is None:
                        error = True
                        
                    else:
                        business_code_available = True
                    
                else:
                    business_code = None
                    error = True
                print(planid, "business code:", business_code)
                ## end business code block
            
                ## begin SDBA block (2R char code)
            
                char_2r = False
                
                previous_year_projects = pp.get_projects_by_planid(planid, filters=f"ActiveOn gt '1/1/{previous_year}'")
                
                target_projects = [project for project in previous_year_projects if 'Annual Valuation' in project['Name'] and project['PeriodEnd'] == period_end]
                
                for project in target_projects:
                    if project['Name'].startswith("Annual Valuation"):
                        char_2r = True
            
                ## end sdba block
                
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
                        
            ## These two are not applicable to EZs and cause errors
            #         # 2F: Participant-directed investment
            #         char_2f = False
            
            #         if df_doc.at[0,"DirInvAcc"] == "x":
            #             char_2f = True       
            
            #         # 2G: Total participant-directed investment
            #         char_2g = False
            
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
            
                    
                #Check for missing NAIC codes or IRS Plan ID
                missing_irs_num = False
                
                if irs_number == "" or irs_number is None:
                    missing_irs_num = True
                    error = True
                    
                if phone_number == "" or phone_number is None:
                    error = True
                
                ## collate information above into new line to put into a dataframe:
                pp_output = [planid,
                period_start,
                period_end,
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
                plan_end,
                plan_group,
                error,
                client_name,
                boy_participants,
                eoy_participants,
                boy_active_participants,
                eoy_active_participants,
                term_unvested_participants,
                eoy_assets,
                employer_contrib,
                part_contrib,
                other_income,
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
                char_2r,
                char_2a,
                char_2c,
                char_2e,
                char_2h,
                char_2j,
                char_2k,
                char_2m,
                char_2l,
                char_2s,
                char_2t,
                char_3b,
                char_3d,
                char_3h,
                cc_string,
                relius_doc_available,
                asc_extract_available,
                asc_extract_read_success,
                missing_irs_num]
            
                pp_update_data.append(pp_output)
            
                if error == False:
                    ## Dump information into DGEM text import file
                    # dgem_dictionary = dict.fromkeys(template_columns,"")
                    
                    initial_filing = convert_TF_to_digit(first_year_return)
                    amended = convert_TF_to_digit(amended_filing)
                    final = convert_TF_to_digit(final_return)
                    short = convert_TF_to_digit(short_plan_year)
            
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
                    
                    beginning_value_dgem = ''
                    
                    if beginning_value is None:
                        beginning_value_dgem = ''
                        
                    elif beginning_value is not None and first_year_return is True:
                        beginning_value_dgem = beginning_value
                        
                    elif beginning_value is not None and takeover_plan is True:
                        beginning_value_dgem = beginning_value
                        
            
                    dgem_dictionary = {'SponsorEIN': ein,
                                    'SponsPlanNum': f"{irs_number}",
                                    'PlanYear': '2022',
                                    'PlanYearBeginDate': period_start_dgem,
                                    'PlanYearEndDate': period_end_dgem,
                                    'InitialFilingInd': initial_filing,
                                    'AmendedInd': amended,
                                    'FinalFilingInd': final,
                                    'ShortPlanYrInd': short,
                                    'Form5558ApplicationFiledInd': 1,
                                    'ExtAutomaticInd': '',
                                    'ExtSpecialInd': '',
                                    'ExtSpecialText': '',
                                    'EZForeignPlanInd': '',
                                    'EZPenaltyReliefInd': '',
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
                                    'SponsorPhoneNum': phone_number,
                                    'SponsorForeignPhoneNum': '',
                                    'BusinessCode': business_code,
                                    'AdminName': 'SAME',
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
                                    'LastRptEmplrName': '',
                                    'LastRptEmplrEIN': '',
                                    'LastRptPlanName': '',
                                    'LastRptPlanNum': '',
                                    'TotPartcpBoyCnt': boy_participants,
                                    'TotActPartcpBoyCnt': boy_active_participants,
                                    'TotActRtdSepBenefCnt': eoy_participants,
                                    'TotActPartcpEoyCnt': eoy_active_participants,
                                    'SepPartcpPartlVstdCnt': term_unvested_participants,
                                    'TotAssetsBoyAmt': beginning_value_dgem,
                                    'TotLiabilitiesBoyAmt': '',
                                    'NetAssetsBoyAmt': beginning_value_dgem,
                                    'TotAssetsEoyAmt': eoy_assets,
                                    'TotLiabilitiesEoyAmt': '',
                                    'NetAssetsEoyAmt': eoy_assets,
                                    'EmplrContribIncomeAmt': employer_contrib,
                                    'ParticipantContribIncomeAmt': part_contrib,
                                    'OthContribRcvdAmt': other_contrib,
                                    'TypePensionBnftCode': cc_string,
                                    'PartcpLoansInd': loan_yn,
                                    'PartcpLoansEoyAmt': loan_amt_formatted,
                                    'UnpaidMinContribCurrYrTotAmt': '',
                                    'DbPlanFundingReqdInd': 2,
                                    'DcPlanFundingReqdInd': 2,
                                    'RulingLetterGrantDate': '',
                                    'Sec412ReqContribAmt': '',
                                    'EmplrContribPaidAmt': '',
                                    'FundingDeficiencyAmt': '',
                                    'FundingDeadlineInd': ''}
                    
                    regex_error = False
                    
                    print(loan_yn, loan_amt_formatted, planid)
                    
                    for i in df_dgem_instructions.index:
                        tag_name = df_dgem_instructions.at[i,"Tag Name"]
                        tag_regex = df_dgem_instructions.at[i,"Regular Expressions"]
                        
                        if tag_name == "AdminName":
                            continue
                        
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
                                print("REGEX VIOLATION:", i, planid, f'{tag_regex}', tag_name, str(test_value))
                                regex_error = True
            
                        if len(str(test_value)) > max_length:
                            print("max length violation:", i, planid, f'{max_length}', str(test_value))
                            regex_error = True
                            
                if regex_error is False:
                    plan_dgem_output = list(dgem_dictionary.values())
                    dgem_dataframe_values.append(plan_dgem_output)
                    print(planid, "printing to output")
            
                b+=1
                
            print('Done!')
            
            
            # In[21]:
            
            
            dfk = pd.DataFrame(dgem_dataframe_values, columns=template_columns)
            dfk['einlu'] = dfk['SponsPlanNum'] + dfk['SponsorEIN']
            dfk
            
            
            # In[22]:
            
            
            dfk['NetAssetsEoyAmt'] = dfk['TotAssetsEoyAmt'].replace('', 0) - dfk['TotLiabilitiesEoyAmt'].replace('', 0)
            
            
            # In[23]:
            
            
            dfk['SponsorUSAddressZipCode'] = dfk['SponsorUSAddressZipCode'].str.replace('-', '')
            
            
            # In[24]:
            
            
            dgem_dataframe_values0 = dgem_dataframe_values
            
            
            # In[25]:
            
            
            # dgem_dataframe_values = dff.iloc[:, :-6].values.tolist()
            df_report = dfk.iloc[:, :].values.tolist()
            
            
            # In[26]:
            
            
            len(dgem_dataframe_values), len(dgem_dataframe_values0)
            
            
            # In[ ]:
            
            
            
            
            
            # In[27]:
            
            
            # create dgem export file, plus xlsx version
            
            if len(dgem_dataframe_values) > 0:
                df_dgem_import = pd.DataFrame(dgem_dataframe_values, columns=template_columns,dtype=str)
                
                df_obj = df_dgem_import.select_dtypes(['object'])
            
                df_dgem_import[df_obj.columns] = df_obj.apply(lambda x: x.str.strip())
                
                generated_import_file = f'Y:/ASC/Exported Reports/5500 Automation/DGEM Import Files/{today}_DGEM_EZ_Import.txt'
                
                df_dgem_import.to_csv(generated_import_file, sep ='\t', index=False, encoding='utf-8',doublequote=False)
                
                dfk.to_excel(f'Y:/ASC/Exported Reports/5500 Automation/DGEM Import Files/{today}_DGEM_EZ_Import.xlsx', index=None)
                
            else: 
                df_dgem_import = pd.DataFrame()
            
            
            # In[28]:
            
            
            # final log write
            
            if len(pp_update_data) > 0:
                df_pp_output = pd.DataFrame(pp_update_data, columns=pp_logging_columns)
                
            else:
                df_pp_output = pd.DataFrame()
            
            writer = pd.ExcelWriter(f'Y:/5500/2022/Automation/EZ Production/Logging/{today}_5500 EZ Production Log.xlsx', engine='xlsxwriter')
            
            # Write each dataframe to a different worksheet.
            df_pp_output.to_excel(writer, sheet_name='PP Data Output')
            df_dgem_import.to_excel(writer, sheet_name='DGEM Import File')
            
            # Close the Pandas Excel writer and output the Excel file.
            writer.save()
            # writer.close()
            
            
            # In[29]:
            
            
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
            
            # Click to send verification code via "Email"
            WebDriverWait(browser,10).until(ec.visibility_of_element_located((By.XPATH,'//*[@id="MFAUC_rbEmail"]')))
            browser.find_element_by_xpath('//*[@id="MFAUC_rbEmail"]').click()
            
            # Click SEND
            browser.find_element_by_xpath('//*[@id="MFAUC_btnSend"]').click()
            
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
            
            
            # In[30]:
            
            
            time.sleep(15)
            
            browser.get('https://dgem.asc-net.com/ascidoc/efast2/wizards/Import_3rdParty_txt.aspx')
            
            time.sleep(15)
            
            dropdown_upload = browser.find_element(by='id',value='ddlFormType').send_keys("Form 5500 EZ")
            upload_path = browser.find_element(by='id',value='fileSelect').send_keys(generated_import_file)
            
            email_box = browser.find_element(by='id',value='tbEmail').send_keys("automation@nova401k.com")
            email_next = browser.find_element(by='id',value='ibtnUpload').click()
            
            time.sleep(60)
            
            
            # In[31]:
            
            
            inbox = mailbox.inbox_folder().get_messages()
            
            for message in inbox:
                msg_sender = str(message.sender)
                msg_time_sent = message.sent #get time message was sent
                msg_time_sent = msg_time_sent.replace(tzinfo=None)
                recent_email = msg_time_sent > datetime.now() - timedelta(minutes=3)
                if (msg_sender == 'Import results (support@pension-plan-emails.com)'
                    and (msg_subject := message.subject) == 'Completed importing files'
                   and recent_email is True):
                    msg_body = message.body
                    regex = re.search(r'<a class="fill-div" href="([^"]+)"',msg_body) # find sendgrid url
                    import_report_download = regex.group(1)
                    message.mark_as_read()
                    email_found=True
                    break
            
            
            # In[32]:
            
            
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
                    recent_email = msg_time_sent > datetime.now() - timedelta(minutes=7)
                    print(time.time(),msg_sender,msg_time_sent)
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
            
            
            # In[33]:
            
            
            ## Reads in the import results
            latest_import_result = get_latest_file(download_path)
            import_directory = r'Y:\ASC\Exported Reports\5500 Automation\DGEM Import Logs'
            
            df_import_results = pd.read_table(latest_import_result, converters={"EIN":str,"Plan Number":str})
            
            df_import_results['Lookup'] = df_import_results['Plan Number'] + df_import_results['EIN']
            
            just_import_file = latest_import_result.split('\\')[1]
            shutil.copy(latest_import_result, f"{import_directory}/{just_import_file}")
            os.remove(latest_import_result)
            
            
            # In[34]:
            
            
            df_import_results
            
            
            # In[35]:
            
            
            df_pp_output['Lookup'] = df_pp_output['irs_number'] + df_pp_output['ein']
            
            
            # In[36]:
            
            
            df_import_concat = df_pp_output.merge(df_import_results, how='right', on="Lookup")
            df_successful_upload = df_import_concat[df_import_concat['Result'] == "Success"]
            
            df_failed_upload = df_import_concat[df_import_concat['Result'] != "Success"]
            
            
            # In[37]:
            
            
            df_failed_upload
            
            
            # In[38]:
            
            
            # Advances the projects that had DGEM upload issues to specialists for correction
            
            note_text1 = "Needs to be completed manually."
            
            task_names = ['Automation Work','Specialist Correction of ASC Data']
            
            for i in df_failed_upload.index[1:]:
                print(i)
                planid = str(df_failed_upload.at[i,'planid'])
                period_end = df_failed_upload.at[i,'period_end']
                error_message = df_failed_upload.at[i,'Message']
                projid = str(df_failed_upload.at[i,'projid'])
            
                note_text2 = "DGEM Upload error - " + error_message
                
                project = pp.get_project_by_projectid(projid, expand="TaskGroups.Tasks")
                    
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
            
            
            # In[ ]:
            
            
            
            
            
            # In[39]:
            
            
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
            
                period_end_year = int(period_end_dgem.split("-")[0])
                
                unsupported_form_date = False
                
                if period_end_year - unsupported_form_year != 1:
                    unsupported_form_date = True
                    note_text_to_add1 = "Unsupported plan year: " + str(period_end_year) + " <br>"
                    note_text2 += note_text_to_add1
                    advance = True
                
                if short_plan_year == "True":
                    note_text_to_add2 = "Process does not yet support short plan years. <br>"
                    note_text2 += note_text_to_add2
                    advance = True
                                      
                if ein_available == "True":
                    note_text_to_add3 = "Plan EIN is not available for the relevant period end. <br>"
                    note_text2 += note_text_to_add3
                    advance = True
            
                if relius_doc_available == "True":
                    note_text_to_add4 = "Relius plan document extract is not available. <br>"
                    note_text2 += note_text_to_add4
                    advance = True
                                      
                if phone_available == "True":
                    note_text_to_add5 = "There is no phone number on file for the plan. <br>"
                    note_text2 += note_text_to_add5
                    advance = True
                                      
                if business_code_available == "True":
                    note_text_to_add6 = "Business code is not available or not valid. <br>"
                    note_text2 += note_text_to_add6
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
            
            
            # In[40]:
            
            
            # Download Find results
            time.sleep(10)
            errorlist = []
            
            browser.get('https://dgem.asc-net.com/ascidoc/Find.aspx')
            
            time.sleep(10)
            checkbox = browser.find_element(by='name',value='cb5500StatusChangedAfter').click()
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
            
            results = pd.read_html(latest_find_result, converters={"EIN":str,"PlanNumber":str,"f5500Id":str})
            df_results = results[0]
            
            df_results['Lookup'] = df_results['PlanNumber'] + df_results['EIN']
            
            os.remove(latest_find_result)
            
            
            # In[41]:
            
            
            df_successful_upload['Plan Name'] = df_successful_upload['Plan Name'].apply(lambda x: saxutils.unescape(x))
            
            df_dl_vl_targets = df_results.merge(df_successful_upload, on='Lookup')
            
            
            # In[42]:
            
            
            try:
                planlist = list(set(df_dl_vl_targets['PlanName'].tolist()))
                
            except:
                planlist = list(set(df_dl_vl_targets['PlanName_x'].tolist()))
            
            # planlist = df_dl_vl_targets['Lookup'].tolist()
              
            # planlist = [saxutils.unescape(plan) for plan in planlist]
            print(len(planlist))
            
            
            # In[43]:
            
            
            # Request PDFs to be emailed to automation@nova401k.com
            
            browser.get('https://dgem.asc-net.com/ascidoc/Find.aspx')
            
            checkbox = browser.find_element(by='name',value='cb5500StatusChangedAfter').click()
            
            checkbox = browser.find_element(by='id',value='lbtnNext_5500').click()
            
            
            # In[44]:
            
            
            b=0
            errorlist = []
            for plan in planlist[b:]:
                try:
                    browser.find_element(by='xpath',value=f'//td[contains(text(),"{plan}")]/ancestor::tr[1]//input[@type = "checkbox"]').click()
                    b+=1
                    print(plan, b, planlist.index(plan))
            #         if b == 500:
            #             break
                except:
                    errorlist.append(plan)
                    continue
            
            dropdown_menu = browser.find_element(by='name',value='lbAction5500').send_keys("Export PDF (5500VS Batch)")
            
            
            # In[45]:
            
            
            errorlist
            
            
            # In[46]:
            
            
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
            
            
            # In[47]:
            
            
            ## Pre-validate and get validation file
            
            errorlist = []
            
            browser.get('https://dgem.asc-net.com/ascidoc/Find.aspx')
            
            checkbox = browser.find_element(by='name',value='cb5500StatusChangedAfter').click()
            
            checkbox = browser.find_element(by='id',value='lbtnNext_5500').click()
            b=0
            
            for plan in planlist[b:]:
            
                try:
                    browser.find_element(by='xpath',value='//td[contains(text(),"{}")]/ancestor::tr[1]//input[@type = "checkbox"]'.format(plan)).click()
                    b+=1
                    print(plan, b, planlist.index(plan))
            #         if b == 67:
            #             break
                except:
                    errorlist.append(plan)
                    continue
            dropdown_menu = browser.find_element(by='name',value='lbAction5500').send_keys("Pre-validate (5500VS Batch)")
            
            
            # In[48]:
            
            
            find_next = browser.find_element(by='id',value='lbtnClientNext5500').click()
            
            time.sleep(10)
            
            link_check = False
            
            while link_check is False:
                if "Pre-Validate completed" in browser.page_source:
                    time.sleep(1)
                    browser.find_element(by='link text',value='Click here').click()
                    link_check = True
                    
            time.sleep(30)
            
            
            
            
            
            ## Copy the file
            validation_directory = r'Y:\ASC\Exported Reports\5500 Automation\DGEM Validation Files'
            
            
            # In[49]:
            
            
            # Get downloaded file
            latest_file_string = get_latest_file(download_path)
            
            just_file = latest_file_string.split('\\')[1]
            shutil.copy(latest_file_string, f"{validation_directory}/{just_file}")
            os.remove(latest_file_string)
            
            browser.quit()
            
            
            # In[50]:
            
            
            pdf_root = 'Y:/5500/2022/Automation/EZ Production/PDF Downloads'
            
            # Switch active directory to the folder where the PDFs are placed after download
            os.chdir('Y:/ASC/Exported Reports/5500 Automation/Put Downloaded PDF Files Here')
            pdf_folder = os.listdir()
            pdf_folder = [file for file in pdf_folder if file.endswith(".pdf")]
            
            pdf_folder = ([file for file in pdf_folder if 'Form5500_' in file and "_EZ_" in file and not '__' in file and not 'Identifier' in file])
            len(pdf_folder)
            
            if len(pdf_folder) > 1:
            
                # create directory for current date for copy of files
                newdir_name = f'{pdf_root}/Dated Form Downloads/{today} PDF Downloads'
                
                if not os.path.exists(newdir_name):
                
                    os.mkdir(newdir_name)
            
                for file in pdf_folder:
                
                    tpa_planid = file.split("_")[1]
                    if "Form 5500-EZ.pdf" not in file:
                        try:
                            os.rename(file, f"{tpa_planid}_Form 5500-EZ_{today}.pdf")
            
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
            
            
            # In[51]:
            
            
            a = target_files[:]
            a
            
            # THIS PART NEEDS TO BE UPDATED TO DYNAMICALLY CHANGE YEAR FOLDER
            # Also going to be an issue with off-calendar plans, since I don't know if those are consistent
                
                
            for file in a:
                
                try:
                
                
                    tpa_planid = file.split('_')[0]
            
            
            #     this should work for now since these are the only projects, will need to update this to work off a worktray in the future 
            
                    project = pp.get_projects_by_planid(tpa_planid, filters="Name eq 'DC Annual Governmental Forms - 5500-EZ, Owner Only Filer (Automated)'", expand="TaskGroups.Tasks")[0]
            
                    for taskgroup in project['TaskGroups']:
                        for task in taskgroup['Tasks']:
                            if task['TaskName'] == 'Automation Work' or task['TaskName'] == 'Specialist Correction of ASC Data':
                                pp.override_task(task['Id'])
                                print(tpa_planid, task['TaskName'], "overridden")
            
                    nova.copy_file(file,f'2022/5500')
                    shutil.copy(file,f'{newdir_name}/{file}')
                    shutil.move(file,f'{pdf_root}/All Processed PDF Downloads/{file}')
                    
                except Exception as e:
                    
                    print('                         ', tpa_planid, 'error', e)
                    continue
                        
            print('Done!')
            
            
            # In[52]:
            
            
            df_populate = get_worktray_for_sf('5500 Preparation')
            
            
            # In[53]:
            
            
            df_populate = df_populate[(df_populate['task_name'] == 'Specialist Review of Form 5500') & (df_populate['proj_name'] == 'DC Annual Governmental Forms - 5500-EZ, Owner Only Filer (Automated)')]
            df_populate.dtypes
            
            
            # In[54]:
            
            
            latest_file_string = get_latest_file(validation_directory)
            
            try:
                valdata = pd.read_html(latest_file_string, converters={"EIN":str,"Plan Number":str})
                dfv = valdata[0]
                dfv_header = dfv.iloc[0] #grab the first row for the header
                dfv = dfv[1:] #take the data less the header row
                dfv.columns = dfv_header #set the header row as the df header    
                
            except:
                dfv = pd.read_excel(latest_file_string, converters={"EIN":str,"Plan Number":str})
                
            dfv['Plan Number'] = dfv['Plan Number'].str.zfill(3)
            dfv['Lookup'] = dfv['Plan Number'] + dfv['EIN']
            
            ###
            
            df_validation = dfv.merge(df_results, on='Lookup')
            df_validation['TPA Plan ID'] = df_validation['f5500Id']
            
            validation_list = df_validation['TPA Plan ID'].astype(str).tolist()
            validation_list = list(set(validation_list))
            validation_list = [str(value).split(".0")[0] for value in validation_list]
            
            df_populate = df_populate[df_populate['planid'].isin(validation_list)]
            df_populate
            
            
            # In[55]:
            
            
            df_populate
            
            
            # In[56]:
            
            
            print(len(df_populate))
            
            # get all client information (80 at a time to prevent the query from erroring)
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
            
            os.chdir('Y:/ASC/Exported Reports/5500 Automation/All Output (new)')
            
            
            # In[57]:
            
            
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
                
                previous_year_projects = pp.get_projects_by_planid(planid, filters=f"ActiveOn gt '1/1/{previous_year}'",expand="Plan.MultipleEmployerPlan,Plan.Status,Plan.PlanType,Plan.PlanCategory,Plan.FilingStatus,Plan.PlanGroup,TaskGroups.Tasks")
                
            #     # get refund projects for field population later
            #     refund_projects = [project for project in previous_year_projects if ('Refund' in project['Name'])]
            #     if len(refund_projects) > 0:
            #         refunds = True
                    
            #     else:
            #         refunds = False
                    
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
                asc_extract_available = False
                asc_extract = [file for file in all_output_folder if "_5500SFExport_" in file and file.startswith(f"{planid}_") and file.endswith(".txt")]
                if len(asc_extract) > 0:
                    asc_extract_path = asc_extract[0]
                    df_asc = pd.read_table(asc_extract_path,header=None)
                    asc_extract_available = True
            
                else:
                    asc_extract_available = False
                    error = True
            
                # import status grid to check for missing dates & get beginning values
                blank_date_found = False
                status_grids = [file for file in all_output_folder if 'Status Grid' in file and file.startswith(f"{planid}_")]
                if len(status_grids) > 0:
                    status_grid = status_grids[0]
                    
                    if status_grid.endswith('xlsx'):
                        df_status_grid = pd.read_excel(status_grid)
                        
                    elif status_grid.endswith('txt'):
                        try:
                            df_status_grid = pd.read_table(status_grid)
                            
                        except:
                            df_status_grid = pd.read_table(status_grid,encoding='ISO-8859-1')
            
                    df_status_grid.replace({pd.NaT: 0}, inplace=True)
            
                    for i in df_status_grid.index:
                        birth_date = df_status_grid.at[i,'Birth Date']
                        hire_date = df_status_grid.at[i,'Hire Date']
                        if birth_date == 0 or hire_date == 0:
                            blank_date_found = True
                            
                    beginning_value = round(df_status_grid['Beginning Value (0; 0)'].sum())
            
                # import JH PARS
                df_algodocs_pars = df_pars[df_pars['planid'] == planid]
                df_algodocs_pars.reset_index(inplace=True)
            
                if len(df_algodocs_pars) == 0:
                    jh_pars_available = False
            
                else:
                    jh_pars_available = True
            
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
                period_start_dgem = datetime.strftime(project_period_start,'%Y-%m-%d')
                period_end_dgem = datetime.strftime(project_period_end,'%Y-%m-%d')
            
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
            
                exclude_rks = ['Empower Retirement',
                                'Mass Mutual (Aviator)',
                                'Mass Mutual (Reflex)',
                                'Empower - Non-Automated Distributions',
                                'Mass Mutual']
            
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
                
            #     # import algodocs info
            #     df_algodocs_info = df_algodocs[df_algodocs['plan_id'] == planid]
            #     df_algodocs_info.reset_index(inplace=True)
                
            #     rk_without_schedule_a = False
            #     missing_commission_info = False
                
            #     # if algodocs isn't available, we check to see if the RK doesn't have algodocs output
            #     if len(df_algodocs_info) == 0:
            #         primary_investment_provider = [provider for provider in plan_info['InvestmentProviderLinks'] if provider['IsPrimary'] is True and provider['InvestmentProvider']['DisplayName'] not in algodocs_rks]
            #         if len(primary_investment_provider) == 0:
            #             algodocs_available = False
            #             missing_commission_info = True
            #         elif len(primary_investment_provider) > 0:
            #             algodocs_available = False
            #             rk_without_schedule_a = True
            
            #     else:
            #         algodocs_available = True
                    
            #     # pull in algodocs information for schedule As
            #     if algodocs_available == True:
            
            #         # pull fees from algodocs dataframe
            #         schedulea = df_algodocs_info.at[0,'sum']
            #         broker_fees = int(round(schedulea,0))
            
            #     if algodocs_available == False:
            #         broker_fees = 0
            
                # First return
                first_year_return = False
                if added_on is not None:
                    if date_plan_added >= project_period_start and date_plan_added <= project_period_end:
                        first_year_return = True
            
                # additional EZ first return checks:
                takeover_check = False
                
                previous_period_end = datetime.strptime(period_end,'%m/%d/%Y %H:%M:%S %p')
                previous_period_year = previous_period_end - relativedelta(years=1)
            
                target_period_end = previous_period_year.strftime("%m/%d/%Y %H:%M:%S %p")
                
                ## try to get previous year projects for non-startups and non-takeovers
                if takeover_plan is False:
                    prior_projects = pp.get_projects_by_planid(planid, filters=f"PeriodEnd eq '{target_period_end}'")
                    if len(prior_projects) > 0:
                        prior_projects = [project for project in prior_projects if 
                                          project.startswith("DC Annual Governmental Forms - 5500-EZ, Owner Only Filer") and 
                                          "250,000" not in project]
                        if len(prior_projects) == 0:
                            first_year_return = True
                            
                if takeover_plan is True:
                    if beginning_value is not None:
                        if beginning_value > 249999:
                            first_year_return = False
                            takeover_check = False
                            
                        else:
                            takeover_check = True
                            
                    else:
                        takeover_check = True
            
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
            
                if address_available is False:
                    error = True
            
                # End of address block
            
                # Get EIN
                ein_available = False
                plan_cycle = [plan_cycle for plan_cycle in client_info['EmployerDatas'] if plan_cycle['PeriodStart'] == period_start]
                if len(plan_cycle) > 0:
                    ein = plan_cycle[0]['EIN']
                    ein_available = True
                    print(planid, "EIN, PC")
            
                else:
                    if relius_doc_available is True:
                        ein_relius = df_doc.at[0,"EmployerEIN"]
                        ein = ein_relius.replace("-", "").replace(" ", "").strip()
                        ein_available = True
                        print(planid, "EIN, Relius")
                    else:
                        ein = None
                        ein_available = False
            
                if ein_available is False:
                    error = True
                    print("EIN ISSUE")
                
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
            
                ## end phone block
             
                ## begin business code block
            
                business_code_available = False
                
                plan_cycles = [plan_cycle for plan_cycle in client_info['EmployerDatas']]
            
                if len(plan_cycles) > 0:
                    for plan_cycle in plan_cycles:
                        try:
                            business_code = plan_cycle['NAICCode'].strip().ljust(6, '0')
                            if business_code != "000000" and business_code is not None:
                                break
                        except:
                            business_code = None
                            continue
                    
                    if business_code == "000000" or business_code is None:
                        error = True
                        
                    else:
                        business_code_available = True
                    
                else:
                    business_code = None
                    error = True
                print(planid, "business code:", business_code)
                ## end business code block
            
                ## begin SDBA block (2R char code)
            
                char_2r = False
            
                previous_year_projects = pp.get_projects_by_planid(planid, filters=f"ActiveOn gt '1/1/{previous_year}'")
            
                target_projects = [project for project in previous_year_projects if 'Annual Valuation' in project['Name'] and project['PeriodEnd'] == period_end]
            
                for project in target_projects:
                    if project['Name'].startswith("Annual Valuation"):
                        char_2r = True
            
                ## end sdba block
            
                ## check for prior year EIN
            
                prior_ein_avail = False
                ein_mismatch = False
                previous_ein = None
            
                previous_period_end = datetime.strptime(period_end,'%m/%d/%Y %H:%M:%S %p')
                previous_period_year = previous_period_end - relativedelta(years=1)
                previous_period_end_fmt = datetime.strftime(previous_period_year,'%m/%d/%Y')
            
                previous_plan_cycle = [plan_cycle for plan_cycle in client_info['EmployerDatas'] if 
                              plan_cycle['PeriodEnd'] == previous_period_end_fmt]
                
                if len(previous_plan_cycle) > 0:
                    previous_ein = plan_cycle[0]['EIN']
                    prior_ein_avail = True
                    
                if prior_ein_avail is True:
                    if previous_ein != ein:
                        ein_mismatch = True
                        print("ein mismatch! prior year:", previous_ein, "| current year:", ein)
            
                ## end prior year EIN check
            
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
            
                ## collate information above into new line to put into a dataframe:
                pp_output = [planid,
                period_start,
                period_end,
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
                plan_end,
                plan_group,
                error,
                client_name,
                boy_participants,
                eoy_participants,
                boy_active_participants,
                eoy_active_participants,
                term_unvested_participants,
                eoy_assets,
                employer_contrib,
                part_contrib,
                other_income,
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
                char_2r,
                char_2a,
                char_2c,
                char_2e,
                char_2h,
                char_2j,
                char_2k,
                char_2m,
                char_2l,
                char_2s,
                char_2t,
                char_3b,
                char_3d,
                char_3h,
                cc_string,
                relius_doc_available,
                asc_extract_available,
                asc_extract_read_success,
                missing_irs_num]
            
                pp_update_data.append(pp_output)
            
                if error == False:
                    ## Dump information into DGEM text import file
                    # dgem_dictionary = dict.fromkeys(template_columns,"")
            
                    plan_year = period_end.split("-")[0]
            
                    initial_filing = convert_TF_to_digit(first_year_return)
                    amended = convert_TF_to_digit(amended_filing)
                    final = convert_TF_to_digit(final_return)
                    short = convert_TF_to_digit(short_plan_year)
            
                    ## Begin block to populate 5500 review tasks
            
                    # notes on each of these, while converting TF to YN
            
                    #1 
                    blank_date_yn = convert_TF_to_YN(blank_date_found) #True or False, if True, populate with "Yes"
            
                    #2
                    df_validation_individual = df_validation[df_validation['TPA Plan ID'].astype(str).apply(lambda x: x.split('.')[0]) == planid].reset_index()
                    try:
                        valid_status = df_validation_individual.at[0,'Result']
                        
                    except:
                        valid_status = None
                        
                    if valid_status == "Success":
                        valid_yn = "No"
                        
                    elif valid_status == "Fail":
                        valid_yn = "Yes"
                        
            #         #3
            #         takeover_yn = convert_TF_to_YN(takeover_plan) #True or False, if True, populate with "Yes"
            
                    #4
                    ein_mismatch_yn = convert_TF_to_YN(ein_mismatch)
            
            #         #5
            #         refunds_yn = convert_TF_to_YN(refunds)
            
                    #6
                    char_2r_yn = convert_TF_to_YN(char_2r)
            
            #         #7
            #         rk_without_schedule_a_yn = convert_TF_to_YN(missing_commission_info) 
            #         #updated this to only mark it "yes" if the RK should have commissions
            
            #         #8
            #         blackout_period_yn = convert_TF_to_YN(blackout_period)
            
            #         # get projects to get task for taskid
            #         project = pp.get_project_by_projectid(projid, expand="TaskGroups.Tasks")
            #         for taskgroup in project['TaskGroups']:
            #             for task in taskgroup['Tasks']:
            #                 if task['TaskName'] == "Specialist Review of Form 5500":
            #                     taskid = task['Id']
            #                     print(taskid)
                    
                    all_task_items = [blank_date_yn, valid_yn, ein_mismatch_yn]
                
                    # pull task items, then update them with new values
                    if valid_status is not None:
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
            #                 if task_item['ShortName'] == '5500-SF Corrective Distributions':
            #                     task_item.update({'Value':refunds_yn})
            #                     pp.put_taskitem(task_item)    
            #                     print(task_item['ShortName'], "- populated!")
            #                 if task_item['ShortName'] == '5500-SF Takeover Plan':
            #                     task_item.update({'Value':takeover_yn})
            #                     pp.put_taskitem(task_item)
            #                     print(task_item['ShortName'], "- populated!")
                            if task_item['ShortName'] == '5500-SF EIN or Name Change':
                                task_item.update({'Value':ein_mismatch_yn})
                                pp.put_taskitem(task_item)
                                print(task_item['ShortName'], "- populated!")
                            if task_item['ShortName'] == '5500-SF SDBA':
                                task_item.update({'Value':char_2r_yn})
                                pp.put_taskitem(task_item)
                                print(task_item['ShortName'], "- populated!")
            #                 if task_item['ShortName'] == '5500-SF Blackout':
            #                     task_item.update({'Value':blackout_period_yn})
            #                     pp.put_taskitem(task_item)
            #                     print(task_item['ShortName'], "- populated!")
            #                 if task_item['ShortName'] == '5500-SF Commissions':
            #                     task_item.update({'Value':rk_without_schedule_a_yn})
            #                     pp.put_taskitem(task_item)
            #                     print(task_item['ShortName'], "- populated!")
            
                        advance_task = False
            
                        if "Yes" not in all_task_items and "Completed" not in all_task_items and valid_status is not None and takeover_check is False:
                            advance_task = True
            
                        if advance_task is True:
                            pp.override_task(taskid)
                            print(b, planid, "task advanced")
            
                        else:
                            print(b, planid, "task not advanced")    
            
            
                b+=1
            
            
            # In[58]:
            
            
            df_validation
            
            
            # In[59]:
            
            
            sys.path.insert(0, "Y:\Automation\Team Scripts\Andrew Kim\my modules")
            import datetime
            
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
            
            
            # In[60]:
            
            
            period_start
            
            
            # In[61]:
            
            
            browser.quit()
            
            
            # In[ ]:
            
            
            # df_pp_output[df_pp_output['error'].astype(str) == 'True']
            
            
            # In[ ]:
            
            
            # df_populate = get_worktray_for_sf('5500 Preparation')
            # df_populate = df_populate[df_populate['task_name'] == 'Specialist Review of Form 5500']
            # df_populate = df_populate[df_populate['proj_name'] == 'DC Annual Governmental Forms - 5500-EZ, Owner Only Filer (Automated)']
            
            
            # In[ ]:
            
            
            # df_populate
            
            
            # In[ ]:
            
            
            # for i in df_populate.index[:]:
            #     projid = df_populate.at[i,"projid"]
            #     taskid = df_populate.at[i,"taskid"]
            #     plan_name = df_populate.at[i,"plan_name"]
            #     notes = pp.get_notes_by_projid(projid)
            #     if len(notes) == 0:
            #         previous_taskid1 = str(int(taskid)-1)
            #         previous_taskid2 = str(int(taskid)-2)
            
            #         pp.uncomplete_task(previous_taskid1)
            #         pp.uncomplete_task(previous_taskid2)
            #         print(i, plan_name)
            
            
            # In[ ]:
            
            
            
            

    schedule_logger('Finished', pyfile= __file__)

except SystemExit:
    schedule_logger('Finished', pyfile= __file__)
    raise
    
except Exception as e:
    email_error_to_automation(e, __file__)
    schedule_logger('Error', pyfile= __file__)

            