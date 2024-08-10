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
            import pensionpro_api as pp
            from dateutil.relativedelta import relativedelta
            from datetime import datetime,timedelta,date
            import numpy as np
            import time
            import pandas as pd
            import os
            from lxml import etree
            from xml.sax.saxutils import escape
            start = datetime.now()
            print(start)
            
            
            # In[2]:
            
            
            d12_df = pd.read_table(r'F:\ASC\USER\All ASC Cases - D12.TXT',index_col=None).astype(object)
            d18_df = pd.read_table(r'F:\ASC\USER\All ASC Cases - D18.TXT',index_col=None).astype(object)
            
            d12_df.rename(columns={'S:CLIENTNO': 'tpa_plan_id'}, inplace=True)
            d18_df.rename(columns={'S:CLIENTNO': 'tpa_plan_id'}, inplace=True)
            output_folder = r'Y:\ASC\Exported Reports\SSAPlanList.txt'
            asc_file_folder = r'Y:\ASC\Exported Reports\SSAs'
            xml_folder = r'Y:\5500\2022\Automation\8955-SSA'
            error_path = r'Y:\Automation\Team Scripts\Anjana Shaji\8955 SSA DF\error_report.xlsx'
            os.chdir(asc_file_folder)
            xml_df_file = r"Y:\Automation\Team Scripts\Anjana Shaji\8955 SSA DF\df_concat.xlsx"
            today_date = date.today()
            today_date = datetime.strftime(today_date, "%m-%d-%Y")
            print(today_date)
            
            
            # In[3]:
            
            
            fake_names = ['ACCOUNT','FORFEITURE','TRANSFER',
                          'ZZZ','2020','2021','2022','2023','2024',
                          '2025','HOLDING','TAKEOVER','1',' DNU ', 'ZZ', 'DNU', 'UNCASHED', 'Mr. Forfeiture', 'Nan']
            ssn_fakes = ['111-11-1111', '222-22-2222', '333-33-3333', '444-44-4444', '555-55-5555', '666-66-6666', '777-77-7777', '888-88-8888', '000-00-0000',
                        111-11-1111, 222-22-2222, 333-33-3333, 444-44-4444, 555-55-5555, 666-66-6666, 777-77-7777, 888-88-8888, 000-00-0000]
            
            
            # In[4]:
            
            
            def update_ssa_count(ssa_participant_count, project_id):
                task_group = pp.get_task_groups_by_projectid(project_id, expand = 'tasks.TaskItems')[0]
                for tasks in task_group['Tasks']:
                    if tasks['TaskName'] == 'Client corrections':
                        for task_item in tasks['TaskItems']:
                            if task_item['ShortName'] == 'SSA Participant Count':
                                task_item['Value'] = str(ssa_participant_count)
                                pp.update_taskitem(task_item)
            
            
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
                    file_period_end_dt = datetime.strptime(file_period_end,'%m%d%Y')
                    file_period_end_dt = file_period_end_dt + timedelta(hours=12)
                    if file_plan_id == plan_id and file_period_end_dt == period_end_date:
                        found_flag = True
                        file_name = file
                return found_flag, file_name 
                
            
            
            # In[7]:
            
            
            def get_asc_files():
                asc_files = os.listdir(asc_file_folder)
                asc_file_list = [file for file in asc_files if 'FormSSA' in file and file.endswith('.txt') and not file.startswith('_')]
                return asc_file_list
            
            
            # In[8]:
            
            
            def get_ssa_and_asc_df(df):
                file_found_index = []
                plan_id_dict = {}
                df_error_log = pd.read_excel(error_path)
                if df.empty:
                    raise SystemExit("Script is shutting down")
                for index, row in df.iterrows():
                    plan_id = row['planid']
                    period_end_date = row['per_end']
                    period_end_date = datetime.strptime(period_end_date,'%m/%d/%Y %H:%M:%S %p')
                    asc_file_list = get_asc_files()
                    file_exists_flag, file_name = check_if_asc_file_exists(plan_id, period_end_date, asc_file_list)
                    if file_exists_flag:
                        print('ASC file found for plan: ', plan_id)
                        file_found_index.append(index)
                        plan_id_dict[plan_id] = file_name
                    else:
                        today = date.today()
                        today = datetime.strftime(today, "%Y-%m-%d") 
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
                                    updated = task_item['UpdatedOn'] 
                                    updated = updated.rsplit('T')[0]
                                    if task_item['ShortName'] == '8955 Validation Error' and task_item['Value'] == None:
                                        task_item['Value'] = 'Complete'
                                        pp.update_taskitem(task_item)
                                        note_text = 'No ASC file found'
                                        payload = {
                                            "ProjectID": project_id, 
                                            "NoteText": f"{note_text}",
                                            "NoteCategoryId": 3514,
                                            "ShowOnPSL": False
                                                }
                            
                                        x = pp.add_note(payload)
                                    elif task_item['ShortName'] == '8955 Validation Error' and task_item['Value'] == 'Complete' and not updated == today:
                                        pp.override_task(task_id)
                        df_error_log.loc[df_error_log['TPA Plan ID'].astype(str).str.contains(plan_id), 'ASC Text File Missing'] = 'Yes'
                df_error_log.to_excel(error_path, index = False)
                df_ssa_target = df.loc[file_found_index]
                df_ssa_target.reset_index(drop=True, inplace=True)
                return df_ssa_target, plan_id_dict
            
            
            # In[9]:
            
            
            df = pp.get_worktray('Automation', get_all=True)
            filt1 = df['task_name'] == 'Completion - Form 8955-SSA'
            filt2 = df['proj_name'] == 'Form 8955-SSA (Automated)'
            df = df[filt1 & filt2]
            print(len(df))
            
            
            # In[10]:
            
            
            df
            
            
            # In[11]:
            
            
            df_ssa_target, plan_id_dict = get_ssa_and_asc_df(df)
            len(df_ssa_target)
            
            
            # In[12]:
            
            
            len(df_ssa_target)
            
            
            # In[13]:
            
            
            plan_name_list1 = df['planid'].tolist()
            plan_name_list2 = df_ssa_target['planid'].tolist()
            for plan in plan_name_list1:
                if plan not in plan_name_list2:
                    print(plan)
            
            
            # In[15]:
            
            
            # Find the ASC file for each plan and concatenate all the data into one dataframe df_concat
            cols = ['EIN','plannumber','year','form','entrycode','SSN','firstname','initial','lastname','annuitycode','payfreqcode','b','tot','b2','b3']
            df_concat = pd.DataFrame(columns=cols)
            df_error_log = pd.read_excel(error_path)
            for index, row in df_ssa_target.iterrows():
                plan_id =  row['planid']
                print(plan_id)
                period_end_date = row['per_end']
                project_id = row['projid']
                plan_name = row['plan_name']
                year = period_end_date.rsplit(' ')[0].rsplit('/')[2]
                if year == '2024':
                    print('Year is 2024')
                    continue
                ssa_participant_count = None
                close_out_flag = False
                file_name = plan_id_dict[plan_id]
                try:
                    df_asc_file = pd.read_table(file_name, skiprows=1, header=None, names=cols, dtype=str, encoding='ansi')
                except Exception as e:
                    print(e)
                    df_asc_file = pd.DataFrame()
                if df_asc_file.empty:
                    print('Empty')
                    close_out_flag = True
                else:
                    df_counts = df_asc_file[df_asc_file['entrycode'] == "Counts"]
                    last_count_indices = df_counts.index
                    if len(last_count_indices) > 0:
                        last_count_index = last_count_indices[-1]
                        last_count_index_adjusted = last_count_index + 2
                        df_final = pd.read_table(file_name, skiprows=last_count_index_adjusted, header=None, names=cols, dtype=str, encoding='ansi')
                        
                        if df_final.empty:
                            print('Empty')
                            close_out_flag = True
                        else:
                            df_final = df_final[df_final['year'] == year]
                            df_final.loc[df_final['SSN'].astype(str).str.startswith('9'), 'SSN'] = 'FOREIGN'
                            df_final.loc[df_final['SSN'].astype(str).str.startswith('000') | df_final['SSN'].astype(str).str.startswith('666'), 'SSN'] = 'FOREIGN'
                            
                            df_final.loc[df_final['SSN'].astype(str).str.contains('-00-'), 'SSN'] = 'FOREIGN'
                            df_final.loc[df_final['SSN'].astype(str).str.contains('-0000'), 'SSN'] = 'FOREIGN'
                            ssn_mask = df_final['SSN'].isin(ssn_fakes)
                            df_final.loc[ssn_mask, 'SSN'] = '999-99-9999'
                            contains_letters = df_final['SSN'].astype(str).str.contains(r'[a-zA-Z]', na=False)
                            df_final.loc[contains_letters, 'SSN'] = 'FOREIGN'
                            
                            firstname_mask = df_final['firstname'].isin(fake_names)
                            lastname_mask = df_final['lastname'].isin(fake_names)
                            initial_mask = df_final['initial'].isin(fake_names)
                            df_final.loc[firstname_mask | lastname_mask | initial_mask, 'SSN'] = '999-99-9999'
                            df_final.loc[df_final['firstname'].isna() | (df_final['firstname'] == ''), 'SSN'] = '999-99-9999'
                            df_final.loc[df_final['lastname'].isna() | (df_final['lastname'] == ''), 'SSN'] = '999-99-9999'
                            
                            
                            df_final['tot2'] = df_final.tot.fillna(0).astype(float).apply(lambda x: f'{x:.0f}')
                            df_final.loc[(df_final['entrycode'] == 'A') & (df_final['tot2'].astype(int) < 1) & (df_final['tot2'].astype(int) >= 0), 'tot2'] = '1'
                            df_final.tot2.replace('0', pd.NaT, inplace=True)
                            df_final = df_final[df_final['SSN'] != '999-99-9999']
                            df_final['planid'] = plan_id
                            df_final['projid'] = project_id
                            df_final['A'] = len(df_final[df_final.entrycode == 'A'])
                            df_final['A'] = df_final['A'].apply(lambda x: f'{x:.0f}')
                            df_final['D'] = len(df_final[df_final.entrycode == 'D']) 
                            df_final['planname'] = plan_name
                            
                            df_concat = pd.concat([df_concat, df_final], ignore_index=True)
                            try:
                                a_sum = df_final['A'].astype(int).iloc[0]
                                d_sum = df_final['D'].astype(int).iloc[0]
                                ssa_participant_count = a_sum + d_sum 
                                update_ssa_count(ssa_participant_count, project_id)
                            except Exception as e:
                                continue
                    elif len(last_count_indices) == 0:
                        df_final = pd.read_table(file_name, skiprows=1, header=None, names=cols, dtype=str, encoding='ansi')
                        if df_final.empty:
                            print('Empty')
                            close_out_flag = True
                        else:
                            df_final = df_final[df_final['year'] == year]
                            df_final.loc[df_final['SSN'].astype(str).str.startswith('9'), 'SSN'] = 'FOREIGN'
                            df_final.loc[df_final['SSN'].astype(str).str.startswith('000') | df_final['SSN'].astype(str).str.startswith('666'), 'SSN'] = 'FOREIGN'
                            df_final.loc[df_final['SSN'].astype(str).str.contains('-00-'), 'SSN'] = 'FOREIGN'
                            df_final.loc[df_final['SSN'].astype(str).str.contains('-0000'), 'SSN'] = 'FOREIGN'
                            ssn_mask = df_final['SSN'].isin(ssn_fakes)
                            df_final.loc[ssn_mask, 'SSN'] = '999-99-9999'
                            contains_letters = df_final['SSN'].astype(str).str.contains(r'[a-zA-Z]', na=False)
                            df_final.loc[contains_letters, 'SSN'] = 'FOREIGN'
                            firstname_mask = df_final['firstname'].isin(fake_names)
                            lastname_mask = df_final['lastname'].isin(fake_names)
                            initial_mask = df_final['initial'].isin(fake_names)
                            df_final.loc[firstname_mask | lastname_mask | initial_mask, 'SSN'] = '999-99-9999'
                            df_final.loc[df_final['firstname'].isna() | (df_final['firstname'] == ''), 'SSN'] = '999-99-9999'
                            df_final.loc[df_final['lastname'].isna() | (df_final['lastname'] == ''), 'SSN'] = '999-99-9999'
                            df_final['tot2'] = df_final.tot.fillna(0).astype(float).apply(lambda x: f'{x:.0f}')
                            df_final.loc[(df_final['entrycode'] == 'A') & (df_final['tot2'].astype(int) < 1) & (df_final['tot2'].astype(int) >= 0), 'tot2'] = '1'
                            df_final.tot2.replace('0', pd.NaT, inplace=True)
                            df_final = df_final[df_final['SSN'] != '999-99-9999']
                            df_final['planid'] = plan_id
                            df_final['projid'] = project_id
                            df_final['A'] = len(df_final[df_final.entrycode == 'A'])
                            df_final['A'] = df_final['A'].apply(lambda x: f'{x:.0f}')
                            df_final['D'] = len(df_final[df_final.entrycode == 'D']) 
                            df_final['planname'] = plan_name
                            df_concat = pd.concat([df_concat, df_final], ignore_index=True)
                            try:
                                a_sum = df_final['A'].astype(int).iloc[0]
                                d_sum = df_final['D'].astype(int).iloc[0]
                                ssa_participant_count = a_sum + d_sum
                                update_ssa_count(ssa_participant_count, project_id)
                            except Exception as e:
                                continue
                if  close_out_flag:
                    print('The project needs to be closed out for plan: ', plan_id)
                    note_text = 'No participants to report, project unnecessary.'
                    payload = {
                            "ProjectID": project_id, 
                            "NoteText": f"{note_text}",
                            "NoteCategoryId": 3251,
                            "ShowOnPSL": False
                                }
            
                    x = pp.add_note(payload)
                    pp.close_project(project_id)
                    df_error_log.loc[df_error_log['TPA Plan ID'].astype(str).str.contains(plan_id), 'Project Closed Out'] = 'Yes'
                    
            df_error_log.to_excel(error_path, index = False)     
            df_concat    
            
            
            # In[16]:
            
            
            df_concat.to_excel(xml_df_file)
            
            
            # In[17]:
            
            
            # Generate the xml file that needs to be uploaded to DGEM 	
            str_header = '<root Version="1.0">'
            str_footer = '</root>'
            str_partinfo = ''
            xmlstring = ''
            str3 = '</Form>'
            for planid in df_concat.planid.drop_duplicates()[:]:   
                df1 = df_concat[df_concat['planid'] == planid].astype(str).fillna(value='').copy()
                # str_clientinfo = '<Form><Form5558ApplicationFiledInd>1</Form5558ApplicationFiledInd><EIN>' + df1['EIN'].iloc[0] + '</EIN><PlanName>' + escape(df1['planname'].iloc[0]) + '</PlanName><PlanNumber>' + df1['plannumber'].iloc[0] + '</PlanNumber><!-- Line 6a --><ReqPartCnt>' + df1['A'].iloc[0] + '</ReqPartCnt><!-- Line 6b --><VolPartCnt>0</VolPartCnt><!-- Line 7  --><TotalPartCnt>' + df1['A'].iloc[0] + '</TotalPartCnt>'        
                str_clientinfo = '<Form><EIN>' + df1['EIN'].iloc[0] + '</EIN><PlanName>' + escape(df1['planname'].iloc[0]) + '</PlanName><PlanNumber>' + df1['plannumber'].iloc[0] + '</PlanNumber><!-- Line 6a --><ReqPartCnt>' + df1['A'].iloc[0] + '</ReqPartCnt><!-- Line 6b --><VolPartCnt>0</VolPartCnt><!-- Line 7  --><TotalPartCnt>' + df1['A'].iloc[0] + '</TotalPartCnt>'        
                partstring = ''
                print(df1['planname'].iloc[0])
                for i in df1.index:
                    if df1['entrycode'].loc[i] == 'A':
                        if df1['initial'].loc[i] == 'nan':
                            partstring = '<Participant><EntryCode>A</EntryCode><SSN>' + df1['SSN'].loc[i] + '</SSN><FirstName>' + escape(df1['firstname'].loc[i]) + '</FirstName><LastName>' + escape(df1['lastname'].loc[i]) + '</LastName><AnnuityCode>A</AnnuityCode><PayFreqCode>A</PayFreqCode><DcTotValAccAmt>' + df1['tot2'].loc[i] + '</DcTotValAccAmt></Participant>'
                        else:
                            partstring = '<Participant><EntryCode>A</EntryCode><SSN>' + df1['SSN'].loc[i] + '</SSN><FirstName>' + escape(df1['firstname'].loc[i]) + '</FirstName><Initial>' + escape(df1['initial'].loc[i]) + '</Initial><LastName>' + escape(df1['lastname'].loc[i]) + '</LastName><AnnuityCode>A</AnnuityCode><PayFreqCode>A</PayFreqCode><DcTotValAccAmt>' + df1['tot2'].loc[i] + '</DcTotValAccAmt></Participant>'
                    elif df1['entrycode'].loc[i] == 'B':
                        if df1['initial'].loc[i] == 'nan':
                            partstring = '<Participant><EntryCode>B</EntryCode><SSN>' + df1['SSN'].loc[i] + '</SSN><FirstName>' + escape(df1['firstname'].loc[i]) + '</FirstName><LastName>' + escape(df1['lastname'].loc[i]) + '</LastName><AnnuityCode>A</AnnuityCode><PayFreqCode>A</PayFreqCode><DcTotValAccAmt>' + df1['tot2'].loc[i] + '</DcTotValAccAmt></Participant>'
                        else:
                            partstring = '<Participant><EntryCode>B</EntryCode><SSN>' + df1['SSN'].loc[i] + '</SSN><FirstName>' + escape(df1['firstname'].loc[i]) + '</FirstName><Initial>' + escape(df1['initial'].loc[i]) + '</Initial><LastName>' + escape(df1['lastname'].loc[i]) + '</LastName><AnnuityCode>A</AnnuityCode><PayFreqCode>A</PayFreqCode><DcTotValAccAmt>' + df1['tot2'].loc[i] + '</DcTotValAccAmt></Participant>'
                    else:
                        if df1['initial'].loc[i] == 'nan':
                            partstring = '<Participant><EntryCode>D</EntryCode><SSN>' + df1['SSN'].loc[i] + '</SSN><FirstName>' + escape(df1['firstname'].loc[i]) + '</FirstName><LastName>' + escape(df1['lastname'].loc[i]) + '</LastName></Participant>'
                        else:
                            partstring = '<Participant><EntryCode>D</EntryCode><SSN>' + df1['SSN'].loc[i] + '</SSN><FirstName>' + escape(df1['firstname'].loc[i]) + '</FirstName><Initial>' + escape(df1['initial'].loc[i]) + '</Initial><LastName>' + escape(df1['lastname'].loc[i]) + '</LastName></Participant>'
                    str_partinfo += partstring
                    partstring = ''
                xmlstring += str_clientinfo + str_partinfo + str3
                str_clientinfo = ''
                str_partinfo = ''
                print('done!')
                    
            finalxmlstring = str_header + xmlstring + str_footer
            root = etree.fromstring(finalxmlstring)
            tree = etree.ElementTree(root)
            print('write xml')
            tree.write(f'{xml_folder}/{today_date} Export 2.xml', pretty_print=True, xml_declaration=True, encoding="utf-8")
            print('XML file is ready!')
            
            
            # In[18]:
            
            
            end = datetime.now()
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

            