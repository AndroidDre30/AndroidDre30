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
            

            
            # In[7]:
            
            
            import sys
            sys.path.insert(0, r'C:\Users\Public\WPy64-39100\notebooks\Anjana Shaji')
            import requests
            import pensionpro_api as pp
            import pandas as pd
            import datetime
            from glob import glob
            import time
            import json
            import os
            import shutil
            
            
            # In[8]:
            
            
            year = '2023'
            path = r'Y:\Automation\NovaLink\full_census'
            done_folder = r'Y:\Automation\NovaLink\full_census\done'
            df = pp.get_worktray2('Novalink', get_all=True)
            filt1 = df['task_name'] == 'Census File Creation'
            filt2 = df['proj_name'] == 'Novalink Census Upload'
            filt3 = df['proj_name'] == 'NovaLink Census Upload'
            df = df[filt1 & (filt2 | filt3)]
            if df.empty:
                raise SystemExit("Script is shutting down")
            for index, row in df.iterrows():
                plan_id = row['planid']
                print(plan_id)
                census_file_name = f'{plan_id}_NOVALINK_CENSUS_{year}'
                move_directory = r'G:'
                client_folder = ''
                census_file = None
                try:
                    for file in os.listdir(path):
                        if os.path.isfile(os.path.join(path, file)):
                            if file.startswith(census_file_name):
                                census_file = file
                                file_name, extension = os.path.splitext(file)
                    if not census_file:
                        print(f'Census file not ready for plan {plan_id}')
                        continue
                    for folder in os.listdir(move_directory):
                        if folder.split()[0] == plan_id:
                            client_folder = folder
                    destination_folder = f'{move_directory}\{client_folder}\\{year}\Testing'
                    if not os.path.exists(destination_folder):
                        os.makedirs(destination_folder)
                    source_path = os.path.join(path, census_file)
                    destination_path = os.path.join(destination_folder, census_file)
                    done_path = os.path.join(done_folder, census_file)
                    shutil.copyfile(source_path, destination_path)
                    print(f'moved census file to client directory for plan {plan_id}')
                    shutil.move(source_path, done_path)
                    print(f'moved census file to done folder for plan {plan_id}')
                    # file_name_to_save = f'2023 Novalink Census.{extension}'
                    # os.rename(f'{destination_folder}/{census_file}', f'{destination_folder}/{file_name_to_save}')
                except Exception as e:
                    print(e)
                    print(f'Error while moving the census file or census file already found for plan {plan_id}')
                    continue
                
                pp.override_task(row['taskid'])
                print(f'Task overridden for plan {plan_id}')
                
            print('Done!')    
            

    schedule_logger('Finished', pyfile= __file__)

except SystemExit:
    schedule_logger('Finished', pyfile= __file__)
    raise
    
except Exception as e:
    email_error_to_automation(e, __file__)
    schedule_logger('Error', pyfile= __file__)

            