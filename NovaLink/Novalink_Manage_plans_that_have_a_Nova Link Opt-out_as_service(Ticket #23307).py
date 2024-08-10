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
            

            
            # In[21]:
            
            
            import sys
            sys.path.insert(0, r'C:\Users\Public\WPy64-39100\notebooks\Anjana Shaji')
            import requests
            import pensionpro_api as pp
            import pandas as pd
            import datetime
            import numpy as np
            import os
            import json
            import shutil
            
            
            # In[25]:
            
            
            # For 'Novalink Opt-Out', ProvidedServiceId is 11038 , for Novalink Agreement Void ProvidedServiceId is 11319
            services_data = pp.get_all_services_provided(filters = "ProvidedServiceId eq 11038 or ProvidedServiceId eq 11319")
            print(len(services_data))
            for service in services_data:
                plan_data = pp.get_plan_by_planid(service['PlanId'])
                plan_id = plan_data['InternalPlanId']
                print(plan_id)
                url = f'https://bgs872jw77.execute-api.us-east-1.amazonaws.com/getInvitationStatus?act=by_plan&plan_id={plan_id}'
                response = requests.get(url)
                parsed = json.loads(response.content)
                if parsed:
                    try:
                        inactive_yn = parsed[0]["inactive_yn"]
                        if inactive_yn == 0:
                            opt_out_url = f'https://bgs872jw77.execute-api.us-east-1.amazonaws.com/setInvitationStatus?act=optOut&invitationCd={plan_id}'
                            opt_out_response = requests.get(opt_out_url)
                            opt_out_parsed = json.loads(opt_out_response.content)
                            print("opt_out_parsed: ",opt_out_parsed)
                            if opt_out_parsed['changedRows'] == 1:
                                print(f'Plan {plan_id} is deactivated in the backend')
                    except Exception as e:
                        print(e)
                        continue
                else:
                    print(f"Plan {plan_id} is already deactivated")
            print('Done!')
            
            
            # In[ ]:
            
            
            
            

    schedule_logger('Finished', pyfile= __file__)

except SystemExit:
    schedule_logger('Finished', pyfile= __file__)
    raise
    
except Exception as e:
    email_error_to_automation(e, __file__)
    schedule_logger('Error', pyfile= __file__)

            