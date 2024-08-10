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
            
            
            import subprocess
            import os
            import datetime
            import logging
            
            import smtplib
            from email.mime.multipart import MIMEMultipart
            from email.mime.text import MIMEText
            from email.mime.base import MIMEBase
            from email import encoders
            
            
            # In[2]:
            
            
            def mail_script_failed(script_name):
                html_head = """
                <html xmlns:v="urn:schemas-microsoft-com:vml" xmlns:o="urn:schemas-microsoft-com:office:office" xmlns:w="urn:schemas-microsoft-com:office:word" xmlns:m="http://schemas.microsoft.com/office/2004/12/omml" xmlns="http://www.w3.org/TR/REC-html40">
                <head><META HTTP-EQUIV="Content-Type" CONTENT="text/html; charset=us-ascii"><meta name=Generator content="Microsoft Word 15 (filtered medium)">
                </head>"""
                
                toaddr = ['jworms@nova401k.com', 'msvehla@nova401k.com']
                cc_list = []
            
                html = html_head
            
                fromaddr = 'automation@nova401k.com'
                password = 'Rub73595'
            
                toaddrs = toaddr + cc_list
            
                if len(toaddrs) == 0:
                    x = "No emails to send"
                else:
                    msg = MIMEMultipart('alternative')
            
                    msg['From'] = fromaddr
                    msg['To'] = ','.join(toaddr)
                    msg['CC'] = ','.join(cc_list)
                    subject = f'Script {script_name} failed to run today'
                    msg['Subject'] = subject
                    part = MIMEText(html, 'html')
                    msg.attach(part)
                    
            
            
                    with smtplib.SMTP('smtp.office365.com', 587) as server:
                        server.starttls()
                        server.login(fromaddr, password)
                        x = server.sendmail(fromaddr, toaddrs, msg.as_string())
            
            
            # In[3]:
            
            
            def run_child_script(script_name, log_folder):
                try:
                    # Create log folder if it doesn't exist
                    os.makedirs(log_folder, exist_ok=True)
                    
                    # Define log file path
                    log_file = os.path.join(log_folder, f"{script_name}_log.txt")
            
                    # Open log file in append mode
                    with open(log_file, "a") as f:
                        # Run the child script and capture stdout and stderr
                        subprocess.run(["python", script_name], stdout=f, stderr=subprocess.STDOUT)
                except Exception as e:
                    print(f"Error running {script_name}: {e}")
            #         mail_script_failed(script_name)       
            
            
            # In[4]:
            
            
            def combine_logs(log_folder, combined_log_file):
                try:
                    # Open combined log file in write mode
                    with open(combined_log_file, "w") as combined_file:
                        # Loop through each log file in the log folder
                        for log_file in os.listdir(log_folder):
                            log_file_path = os.path.join(log_folder, log_file)
                            if log_file.endswith("_log.txt"):
                                # Open each log file and append its content to the combined log file
                                with open(log_file_path, "r") as f:
                                    combined_file.write(f.read())
                                
                                # Delete the individual log file
                                os.remove(log_file_path)
                except Exception as e:
                    print(f"Error combining log files: {e}")
            
            
            # In[5]:
            
            
            def main():
            #     "Form 8955-SSA Project Launch Triggers (Ticket #25009).py", 
            #                      "Form 8955-SSA (Automated) task Completion - Form 8955-SSA (Ticket #25012) Part 1.py",
            
                child_scripts = [
                                 "Form 8955-SSA (Automated) task Completion - Form 8955-SSA  (Ticket #25012) Part 2.py",
                                 "Form 8955-SSA Project - Delivery- Form 8955-SSA Task Automation Steps (Ticket #25039).py"]
                
                dt = datetime.datetime.now().strftime('%Y.%m.%d.%I.%M.%S')
                log_folder = r"Y:\Automation\Team Scripts\Anjana Shaji\8955 SSA DF\Logs" 
                combined_log_folder = r"Y:\Automation\Scheduled\stdout" 
                combined_log_file = f"{combined_log_folder}\Form_8955SSA_combined_log_{dt}.txt"
                log_file = os.path.join(log_folder, "parent_script.log")
                
                logging.basicConfig(
                filename=log_file,
                level=logging.INFO,
                format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
                )
            
                logger = logging.getLogger(__name__)
                logger.info(f'\n\n\n------------------ {dt} --------------')
                logger.info('Parent!!!!!!!!!!!!!!!')
                logger.info("Starting parent script")
                
            #     Run each child script sequentially
                for script_name in child_scripts:
                    logger.info(f"Running {script_name}...")
                    run_child_script(script_name, log_folder)
                    logger.info(f"Completed {script_name}\n")
                    
                combine_logs(log_folder, combined_log_file)
                logger.info(f"Combined log file written to {combined_log_file}")
                logger.info("Parent script completed")
            
            
            # In[6]:
            
            
            if __name__ == "__main__":
                main()
            
            
            # In[ ]:
            
            
            
            

    schedule_logger('Finished', pyfile= __file__)

except SystemExit:
    schedule_logger('Finished', pyfile= __file__)
    raise
    
except Exception as e:
    email_error_to_automation(e, __file__)
    schedule_logger('Error', pyfile= __file__)

            