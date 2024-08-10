import datetime as dt
import functools
import json
import math
import time

import pandas as pd
import requests

today = dt.date.today().strftime('%m-%d-%Y')

headers = {'username': 'KiandreThomasafs', 'apikey': '1FDL5D64j2sn1cWykcs93v68'}
#headers = {'username': 'keshih', 'apikey': 'UeT70u9Kfo6814FFF8Cppw6A'}


base = 'https://api.pensionpro.com/v1'


def retry(retries=3, delay=1):
    def wraps(func):
        request_exceptions = (
            requests.exceptions.Timeout,
            requests.exceptions.ConnectionError,
            requests.exceptions.HTTPError
        )
        @functools.wraps(func)
        def inner(*args, **kwargs):
            for i in range(retries+1):
                try:    
                    if i>0:
                        print(f'Retry #{i}... (retries={retries}, delay={pause})')
                        time.sleep(pause)
                    result = func(*args, **kwargs)
                    return result
                    
                except request_exceptions as e:

                    pause = delay
                    rr = e
                    print(f'{e.__class__.__name__}: {e}')
                    
                except APIRateLimit as e:

                    pause = 10*(i+1)
                    rr = e
                    print(f'{e.__class__.__name__}: {e}')
                                    
            raise rr
        return inner
    return wraps

class APIError(requests.exceptions.HTTPError):
    pass

class APIRuleViolation(Exception):
    pass

class APIODataError(Exception):
    pass

class APIItemNotFound(Exception):
    pass

class APIRateLimit(Exception):
    pass

show_calls_remaining=False

def check(r):

    if show_calls_remaining:
        print(r.headers['x-ratelimit-remaining'], 'calls remaining')

    try:
        rjson = r.json() if r.status_code != 204 else True
    except Exception:
        r.raise_for_status()
    
    if not r.ok:
        
        msg = ' | '.join([rjson.get('Message')]+[f"Rule: {rule.get('Rule')}" for rule in rjson.get('Rules', [])]+[f'Errors: {k} {v}' for k,v in rjson.get('Errors', {}).items()]) if isinstance(rjson, dict) else rjson

        if 'Rule Violation' in msg:
            raise APIRuleViolation(msg)

        if 'OData' in msg:
            raise APIODataError(msg)

        if r.status_code == 429:
            raise APIRateLimit(msg)
        
        raise APIError(msg, response=r)
    
    return rjson
        
def set_params(filters=None, expand=None, skip=None, top=None, orderby=None, select=None):
    
    params = {}

    if filters:
        params['$filter'] = filters
    if expand:
        params['$expand'] = expand
    if skip:
        params['$skip'] = skip
    if top:
        params['$top'] = top
    if orderby:
        params['$orderby'] = orderby
    if select:
        params['$select'] = select
        
    return params
   
    
    
    
    
@retry()
def get_sysplanid(planid:str) -> int:
    
    params = set_params(filters=f"InternalPlanId eq '{planid}'", select='Id')
    
    url = f'{base}/plans'
    
    r = requests.get(url, headers=headers, params=params, timeout=10)
   
    # print(r.headers['x-ratelimit-remaining'], 'calls remaining')

    data = check(r)['Values']

    if not data:
        raise APIItemNotFound(f'PlanId {planid} not found.')
    
    sysplanid = data[0]['Id']

    return sysplanid


@retry()
def update_taskitem(payload:dict, expand=None):
    
    params = set_params(expand=expand)
    
    taskitemid = payload['Id']
    
    url = f'{base}/taskitems/{taskitemid}'
    r = requests.put(url, json=payload, headers=headers, params=params, timeout=5)
    
    return check(r)

put_taskitem = update_taskitem


@retry()
def complete_task(taskid:int):
    
    url = f'{base}/tasks/{taskid}/completetask'
    r = requests.put(url, headers=headers, timeout=5)
    
    return check(r)

@retry()
def uncomplete_task(taskid:int):
    
    url = f'{base}/tasks/{taskid}/uncompletetask'
    r = requests.put(url, headers=headers, timeout=5)
    
    return check(r)


@retry()
def add_project(planid:str, ProjectTemplateId:int, StartDate=None, DueOn=None, PeriodStart=None, PeriodEnd=None, Description=None, filters=None, expand=None):

    """
{
  "PeriodEnd": "2020-08-19T20:18:29.341Z",
  "PeriodStart": "2020-08-19T20:18:29.341Z",
  "StartDate": "2020-08-19T20:18:29.341Z",
  "DueOn": "2020-08-19T20:18:29.341Z",
  "Description": "string",
  "PlanId": 0,
  "IsWebRequired": true,
  "ProjectTemplateId": 0,
  "HasBeenWarned": true,
  "IsDeactivated": true
}

    """
    
    params = set_params(filters=filters, expand=expand)
    
    url = f'{base}/projects'
 
    sysplanid = get_sysplanid(planid)

    payload = {
        "PlanId": sysplanid,
        "ProjectTemplateId": ProjectTemplateId,
        "StartDate": StartDate,
        "DueOn": DueOn,
        "PeriodStart":PeriodStart,
        "PeriodEnd":PeriodEnd,
        'Description':Description
    }

    r = requests.post(url, headers=headers, json=payload, params=params, timeout=15)
    return check(r)


@retry()
def add_project_file(filepath:str, ProjectId:int, ProjectFileTypeId=586, ShowOnWeb=True, Title=None, Comment='', Archived=False, HasBeenWarned=True):

    """    
    """

    if Title is None:
        Title = filepath

    url = f'{base}/projectfiles'

    payload = {
      "ProjectId": ProjectId,
      "ProjectFileTypeId": ProjectFileTypeId,
      "ShowOnWeb": ShowOnWeb,
      "Title": Title,
      "Comment": Comment,
      "Archived": Archived,
      "HasBeenWarned": HasBeenWarned
    }

    files = {
        
        'ProjectFile': (None, json.dumps(payload)),
        'file': (filepath , open(filepath, 'rb'), 'multipart/form-data')
   
    }

    r = requests.post(url, files=files, headers=headers)
    return check(r)

@retry()
def delete_project_file(projectFileId:int):

    url = f'{base}/projectfiles/{projectFileId}'
    r = requests.delete(url, headers=headers, timeout=15)
    
    return check(r)

@retry()
def get_projects_by_planid(planid:str, filters=None, expand=None, skip=None, top=None, orderby=None):

    params = set_params(filters=filters, expand=expand, skip=skip, top=top, orderby=orderby)
        
    sysplanid = get_sysplanid(planid)

    url = f'{base}/plans/{sysplanid}/projects'
    r = requests.get(url, headers=headers, params=params, timeout=15)
    
    return check(r)['Values']

@retry()
def get_project_files_by_projectid(projid:int, filters=None, expand=None, skip=None, top=None, orderby=None):
    
    params = set_params(filters=filters, expand=expand, skip=skip, top=top, orderby=orderby)
    
    url = f'{base}/projects/{projid}/projectfiles'
    r = requests.get(url, headers=headers, params=params, timeout=15)
    
    return check(r)['Values']

@retry()
def get_task_groups_by_projectid(projid:int, filters=None, expand=None, skip=None, top=None, orderby=None):

    params = set_params(filters=filters, expand=expand, skip=skip, top=top, orderby=orderby)

    url = f'{base}/projects/{projid}/taskgroups'
    r = requests.get(url, headers=headers, params=params, timeout=15)
    
    return check(r)['Values']

@retry()
def get_tasks_by_taskgroupid(taskGroupId:int, taskOrderId=None, filters=None, expand=None, skip=None, top=None, orderby=None):

    params = set_params(filters=filters, expand=expand, skip=skip, top=top, orderby=orderby)    
    
    if taskOrderId:
        params['taskOrderId'] = taskOrderId

    url = f'{base}/taskgroups/{taskGroupId}/tasks'
    r = requests.get(url, headers=headers, params=params, timeout=15)
    
    return check(r)['Values']
              
@retry()
def get_taskitems_by_taskid(taskid:int, filters=None, expand=None, skip=None, top=None, orderby=None):

    params = set_params(filters=filters, expand=expand, skip=skip, top=top, orderby=orderby)   

    url = f'{base}/tasks/{taskid}/taskitems'
    r = requests.get(url, headers=headers, params=params, timeout=15)
    
    return check(r)['Values']

get_task_items_by_taskid = get_taskitems_by_taskid

@retry()
def get_project_templates(filters=None, expand=None, skip=None, top=None, orderby=None):

    params = set_params(filters=filters, expand=expand, skip=skip, top=top, orderby=orderby) 

    url = f'{base}/projecttemplates'
    r = requests.get(url, headers=headers, params=params, timeout=15)
    
    return check(r)['Values']

@retry()
def get_projects(filters=None, expand=None, skip=None, top=None, orderby=None, get_all:bool=False):

    params = set_params(filters=filters, expand=expand, skip=skip, top=top, orderby=orderby) 
      
    url = f'{base}/projects'
        
    r = requests.get(url, headers=headers, params=params, timeout=30)
    data = check(r)
    
    if get_all:
            
        totalpages = data['TotalPages']
    
        if totalpages > 1:
            results = []
            results.extend(data['Values'])            
            
            for i in range(1, totalpages):
                params['$skip'] = i*1000
                r = requests.get(url, headers=headers, params=params, timeout=15)
                data = check(r)['Values']
                results.extend(data)
            return results
        
    return data['Values']

@retry()
def get_investment_providers_by_planid(planid:str, filters=None, expand='InvestmentProvider', skip=None, top=None, orderby=None):

    params = set_params(filters=filters, expand=expand, skip=skip, top=top, orderby=orderby)
    
    sysplanid = get_sysplanid(planid)

    url = f'{base}/plans/{sysplanid}/investmentproviders'
    r = requests.get(url, headers=headers, params=params, timeout=15)
    
    return check(r)['Values']

@retry()
def get_plans(filters=None, select=None, expand=None, top=None, orderby=None, skip=None, get_all=False):

    params = set_params(filters=filters, expand=expand, skip=skip, top=top, orderby=orderby, select=select)

    url = f'{base}/plans'
    r = requests.get(url, headers=headers, params=params, timeout=15)

    data = check(r)

    if get_all:
            
        totalpages = data['TotalPages']
    
        if totalpages > 1:
            results = []
            results.extend(data['Values'])            
            
            for i in range(1, totalpages):
                params['$skip'] = i*1000
                r = requests.get(url, headers=headers, params=params, timeout=15)
                data = check(r)['Values']
                results.extend(data)
            return results

    return data['Values']

@retry()
def get_plan_by_planid(planid:str, select=None, expand=None):

    filters = f"InternalPlanId eq '{planid}'"

    params = set_params(filters=filters, select=select, expand=expand)

    url = f'{base}/plans'
    r = requests.get(url, headers=headers, params=params, timeout=15)
    
    data = check(r)['Values']
    
    if not data:
        raise APIItemNotFound(f'PlanId {planid} not found.')
        
    return data[0]

@retry()
def get_plan_contact_roles_by_planid(planid:str, filters=None, expand='Contact,RoleType', skip=None, top=None):

    params = set_params(filters=filters, expand=expand, skip=skip, top=top)
    
    sysplanid = get_sysplanid(planid)

    url = f'{base}/plans/{sysplanid}/plancontactroles'
    r = requests.get(url, headers=headers, params=params, timeout=15)
    
    return check(r)['Values']

@retry()
def get_contact_by_id(contactid:int, expand='ContactPhoneNumberLinks.PhoneNumber,ContactPhoneNumberLinks.PhoneNumberType'):

    params = set_params(expand=expand)

    url = f'{base}/contacts/{contactid}'
    r = requests.get(url, headers=headers, params=params, timeout=15)
    
    return check(r)

@retry()
def get_clients(filters=None, select=None, expand=None, top=None, orderby=None, skip=None, get_all=False):

    params = set_params(filters=filters, expand=expand, skip=skip, top=top, orderby=orderby, select=select)

    url = f'{base}/clients'
    r = requests.get(url, headers=headers, params=params, timeout=15)
    
    return check(r)

@retry()
def get_legal_plan_files_by_planid(planid:str, filters=None, expand=None, skip=None, top=None, orderby=None):

    params = set_params(filters=filters, expand=expand, skip=skip, top=top, orderby=orderby) 
          
    sysplanid = get_sysplanid(planid)
        
    url = f'{base}/plans/{sysplanid}/legalplanfiles'
    r = requests.get(url, headers=headers, params=params, timeout=15)
    
    return check(r)['Values']

@retry()
def update_legal_plan_file(payload:dict, expand=None):
    
    params = set_params(expand=expand)
    
    id_ = payload['Id']
    
    url = f'{base}/legalplanfiles/{id_}'
    
    r = requests.put(url, json=payload, headers=headers, params=params, timeout=5)
    
    return check(r)

@retry()
def add_legal_plan_file(filepath:str, planid:str, LegalPlanFileTypeId=1446, ShowOnWeb=True, Title=None, Comment='', Archived=False, HasBeenWarned=True):

    """
    
    """
    
    sysplanid = get_sysplanid(planid)
    
    if Title is None:
        Title = filepath

    url = f'{base}/legalplanfiles'

    payload = {
      "PlanId": sysplanid,
      "LegalPlanFileTypeId": LegalPlanFileTypeId,
      "ShowOnWeb": ShowOnWeb,
      "Title": Title,
      "Comment": Comment,
      "Archived": Archived,
      "HasBeenWarned": HasBeenWarned
    }

    files = {
        'LegalPlanFile': (None, json.dumps(payload)),
        'file': (filepath , open(filepath, 'rb'), 'multipart/form-data')
    }

    r = requests.post(url, files=files, headers=headers)
    return check(r)

@retry()
def delete_legal_plan_file(legalPlanFileId:int):

    url = f'{base}/legalplanfiles/{legalPlanFileId}'
    r = requests.delete(url, headers=headers, timeout=15)
    
    return check(r)

@retry()
def add_admin_form_plan_file(filepath:str, planid:str, AdminFormPlanFileTypeId=636, ShowOnWeb=True, Title=None, Comment='', Archived=False, HasBeenWarned=True):

    """
    
    """
    
    sysplanid = get_sysplanid(planid)
        
    if Title is None:
        Title = filepath

    url = f'{base}/adminformplanfiles'

    payload = {
      "PlanId": sysplanid,
      "AdminFormPlanFileTypeId": AdminFormPlanFileTypeId,
      "ShowOnWeb": ShowOnWeb,
      "Title": Title,
      "Comment": Comment,
      "Archived": Archived,
      "HasBeenWarned": HasBeenWarned
    }

    files = {
        'AdminFormPlanFile': (None, json.dumps(payload)),
        'file': (filepath , open(filepath, 'rb'), 'multipart/form-data')
      }

    r = requests.post(url, files=files, headers=headers)
    
    return check(r)

@retry()
def get_employee_plan_roles_by_planid(planid:str, filters=None, expand='Contact,RoleType', skip=None, top=None, orderby=None):

    params = set_params(filters=filters, expand=expand, skip=skip, top=top, orderby=orderby)
    
    sysplanid = get_sysplanid(planid)

    url = f'{base}/plans/{sysplanid}/employeeplanroles'
    r = requests.get(url, headers=headers, params=params, timeout=15)
    
    return check(r)['Values']

@retry()
def put_employee_plan_role(payload:dict, expand=None):
    
    id_ = payload['Id']
    params = set_params(expand=expand)
    
    url = f'{base}/employeeplanroles/{id_}'
    r = requests.put(url, json=payload, headers=headers, params=params, timeout=5)
    
    return check(r)

@retry()
def get_contacts(filters=None, expand=None, top=None, orderby=None, skip=None):

    params = set_params(filters=filters, expand=expand, skip=skip, top=top, orderby=orderby)

    url = f'{base}/contacts'
    r = requests.get(url, headers=headers, params=params, timeout=15)
    
    return check(r)['Values']

@retry()
def get_fee_schedules_by_planid(planid:str, filters=None, expand=None, skip=None, top=None, orderby=None):

    params = set_params(filters=filters, expand=expand, skip=skip, top=top, orderby=orderby)

    sysplanid = get_sysplanid(planid)

    url = f'{base}/plans/{sysplanid}/feeschedules'
    r = requests.get(url, headers=headers, params=params, timeout=15)
    
    return check(r)['Values']

@retry()
def get_distributions(filters=None, expand=None, skip=None, top=None, orderby=None):

    params = set_params(filters=filters, expand=expand, skip=skip, top=top, orderby=orderby)

    url = f'{base}/distributions'
    r = requests.get(url, headers=headers, params=params, timeout=15)
    
    return check(r)['Values']

@retry()
def add_distribution(planid:str, DistributionReasonId=None, Participant=None, ProjectTemplateId=None, InvestmentProviderLinkId=None, FeeScheduleItemTemplateId=None, FeeScheduleItemId=None, filters=None, expand=None):

    """
    {
      "PlanId": 0,
      "DistributionReasonId": 0,
      "Participant": "{ FirstName,  LastName,  SSN }",
      "ProjectTemplateId": 0,
      "InvestmentProviderLinkId": 0,
      "FeeScheduleItemTemplateId": 0,
      "FeeScheduleItemId": 0
    }

    """
    params = set_params(filters=filters, expand=expand)

    sysplanid = get_sysplanid(planid)

    payload = {
        "PlanId": sysplanid,
        "DistributionReasonId": DistributionReasonId,
        "Participant": Participant,
        "ProjectTemplateId": ProjectTemplateId,
        "InvestmentProviderLinkId":InvestmentProviderLinkId,
        "FeeScheduleItemTemplateId":FeeScheduleItemTemplateId,
        "FeeScheduleItemId": FeeScheduleItemId
    }
    
    url = f'{base}/distributions/createplandistribution'
    r = requests.post(url, headers=headers, json=payload, params=params, timeout=15)
    
    return check(r)

@retry()
def add_distribution2(payload, filters=None, expand=None):

    """
    {
      "PlanId": 0,
      "DistributionReasonId": 0,
      "Participant": "{ FirstName,  LastName,  SSN }",
      "ProjectTemplateId": 0,
      "InvestmentProviderLinkId": 0,
      "FeeScheduleItemTemplateId": 0,
      "FeeScheduleItemId": 0
    }

    """
    params = set_params(filters=filters, expand=expand)

    url = f'{base}/distributions/createplandistribution'
    r = requests.post(url, headers=headers, json=payload, params=params, timeout=15)
    
    return check(r)

@retry()
def get_investment_providers(filters=None, expand=None, skip=None, top=None, orderby=None):

    params = set_params(filters=filters, expand=expand, skip=skip, top=top, orderby=orderby)

    url = f'{base}/investmentproviders/investmentproviders'
    r = requests.get(url, headers=headers, params=params, timeout=15)
    
    return check(r)['Values']

@retry()
def add_refund_all(planid, investprovid=None, filters=None, expand=None):

    params={}
    
    if filters:
        params.update({'$filter':filters})

    if expand:
        params.update({'$expand':expand})

    url = f'{base}/distributions/createplandistribution'
 
    #invest_prov_id = get_investment_providers(filters=f'DisplayName eq "{invest_prov}"')[0]['Id']
    
#     fee_schedule = get_fee_schedules_by_planid(planid, filters='EndDate eq null', expand='FeeScheduleItems')[0]
    fee_schedule = get_fee_schedules_by_planid(planid, filters='EndDate eq null', expand='FeeScheduleTemplate.FeeScheduleItemTemps')[0]
    sysplanid = fee_schedule['PlanId']
#     fee_schedule_item_id = [item for item in fee_schedule['FeeScheduleItems'] if item['FeeTypeId'] == 21582][0]['Id']
    fee_schedule_item_template_id = [i for i in fee_schedule['FeeScheduleTemplate']['FeeScheduleItemTemps'] if i['FeeTypeId'] == 21582][0]['Id']
    #fee_schedule_item_template_id = 195
    
    participant = {'FirstName': 'All',
                   'LastName': 'Participants',
                   'SSN':'000000000'}
    
    if not investprovid:
        investprovs = get_investment_providers_by_planid(planid)
        investprovid = [i for i in investprovs if i['IsPrimary'] is True][0]['Id']

    payload = {
        "PlanId": sysplanid,
        "DistributionReasonId": 41308,
        "Participant": participant,
        "ProjectTemplateId": 77159,
        "InvestmentProviderLinkId": investprovid,
        "FeeScheduleItemTemplateId":fee_schedule_item_template_id
        #"FeeScheduleItemId": fee_schedule_item_id
    }

    r = requests.post(url, headers=headers, json=payload, params=params, timeout=15)
    
    if not r.ok:
        print(r.json())
   
    r.raise_for_status()
         
    data = r.json()
    return data

@retry()
def add_refund(planid, first_name, last_name, ssn, reason_id=41308, investprovid=None, filters=None, expand=None):

    params={}
    
    if filters:
        params.update({'$filter':filters})

    if expand:
        params.update({'$expand':expand})

    url = f'{base}/distributions/createplandistribution'


# #   fee_schedule = get_fee_schedules_by_planid(planid, filters='EndDate eq null', expand='FeeScheduleItems')[0]
#    fee_schedule = get_fee_schedules_by_planid(planid, filters='EndDate eq null', expand='FeeScheduleTemplate.FeeScheduleItemTemps')[0]
#    sysplanid = fee_schedule['PlanId']
#     fee_schedule_item_id = [item for item in fee_schedule['FeeScheduleItems'] if item['FeeTypeId'] == 21582][0]['Id']
# #    fee_schedule_item_template_id = [i for i in fee_schedule['FeeScheduleTemplate']['FeeScheduleItemTemps'] if i['FeeTypeId'] == 21582][0]['Id']

    sysplanid = get_sysplanid(planid)
    fee_schedule_item_template_id = 195

    participant = {'FirstName': first_name,
                   'LastName': last_name,
                   'SSN': ssn}
    
    if not investprovid:
        investprovs = get_investment_providers_by_planid(planid)
        investprovid = [i for i in investprovs if i['IsPrimary'] is True][0]['Id']

    payload = {
        "PlanId": sysplanid,
        "DistributionReasonId": reason_id,
        "Participant": participant,
        "ProjectTemplateId": 77159,
        "InvestmentProviderLinkId": investprovid,
        "FeeScheduleItemTemplateId":fee_schedule_item_template_id
        #"FeeScheduleItemId": fee_schedule_item_id
    }

    r = requests.post(url, headers=headers, json=payload, params=params, timeout=15)
    
    if not r.ok:
        print(r.json())
   
    r.raise_for_status()
         
    data = r.json()
    return data

@retry()
def get_tasks(filters=None, expand=None, skip=None, top=None, orderby=None, get_all=False):

    params = set_params(filters=filters, expand=expand, skip=skip, top=top, orderby=orderby)

    url = f'{base}/tasks'
    r = requests.get(url, headers=headers, params=params, timeout=30)
    
    data = check(r)
    
    t = data['TotalPages']
    results = data['Values']
    
    if get_all and t>1:
    
        t = min(t, 15)

        for i in range(1, t):
            
            skip = {'$skip': i*1000}
            params.update(skip)
            
            r = requests.get(url, headers=headers, params=params, timeout=15)
            data2 = check(r)['Values']
            results.extend(data2)
            
    return results

@retry()
def get_teams(filters=None, skip=None, top=None, orderby=None):

    params = set_params(filters=filters, skip=skip, top=top, orderby=orderby)

    url = f'{base}/worktrays'
    r = requests.get(url, headers=headers, params=params, timeout=15)
    
    return check(r)['Values']


@retry()
def get_plan_contact_roles(filters=None, expand=None, skip=None, top=None, orderby=None):

    params = set_params(filters=filters, expand=expand, skip=skip, top=top, orderby=orderby)

    url = f'{base}/plancontactroles'
    r = requests.get(url, headers=headers, params=params, timeout=15)
    
    return check(r)['Values']

@retry()
def get_addresses(filters=None, expand=None, skip=None, top=None, orderby=None):

    params = set_params(filters=filters, expand=expand, skip=skip, top=top, orderby=orderby)

    url = f'{base}/addresses'
    r = requests.get(url, headers=headers, params=params, timeout=15)
    
    return check(r)['Values']

@retry()
def add_note(payload:dict):

    """
    {
      "PlanId": 0,
      "ClientId": 0,
      "ContactId": 0,
      "ProjectId": 0,
      "TaskId": 0,
      "ProspectId": 0,
      "ProposalId": 0,
      "OpportunityId": 0,
      "NoteText": "string",
      "ShowOnPSL": true,
      "IsImportant": true,
      "Archived": true,
      "CreatedByContactId": 0,
      "NoteCategoryId": 0,
      "HasBeenWarned": true,
      "IsDeactivated": true
    }

    """

    url = f'{base}/notes'
    r = requests.post(url, headers=headers, json=payload, timeout=15)
    
    return check(r)

@retry()
def get_project_by_projectid(projid:int, expand=None):

    params = set_params(expand=expand)

    url = f'{base}/projects/{projid}'
    r = requests.get(url, headers=headers, params=params, timeout=15)
    
    return check(r)

@retry()
def update_plan(payload:dict, select=None, expand=None):
    
    params = set_params(expand=expand, select=select)
    
    sysplanid = payload['Id']
    
    url = f'{base}/plans/{sysplanid}'
    r = requests.put(url, json=payload, params=params, headers=headers, timeout=15)
    
#    if not r.ok:
#        print(r.json())
#        r = requests.put(url, data=json.dumps(payload, separators=(',', ':')), params=params, headers=headers, timeout=15)
        
    return check(r)

@retry()
def update_project_file(payload:dict, expand=None):
    
    params = set_params(expand=expand)
    
    projfileid = payload['Id']
    
    url = f'{base}/projectfiles/{projfileid}'
    r = requests.put(url, json=payload, headers=headers, params=params, timeout=5)
    
    return check(r)

@retry()
def add_distribution_file(filepath:str, ProjectId:int, DistributionFileTypeId=1013, ShowOnWeb=True, Title=None, Comment='', Archived=False, HasBeenWarned=True):

    """    
    """

    if Title is None:
        Title = filepath

    payload = {
      "ProjectId": ProjectId,
      "DistributionFileTypeId": DistributionFileTypeId,
      "ShowOnWeb": ShowOnWeb,
      "Title": Title,
      "Comment": Comment,
      "Archived": Archived,
      "HasBeenWarned": HasBeenWarned
    }

    files = {
        
        'DistributionFile': (None, json.dumps(payload)),
        'file': (filepath , open(filepath, 'rb'), 'multipart/form-data')
   
    }

    url = f'{base}/distributionfiles'
    r = requests.post(url, files=files, headers=headers, verify=True)
    
    return check(r)

@retry()
def add_employee_plan_role(planid:str, ContactId:int, RoleTypeId:int, HasBeenWarned=False, IsDeactivated=False, filters=None, expand=None):

    """
    {
      "PlanId": 0,
      "ContactId": 0,
      "RoleTypeId": 0,
      "HasBeenWarned": true,
      "IsDeactivated": true
    }

    """
    params = set_params(filters=filters, expand=expand)

    sysplanid = get_sysplanid(planid)

    payload = {
        "PlanId": sysplanid,
        "ContactId": ContactId,
        "RoleTypeId": RoleTypeId,
        "HasBeenWarned": HasBeenWarned,
        "IsDeactivated": IsDeactivated
    }

    url = f'{base}/employeeplanroles'
    r = requests.post(url, headers=headers, json=payload, params=params, timeout=15)
    
    return check(r)

@retry()
def get_project_fields(filters:str, expand=None, skip=None, top=None, orderby=None):

    params = set_params(filters=filters, expand=expand, skip=skip, top=top, orderby=orderby)

    url = f'{base}/projectfields'
    r = requests.get(url, headers=headers, params=params, timeout=45)
    
    return check(r)['Values']

@retry()
def get_project_fields_by_planid(planid:str, filters=None, expand=None, skip=None, top=None, orderby=None):

    params = set_params(filters=filters, expand=expand, skip=skip, top=top, orderby=orderby)

    sysplanid = get_sysplanid(planid)

    url = f'{base}/plans/{sysplanid}/projectfields'
    r = requests.get(url, headers=headers, params=params, timeout=45)
    
    return check(r)['Values']

@retry()
def update_fee_schedule(payload:dict, expand=None):
    
    """
{
  "StartDate": "2020-08-13T19:25:29.550Z",
  "Description": "string",
  "EndDate": "2020-08-13T19:25:29.550Z",
  "FeeScheduleStatusId": 0,
  "DataKey": "string",
  "Id": 0,
  "HasBeenWarned": true,
  "IsDeactivated": true
}
    
    """    
    params = set_params(expand=expand)
    
    feeScheduleId = payload['Id']
    
    url = f'{base}/feeschedules/{feeScheduleId}'
    r = requests.put(url, json=payload, params=params, headers=headers, timeout=15)

    return check(r)

@retry()
def copy_fee_schedule_from_template(planid:str, StartDate:str, FeeScheduleTemplateId:int, FeeScheduleStatusId:int, Description=None, HasBeenWarned=False, IsDeactivated=False, filters=None, expand=None):

    """
{
  "StartDate": "2020-08-13T20:26:06.245Z",
  "Description": "string",
  "FeeScheduleTemplateId": 0,
  "PlanId": 0,
  "FeeScheduleStatusId": 0,
  "HasBeenWarned": true,
  "IsDeactivated": true
}

{
    "Standard Fee Schedule":43,
    "Smooth Start Fee Schedule":904,
    "Standard Fee Schedule - Solo Plan":2755,
    "PBA DC Fee Schedule":3032,
    "PBA Defined Benefit Fee Schedule":3033,
    "Standard Fee Schedule - MEP":3319   
}

{
    'Active':21605,
    'Closed':21606,
    'Edit':21607  
}

    """
    params = set_params(filters=filters, expand=expand)
 
    sysplanid = get_sysplanid(planid)

    payload = {
        "StartDate": StartDate,
        "Description": Description,
        "FeeScheduleTemplateId": FeeScheduleTemplateId,
        "PlanId": sysplanid,
        "FeeScheduleStatusId": FeeScheduleStatusId,
        "HasBeenWarned": HasBeenWarned,
        "IsDeactivated": IsDeactivated
    }

    url = f'{base}/feeschedules/copyfromtemplate'
    r = requests.post(url, headers=headers, json=payload, params=params, timeout=15)
    
    return check(r)

@retry()
def update_fee_schedule_item(payload:dict, expand=None):
    
    """
{
  "FeeAmount": 0,
  "FeeTypeId": 0,
  "FeeFrequencyId": 0,
  "FeePayorId": 0,
  "FeePaymentSourceId": 0,
  "AmountTypeId": 0,
  "CategoryId": 0,
  "DataKey": "string",
  "Id": 0,
  "HasBeenWarned": true,
  "IsDeactivated": true
}
    
    """    
    params = set_params(expand=expand)
    
    feeScheduleItemId = payload['Id']
    
    url = f'{base}/feescheduleitems/{feeScheduleItemId}'
    r = requests.put(url, json=payload, params=params, headers=headers, timeout=15)

    return check(r)

@retry()
def update_project(payload:dict, expand=None):
    
    params = set_params(expand=expand)
    
    projid = payload['Id']
    
    url = f'{base}/projects/{projid}'
    r = requests.put(url, json=payload, params=params, headers=headers, timeout=15)

#     if not r.ok:
#         print(r.json())
#         r = requests.put(url, data=json.dumps(payload, separators=(',', ':')), params=params, headers=headers, timeout=15)
   
    return check(r)

@retry()
def get_fee_schedule_templates(filters=None, expand=None, skip=None, top=None, orderby=None):

    params = set_params(filters=filters, expand=expand, skip=skip, top=top, orderby=orderby)

    url = f'{base}/feescheduletemplates'
    r = requests.get(url, headers=headers, params=params, timeout=15)
    
    return check(r)['Values']

@retry()
def get_plan_cycles_by_planid(planid:str, filters=None, expand=None, skip=None, top=None, orderby=None):

    params = set_params(filters=filters, expand=expand, skip=skip, top=top, orderby=orderby)

    sysplanid = get_sysplanid(planid)

    url = f'{base}/plans/{sysplanid}/plancycles'
    r = requests.get(url, headers=headers, params=params, timeout=15)
    
    return check(r)['Values']

@retry()
def update_plan_cycle(payload:dict, expand=None):
   
    params = set_params(expand=expand)
    
    planCycleId = payload['Id']
    
    url = f'{base}/plancycles/{planCycleId}'
    r = requests.put(url, json=payload, params=params, headers=headers, timeout=15)

    return check(r)

@retry()
def update_contact(payload:dict, expand=None):

    params = set_params(expand=expand)
    
    contactid = payload['Id']
    
    url = f'{base}/contacts/{contactid}'
    r = requests.put(url, json=payload, headers=headers, params=params, timeout=15)

    return check(r)

@retry()
def update_employee_plan_role(payload:dict, expand=None):
    
    '''
    {
      "PlanId": 0,
      "ContactId": 0,
      "RoleTypeId": 0,
      "DataKey": "string",
      "Id": 0,
      "HasBeenWarned": true,
      "IsDeactivated": true
    }
    '''
    
    params = set_params(expand=expand)

    employeePlanRoleId = payload['Id']
    
    url = f'{base}/employeeplanroles/{employeePlanRoleId}'
    r = requests.put(url, json=payload, params=params, headers=headers, timeout=15)

    return check(r)

@retry()
def delete_employee_plan_role(employeePlanRoleId:int):

    url = f'{base}/employeeplanroles/{employeePlanRoleId}'
    r = requests.delete(url, headers=headers, timeout=15)
    
    return check(r)

@retry()
def get_active_fee_schedule_by_planid(planid, expand=None, skip=None, top=None, orderby=None):

    filters = filters='FeeScheduleStatus.DisplayName eq "Active"'
    fee_schedules = get_fee_schedules_by_planid(planid=planid, filters=filters, expand=expand, skip=skip, top=top, orderby=orderby)
    lenfs = len(fee_schedules)

    if lenfs == 1:
        return fee_schedules[0]
    
    filters = f"(EndDate eq null or EndDate gt '{today}') and FeeScheduleStatusId ne 21606"
    fee_schedules = get_fee_schedules_by_planid(planid=planid, filters=filters, expand=expand, skip=skip, top=top, orderby=orderby)
    
    if len(fee_schedules) > 1:
        fee_schedules = [fs for fs in fee_schedules if pd.to_datetime(fs['StartDate']) <= pd.to_datetime(today)]
        
    if len(fee_schedules) == 1:
        return fee_schedules[0]
    else:
        return {}

@retry()
def get_employees(filters=None, expand=None, top=None, orderby=None, skip=None):

    params = set_params(filters=filters, expand=expand, skip=skip, top=top, orderby=orderby)

    url = f'{base}/employees'
    r = requests.get(url, headers=headers, params=params, timeout=15)
    
    return check(r)

@retry()
def delete_plan(planid, sysplanid:bool=False):

    if not sysplanid:
        planid = get_sysplanid(planid)
    
    url = f'{base}/plans/{planid}'
    r = requests.delete(url, headers=headers, timeout=15)
    
    return check(r)

@retry()
def get_time_entries(filters=None, expand=None, top=None, orderby=None, skip=None):

    params = set_params(filters=filters, expand=expand, skip=skip, top=top, orderby=orderby)

    url = f'{base}/timeEntries'
    r = requests.get(url, headers=headers, params=params, timeout=15)
    
    return check(r)['Values']

@retry()
def override_task(taskid:int):
    
    url = f'{base}/tasks/{taskid}/overrideTask'
    r = requests.put(url, headers=headers, timeout=5)
    
    return check(r)


def get_woktray(name):
    
    print('\U0001F373')
    

def get_worktray(name:str, get_all:bool=False, addfilt:str=None, n:int=75, dashboard:bool=False, simp:bool=False, dtf:bool=False, orderby=None, top=None, skip=None):

    if dashboard:
        
        first_name, last_name = name.split(' ', maxsplit=1)
        
        contact = get_contacts(filters=f'FirstName eq "{first_name}" and LastName eq "{last_name}"')[0]
        contactid = contact['Id']

        ifilt = f"AssignedToId eq {contactid}"

    else:
        
        team = get_teams(filters=f"Name eq '{name}'")[0]
        teamid = team['Id']

        ifilt = f"TeamId eq {teamid}"
        
    filters = f"{ifilt} and TaskActive ne null and DateCompleted eq null and TaskGroup.Project.CompletedOn eq null and (TaskGroup.Project.StartDate le '{today}' or TaskGroup.Project.StartDate eq null)"    
    expand = 'TaskGroup.Project'
    
    if addfilt:
        filters = f'({filters}) and ({addfilt})'
    
    tasks = get_tasks(filters=filters, expand=expand, get_all=get_all, orderby=orderby, top=top, skip=skip)

    projids = [task['TaskGroup']['ProjectId'] for task in tasks]
    projids = list(dict.fromkeys(projids))
    
    a = [[task['TaskGroup']['ProjectId'],
          task['Id'],
          task['TaskName'],
          task['TaskActive'],
          task['DaysToComp'],
          task['Rejected'],
          task['Rejections']] for task in tasks]
    
    df1 = pd.DataFrame(a, columns=['projid', 'taskid', 'task_name', 'task_active', 'daystocomp', 'rej', 'rejs'])

    c = -(-(len(projids)) // n)
    
    projs = []
    for i in range(c):
        projidsa = projids[i*n:(i+1)*n]
        filters = ' or '.join([f'ProjectId eq {projid}' for projid in projidsa])
        expand = 'Plan.Status,Plan.PlanType,Plan.PlanCategory,Plan.FilingStatus,Plan.PBGC,Plan.PlanGroup,Plan.AdminType,Plan.InvestmentProviderLinks'
        projsa = get_projects(filters=filters, expand=expand)
        projs.extend(projsa)
    
    a = [[proj['Id'],
          proj['Plan']['InternalPlanId'],
          proj['Plan']['Name'],
          proj['Name'],
          proj['Plan']['Status']['DisplayName'],
          proj['Plan']['PlanCategory']['DisplayName'],
          proj['Plan']['PlanType']['DisplayName'],
          proj['Plan']['FilingStatus']['DisplayName'],
          f"{proj['Plan']['MonthEnd']}/{proj['Plan']['DayEnd']}",
          proj['Plan']['EffectiveOn'],
          proj['Plan']['PBGC']['DisplayName'],
          proj['DueOn'],
          proj['PeriodStart'],
          proj['PeriodEnd'],
          proj['Plan']['PlanGroup']['DisplayName'],
          proj['Plan']['AdminType']['DisplayName'],
          proj['PriorityLevelId'],
          proj['Description'],
          next((ip for ip in proj['Plan']['InvestmentProviderLinks'] if ip.get('IsPrimary')), {}).get('InvestmentProviderId', pd.NA),
          next((ip for ip in proj['Plan']['InvestmentProviderLinks'] if ip.get('IsPrimary')), {}).get('EffectiveOn', pd.NA),
          proj['Plan']['AddedOn']] for proj in projs]

    cols = ['projid', 'planid', 'plan_name', 'proj_name', 'plan_status', 'plan_category', 'plan_type', 'form5500', 'plan_end', 'eff_on', 'pbgc', 'proj_due_on', 'per_start', 'per_end', 'plan_group', 'admin_type', 'priority', 'desc', 'ipid', 'ip_eff_on', 'add_on']

    df2 = pd.DataFrame(a, columns=cols)

    dfw = df1.merge(df2, on='projid')

    if dtf:
        dtfcols = ['task_active', 'eff_on', 'proj_due_on', 'per_start', 'per_end', 'ip_eff_on', 'add_on']
        for col in dtfcols:
            dfw[col] = pd.to_datetime(dfw[col], format='%m/%d/%Y %I:%M:%S %p')
        dfw.per_end = dfw.per_end.dt.normalize()
        dfw.per_start = dfw.per_start.dt.normalize()
    
    if simp:
        simpcols = ['planid', 'plan_name', 'projid', 'proj_name', 'taskid', 'task_name']
        simpcols+=simp if isinstance(simp,list) else []
        dfw = dfw[simpcols].copy()
    
    return dfw

get_worktray2 = get_worktray
get_dashboard = functools.partial(get_worktray, dashboard=True)



@retry()
def get_distribution_by_id(projectId:int, expand=None):

    params = set_params(expand=expand)

    url = f'{base}/distributions/{projectId}'
    r = requests.get(url, headers=headers, params=params, timeout=15)
    
    return check(r)

@retry()
def update_distribution(payload:dict, expand=None):
      
    params = set_params(expand=expand)
    
    projectId = payload['Id']
    
    url = f'{base}/distributions/{projectId}'
    r = requests.put(url, json=payload, params=params, headers=headers, timeout=15)

    return check(r)

@retry()
def update_note(payload:dict, expand=None):
    
    """
{
  "PlanId": 0,
  "ClientId": 0,
  "ContactId": 0,
  "ProjectId": 0,
  "TaskId": 0,
  "ProspectId": 0,
  "ProposalId": 0,
  "OpportunityId": 0,
  "NoteText": "string",
  "ShowOnPSL": true,
  "IsImportant": true,
  "CanEdit": true,
  "CanDelete": true,
  "Archived": true,
  "CreatedByContactId": 0,
  "NoteCategoryId": 0,
  "Category": "{ DisplayName,  Description,  DataKey,  Id,  HasBeenWarned,  CreatedOn,  UpdatedOn,  CreatedByContact,  UpdatedByContact,  IsDeactivated }",
  "DataKey": "string",
  "Id": 0,
  "HasBeenWarned": true,
  "CreatedOn": "2022-12-20T20:14:16.770Z",
  "UpdatedOn": "2022-12-20T20:14:16.770Z",
  "CreatedByContact": "{ ContactId,  FirstName,  LastName,  Email }",
  "UpdatedByContact": "{ ContactId,  FirstName,  LastName,  Email }",
  "IsDeactivated": true
}
    
    """
    
    params = set_params(expand=expand)
    
    noteid = payload['Id']
    
    url = f'{base}/notes/{noteid}'
    r = requests.put(url, json=payload, params=params, headers=headers, timeout=15)

    return check(r)

@retry()
def add_investment_provider(payload:dict, filters=None, expand=None):

    """
    {
      "EffectiveOn": "2023-01-11T18:26:59.364Z",
      "ProviderAccountId": "string",
      "ProviderAccountName": "string",
      "TerminatedOn": "2023-01-11T18:26:59.364Z",
      "BasisPointReimbursement": 0,
      "IsPrimary": true,
      "PlanId": 0,
      "InvestmentProviderId": 0,
      "InvestmentDirectionId": 0,
      "InvestmentModelId": 0,
      "DataCollectionSourceId": 0,
      "ResponsibilityId": 0,
      "DistributionProcessorId": 0,
      "RevenueSharingTypeId": 0,
      "FeeDisclosureTypeId": 0,
      "TransferStatusId": 0,
      "VestingSubmissionId": 0,
      "HasBeenWarned": true,
      "IsDeactivated": true
    }

    """

    params = set_params(filters=filters, expand=expand)

    url = f'{base}/investmentproviderlink'
    r = requests.post(url, headers=headers, json=payload, params=params, timeout=15)
    
    return check(r)

@retry()
def update_investment_provider(payload:dict, expand=None):
    
    """

    *link
    
{
  "IsTerminated": true,
  "EffectiveOn": "2023-01-20T18:04:04.044Z",
  "ProviderAccountId": "string",
  "ProviderAccountName": "string",
  "TerminatedOn": "2023-01-20T18:04:04.044Z",
  "BasisPointReimbursement": 0,
  "IsPrimary": true,
  "PlanId": 0,
  "InvestmentProviderId": 0,
  "InvestmentDirectionId": 0,
  "InvestmentModelId": 0,
  "DataCollectionSourceId": 0,
  "ResponsibilityId": 0,
  "DistributionProcessorId": 0,
  "RevenueSharingTypeId": 0,
  "FeeDisclosureTypeId": 0,
  "TransferStatusId": 0,
  "VestingSubmissionId": 0,
  "DataKey": "string",
  "Id": 0,
  "HasBeenWarned": true,
  "IsDeactivated": true
}
    
    """
    
    params = set_params(expand=expand)
    
    iplinkid = payload['Id']
    
    url = f'{base}/investmentproviderlink/{iplinkid}'
    r = requests.put(url, json=payload, params=params, headers=headers, timeout=15)
  
    return check(r)

@retry()
def get_plan_notes_by_planid(planid:str, filters=None, expand=None):

    params = set_params(filters=filters, expand=expand)

    sysplanid = get_sysplanid(planid)

    url = f'{base}/plans/{sysplanid}/notes'
    r = requests.get(url, headers=headers, params=params, timeout=15)
    
    return check(r)

@retry()
def get_project_fields_by_projectid(projectId:int, filters=None, expand=None, skip=None, top=None, orderby=None):
    
    params = set_params(filters=filters, expand=expand, skip=skip, top=top, orderby=orderby)
    
    url = f'{base}/projects/{projectId}/projectfields'
    r = requests.get(url, headers=headers, params=params, timeout=45)
    
    return check(r)['Values']

@retry()
def add_interaction(payload:dict, filters=None, expand=None):

    """
{
  "InteractionDate": "2023-05-15T14:18:55.833Z",
  "InteractionTypeId": 0,
  "Title": "string",
  "Details": "string",
  "IsHTML": true,
  "Participants": "{ ContactId,  InteractionRoleId,  HasBeenWarned,  IsDeactivated }",
  "ProposalInteractionLinks": "{ ProposalId,  HasBeenWarned,  IsDeactivated }",
  "ProspectInteractionLinks": "{ ProspectId,  HasBeenWarned,  IsDeactivated }",
  "PlanInteractionLinks": "{ PlanId,  HasBeenWarned,  IsDeactivated }",
  "OpportunityInteractionLinks": "{ OpportunityId,  HasBeenWarned,  IsDeactivated }",
  "HasBeenWarned": true,
  "IsDeactivated": true
}
    """
    params = set_params(filters=filters, expand=expand)

    url = f'{base}/interactions'
    r = requests.post(url, headers=headers, json=payload, params=params, timeout=15)
    
    return check(r)

@retry()
def get_participants_by_projectid(projid:int, filters=None, expand=None):

    params = set_params(filters=filters, expand=expand)

    url = f'{base}/projects/{projid}/participants'
    r = requests.get(url, headers=headers, params=params, timeout=15)
    
    return check(r)

@retry()
def add_time_entry(payload:dict, filters=None, expand=None):

    """
{
  "TimeCodeId": 0,
  "ContactId": 0,
  "PlanId": 0,
  "ProjectId": 0,
  "TaskGroupId": 0,
  "HoursWorked": 0,
  "WorkDate": "2023-05-16T17:17:08.778Z",
  "Description": "string",
  "IsManagerApproved": true,
  "TaskId": 0,
  "CreationDate": "2023-05-16T17:17:08.778Z",
  "IsReviewedForBiling": true,
  "IsBillable": true,
  "PlanCycleId": 0,
  "HasBeenWarned": true,
  "IsDeactivated": true
}
    """
    
    params = set_params(filters=filters, expand=expand)

    url = f'{base}/timeEntries'
    r = requests.post(url, headers=headers, json=payload, params=params, timeout=15)
    
    return check(r)

@retry()
def delete_time_entry(timeEntryId:int):

    url = f'{base}/timeEntries/{timeEntryId}'
    r = requests.delete(url, headers=headers, timeout=15)

    return check(r)


@retry()
def add_plan_contact_right(planid:str, contactid:int, rightid:int, HasBeenWarned=False, IsDeactivated=False, filters=None, expand=None):

    """
{
  "PlanId": 0,
  "ContactId": 0,
  "RightId": 0,
  "HasBeenWarned": true,
  "IsDeactivated": true
}

    """
    
    params = set_params(filters=filters, expand=expand)

    sysplanid = get_sysplanid(planid)

    payload = {"PlanId": sysplanid,
               "ContactId": contactid,
               "RightId": rightid,
               "HasBeenWarned": HasBeenWarned,
               "IsDeactivated": IsDeactivated
              }

    url = f'{base}/plancontactright'
    r = requests.post(url, headers=headers, json=payload, params=params, timeout=15)
    
    return check(r)

@retry()
def get_contact_notes_by_contactid(contactid:int, filters=None, expand=None):

    params = set_params(filters=filters, expand=expand)

    url = f'{base}/contacts/{contactid}/notes'
    r = requests.get(url, headers=headers, params=params, timeout=15)
    
    return check(r)['Values']

@retry()
def get_project_notes_by_projectid(projid:int, filters=None, expand=None):

    params = set_params(filters=filters, expand=expand)

    url = f'{base}/projects/{projid}/notes'
    r = requests.get(url, headers=headers, params=params, timeout=15)
    
    return check(r)['Values']



@retry()
def get_employee_plan_roles(filters=None, expand=None, top=None, orderby=None, skip=None, select=None, get_all=False):

    params = set_params(filters=filters, expand=expand, skip=skip, top=top, orderby=orderby, select=select)

    url = f'{base}/employeeplanroles'
    r = requests.get(url, headers=headers, params=params, timeout=15)

    data = check(r)

    if get_all:
            
        totalpages = data['TotalPages']
    
        if totalpages > 1:
            results = []
            results.extend(data['Values'])            
            
            for i in range(1, totalpages):
                params['$skip'] = i*1000
                r = requests.get(url, headers=headers, params=params, timeout=15)
                data = check(r)['Values']
                results.extend(data)
            return results

    return data['Values']


@retry()
def get_entity_types(filters=None, expand=None, top=None, orderby=None, skip=None):

    params = set_params(filters=filters, expand=expand, skip=skip, top=top, orderby=orderby)

    url = f'{base}/employerdata/entitytypes'
    r = requests.get(url, headers=headers, params=params, timeout=15)
    
    return check(r)['Values']



@retry()
def add_plan_contact_role(planid:str, ContactId:int, RoleTypeId:int, HasBeenWarned=False, IsDeactivated=False, ShouldCC=False, ShouldBCC=False, ShowOnWeb=True, filters=None, expand=None):

    """
    {
      "ShouldCC": true,
      "ShouldBCC": true,
      "ShowOnWeb": true,
      "PlanId": 0,
      "ContactId": 0,
      "RoleTypeId": 0,
      "HasBeenWarned": true,
      "IsDeactivated": true
    }

    """
    params = set_params(filters=filters, expand=expand)

    sysplanid = get_sysplanid(planid)

    payload = {
        "ShouldCC": ShouldCC,
        "ShouldBCC": ShouldBCC,
        "ShowOnWeb": ShowOnWeb,
        "PlanId": sysplanid,
        "ContactId": ContactId,
        "RoleTypeId": RoleTypeId,
        "HasBeenWarned": HasBeenWarned,
        "IsDeactivated": IsDeactivated
    }

    url = f'{base}/plancontactroles'
    r = requests.post(url, headers=headers, json=payload, params=params, timeout=15)
    
    return check(r)


def close_project(projid:int):
    
    taskids = []
    
    tgs = get_task_groups_by_projectid(projid, expand='Tasks', orderby='Order')
    
    for tg in tgs:
        
        if tg['DateCompleted']:
            
            continue
    
        for task in sorted(tg['Tasks'], key=lambda x: x['Order']):
            
            if task['DateCompleted']:
                
                continue
                
            taskid = task['Id']
            x = override_task(taskid)
            taskids.append(taskid)
            
    return taskids


@retry()
def reassign_task(taskid:int, worktray:int=None, employee:int=None):

    r = []
    
    if worktray:
        if not isinstance(worktray, int):
            team = get_teams(filters=f"Name eq '{worktray}'")[0]
            worktray = team['Id']
        url = f'{base}/tasks/{taskid}/{worktray}'
        r1 = requests.put(url, headers=headers, timeout=15)
        r.append(check(r1))
    
    if employee:
        if not isinstance(employee, int):
            first_name, last_name = employee.split(' ', maxsplit=1)
            contact = get_contacts(filters=f'FirstName eq "{first_name}" and LastName eq "{last_name}"')[0]
            employee = contact['Id']
        url = f'{base}/tasks/employee/{taskid}/{employee}'
        r2 = requests.put(url, headers=headers, timeout=15)
        r.append(check(r2))

    if not(worktray or employee):
        raise ValueError('must provide worktray or employee to reassign to')
    
    return r

@retry()
def abort_project(projid:int, note:str, expand=None):
    
    params = set_params(expand=expand)

    payload = {'ProjectId': projid,
               'NoteText': note}
        
    url = f'{base}/projects/{projid}/abort'
    r = requests.put(url, json=payload, headers=headers, params=params, timeout=15)
    
    return check(r)



__version__ = '1.2 (5-7-24)'

__changelog__ = '''


'''





    
if __name__ == "__main__":
    print('Main')
