import datetime as dt
import json
import time
import functools
import pandas as pd

import requests

today = dt.date.today().strftime('%m-%d-%Y')

headers = {'username': 'Kiandre', 'apikey': 'UeT70u9Kfo6814FFF8Cppw6A'}#put API key from PensionPro here

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
        
        msg = ' | '.join([rjson.get('Message')]+[f"Rule: {rule.get('Rule')}" for rule in rjson.get('Rules', [])]) if isinstance(rjson, dict) else rjson

        if 'Rule Violation' in msg:
            raise APIRuleViolation(msg)

        if 'OData' in msg:
            raise APIODataError(msg)

        if r.status_code == 429:
            raise APIRateLimit(msg)
        
        raise APIError(msg, response=r)
    
    return rjson

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
def get_sysplanid(planid):
    
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
def get_plan_by_planid(internal_planid):

    params = {}
        
    url = f'{base}/plans/{internal_planid}'

    r = requests.get(url, headers=headers, params=params, timeout=15)
    
    if not r.ok:
        print(r.json())
        

    data = check(r)

    return data

@retry()
def get_plan_by_tpaplanid(planid):

    params = {}
    internal_planid = get_sysplanid(planid)
    url = f'{base}/plans/{internal_planid}'

    r = requests.get(url, headers=headers, params=params, timeout=15)
    
    if not r.ok:
        print(r.json())
        

    data = check(r)

    return data

@retry()
def add_search_text(payload, internal_planid):
    """
        {
          "Name": "string",
          "AddedOn": "",
          "ClientId": int,
          "PlanTypeId": int,
          "AdminTypeId": int,
          "EffectiveOn": "",
          "DistributionsOnPSLId": int
          "DataKey": "string"
        }
    """

    params = {}

    url = f'{base}/plans/{internal_planid}'

    r = requests.put(url,json=payload, params=params, headers=headers, timeout=15)
    
    if not r.ok:
        print(r.json())
        

    data = check(r)

    return data 

@retry()
def get_services_provided_by_planid(internal_planid, filters=None, expand=None, skip=None, top=None):

    params = {}
        
    if filters:
        params.update({'$filter':filters})

    if expand:
        params.update({'$expand':expand})

    if skip:
        params.update({'$skip':skip})
                
    if top:
        params.update({'$top':top})


    url = f'{base}/plans/{internal_planid}/planServicesProvidedLinks'

    r = requests.get(url, headers=headers, params=params, timeout=15)
    
    if not r.ok:
        print(r.json())
        

    data = check(r)['Values']

    return data

@retry()
def add_services_provided_by_planid(payload, internal_planid):

    """
       {
          "Description": "string",
          "PlanId": 0,
          "ProvidedServiceId": 0,
          "HasBeenWarned": true,
          "IsDeactivated": true
       }

    """

    url = f'{base}/planServicesProvidedLinks'

    r = requests.post(url, headers=headers, json=payload, timeout=15)
    
    if not r.ok:
        print(r.json())
        

    data = check(r)

    return data 

@retry()
def get_project_template_by_name(Name):

    params = {'$filter':f'Name eq "{Name}"'}

    url = f'{base}/projecttemplates'

    r = requests.get(url, headers=headers, params=params, timeout=15)

    data = check(r)['Values']

    return data



@retry()
def add_project(planid, ProjectTemplateId, StartDate=None, DueOn=None, PeriodStart=None, PeriodEnd=None, Description='', EnableToShowOnPSLProjectsTab=False, filters=None, expand=None):

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
    
    params={}
    
    if filters:
        params.update({'$filter':filters})

    if expand:
        params.update({'$expand':expand})

    url = f'{base}/projects'
 
    sysplanid = get_sysplanid(planid)

    payload = {
        "PlanId": sysplanid,
        "ProjectTemplateId": ProjectTemplateId,
        "StartDate": StartDate,
        "DueOn": DueOn,
        "PeriodStart":PeriodStart,
        "PeriodEnd":PeriodEnd,
        "Description":Description,
        "EnableToShowOnPSLProjectsTab":EnableToShowOnPSLProjectsTab
    }

    r = requests.post(url, headers=headers, json=payload, params=params, timeout=15)
    
    if not r.ok:
        print(r.json())
   
    r.raise_for_status()
         
    data = check(r)
    return data

@retry()                       
def override_task(taskid):         # Added by Lam on 1/6/2022

    url = 'https://api.pensionpro.com/v1/tasks/{}/overrideTask'.format(taskid)
    
    r = requests.put(url, headers=headers, timeout=5)
    
    if not r.ok:
        print(r.json())
   
        
    data = check(r)
    return data

@retry()
def get_task_groups_by_projectid(projid, filters=None, expand=None, skip=None, top=None, orderby=None):

    params = set_params(filters=filters, expand=expand, skip=skip, top=top, orderby=orderby)

    url = f'{base}/projects/{projid}/taskgroups'
    r = requests.get(url, headers=headers, params=params, timeout=15)
    data = check(r)['Values']
    return data

@retry()
def get_teams(filters=None, skip=None, top=None, orderby=None):

    params = set_params(filters=filters, skip=skip, top=top, orderby=orderby)

    url = f'{base}/worktrays'
    r = requests.get(url, headers=headers, params=params, timeout=15)

    data = check(r)['Values']
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
    
        t = min(t, 10)

        for i in range(1, t):
            
            skip = {'$skip': i*1000}
            params.update(skip)
            
            r = requests.get(url, headers=headers, params=params, timeout=15)
            
            data2 = check(r)['Values']
            results.extend(data2)
            
    return results

@retry()
def get_projects(filters=None, expand=None, skip=None, top=None, orderby=None, get_all=False):

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

def get_worktray2(name, get_all=False, addfilt=None, n=75):
    
    team = get_teams(filters=f"Name eq '{name}'")[0]
    teamid = team['Id']
    
    filters = f"TeamId eq {teamid} and TaskActive ne null and DateCompleted eq null and TaskGroup.Project.CompletedOn eq null and (TaskGroup.Project.StartDate le '{today}' or TaskGroup.Project.StartDate eq null)"    
    expand = 'TaskGroup.Project'
    
    if addfilt:
        filters = f'{filters} and {addfilt}'
    
    tasks = get_tasks(filters=filters, expand=expand, get_all=get_all)

    projids = [task['TaskGroup']['ProjectId'] for task in tasks]
    projids = list(dict.fromkeys(projids))
    
    a = [[task['TaskGroup']['ProjectId'],
          task['Id'],
          task['TaskName']] for task in tasks]
    
    df1 = pd.DataFrame(a, columns=['projid', 'taskid', 'task_name'])

    c = -(-(len(projids)) // n)
    
    projs = []
    for i in range(c):
        projidsa = projids[i*n:(i+1)*n]
        filters = ' or '.join([f'ProjectId eq {projid}' for projid in projidsa])
        expand = 'Plan.Status,Plan.PlanType,Plan.PlanCategory,Plan.FilingStatus,Plan.PBGC'
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
          proj['DueOn']] for proj in projs]

    cols = ['projid', 'planid', 'plan_name', 'proj_name', 'plan_status', 'plan_category', 'plan_type', 'form5500', 'plan_end', 'effective_on', 'pbgc', 'proj_due_on']

    df2 = pd.DataFrame(a, columns=cols)
    dfw = df1.merge(df2, on='projid')
    
    return dfw

def get_worktray(name, get_all=False, addfilt=None, n=75):
    
    team = get_teams(filters=f"Name eq '{name}'")[0]
    teamid = team['Id']
    
    filters = f"TeamId eq {teamid} and TaskActive ne null and DateCompleted eq null and TaskGroup.Project.CompletedOn eq null and (TaskGroup.Project.StartDate le '{today}' or TaskGroup.Project.StartDate eq null)"    
    expand = 'TaskGroup.Project'
    
    if addfilt:
        filters = f'{filters} and {addfilt}'
    
    tasks = get_tasks(filters=filters, expand=expand, get_all=get_all)

    projids = [task['TaskGroup']['ProjectId'] for task in tasks]
    projids = list(dict.fromkeys(projids))
    
    a = [[task['TaskGroup']['ProjectId'],
          task['Id'],
          task['TaskName'],
         task['TaskActive'],
         task['DaysToComp']] for task in tasks]
    
    df1 = pd.DataFrame(a, columns=['projid', 'taskid', 'task_name', 'task_active', 'daystocomp'])

    c = -(-(len(projids)) // n)
    
    projs = []
    for i in range(c):
        projidsa = projids[i*n:(i+1)*n]
        filters = ' or '.join([f'ProjectId eq {projid}' for projid in projidsa])
        expand = 'Plan.Status,Plan.PlanType,Plan.PlanCategory,Plan.FilingStatus,Plan.PBGC,Plan.PlanGroup,Plan.AdminType'
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
          proj['PriorityLevelId']] for proj in projs]

    cols = ['projid', 'planid', 'plan_name', 'proj_name', 'plan_status', 'plan_category', 'plan_type', 'form5500', 'plan_end', 'effective_on', 'pbgc', 'proj_due_on', 'per_start', 'per_end', 'plan_group', 'admin_type', 'priority']

    df2 = pd.DataFrame(a, columns=cols)

    dfw = df1.merge(df2, on='projid')
    
    return dfw

def get_worktray3(name, get_all=False, addfilt=None, n=75):
    
    team = get_teams(filters=f"Name eq '{name}'")[0]
    teamid = team['Id']
    
    filters = f"TeamId eq {teamid} and TaskActive ne null and DateCompleted eq null and TaskGroup.Project.CompletedOn eq null and (TaskGroup.Project.StartDate le '{today}' or TaskGroup.Project.StartDate eq null)"    
    expand = 'TaskGroup.Project'
    if addfilt:
        filters = f'{filters} and {addfilt}'
    
    tasks = get_tasks(filters=filters, expand=expand, get_all=get_all)

    projids = [task['TaskGroup']['ProjectId'] for task in tasks]
    projids = list(dict.fromkeys(projids))
    
    a = [[task['TaskGroup']['ProjectId'],
          task['Id'],
          task['TaskName'],
         task['TaskActive'],
         task['DaysToComp']] for task in tasks]
    
    df1 = pd.DataFrame(a, columns=['projid', 'taskid', 'task_name', 'task_active', 'daystocomp'])

    c = -(-(len(projids)) // n)
    
    projs = []
    for i in range(c):
        projidsa = projids[i*n:(i+1)*n]
        filters = ' or '.join([f'ProjectId eq {projid}' for projid in projidsa])
        expand = 'Plan.Status,Plan.PlanType,Plan.PlanCategory,Plan.FilingStatus,Plan.PBGC'
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
          proj['DueOn']] for proj in projs]

    cols = ['projid', 'planid', 'plan_name', 'proj_name', 'plan_status', 'plan_category', 'plan_type', 'form5500', 'plan_end', 'effective_on', 'pbgc', 'proj_due_on']

    df2 = pd.DataFrame(a, columns=cols)

    dfw = df1.merge(df2, on='projid')
    
    return dfw


@retry()
def get_project_fields_by_planid(planid, filters=None, expand=None, skip=None, top=None, orderby=None):

    params = set_params(filters=filters, expand=expand, skip=skip, top=top, orderby=orderby)

    sysplanid = get_sysplanid(planid)

    url = f'{base}/plans/{sysplanid}/projectfields'
    r = requests.get(url, headers=headers, params=params, timeout=15)
    
    return check(r)['Values']

@retry()
def get_project_fields_by_internalplanid(sysplanid, filters=None, expand=None, skip=None, top=None, orderby=None):

    params = set_params(filters=filters, expand=expand, skip=skip, top=top, orderby=orderby)

    url = f'{base}/plans/{sysplanid}/projectfields'
    r = requests.get(url, headers=headers, params=params, timeout=15)
    
    return check(r)['Values']

@retry()
def get_projects_by_planid(planid, filters=None, expand=None, skip=None, top=None, orderby=None):

    params = set_params(filters=filters, expand=expand, skip=skip, top=top, orderby=orderby)
        
    sysplanid = get_sysplanid(planid)

    url = f'{base}/plans/{sysplanid}/projects'
    r = requests.get(url, headers=headers, params=params, timeout=15)
    
    return check(r)['Values']

@retry()
def get_project_fields_by_projectid(projectId, filters=None, expand=None, skip=None, top=None, orderby=None):
    
    params = set_params(filters=filters, expand=expand, skip=skip, top=top, orderby=orderby)
    
    url = f'{base}/projects/{projectId}/projectfields'
    r = requests.get(url, headers=headers, params=params, timeout=15)

    data = check(r)['Values']
    return data

@retry()
def get_task_groups_by_projectid(projid, filters=None, expand=None, skip=None, top=None, orderby=None):

    params = set_params(filters=filters, expand=expand, skip=skip, top=top, orderby=orderby)

    url = f'{base}/projects/{projid}/taskgroups'
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
def add_note(payload):

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
    
    if not r.ok:
        print(r.json())
        

    data = check(r)

    return data

@retry()
def delete_note_by_note_id(note_id):
    url = f'{base}/notes/{note_id}'
    r = requests.delete(url, headers=headers, timeout=15)
    if not r.ok:
        print(r.json())
        


@retry()
def get_plan_notes_by_planid(planid, filters=None, expand=None):

    params = set_params(filters=filters, expand=expand)

    sysplanid = get_sysplanid(planid)

    url = f'{base}/plans/{sysplanid}/notes'
    r = requests.get(url, headers=headers, params=params, timeout=15)
    
    if not r.ok:
        print(r.json())
        

    data = check(r)

    return data['Values']

@retry()
def get_project_notes_by_projectid(projectid, filters=None, expand=None):

    params = set_params(filters=filters, expand=expand)
    
    url = f'{base}/projects/{projectid}/notes'
    r = requests.get(url, headers=headers, params=params, timeout=15)
    
    if not r.ok:
        print(r.json())
        

    data = check(r)

    return data['Values']

@retry()
def get_employees(filters=None, expand=None, skip=None, top=None, orderby=None, get_all=False):

    params = {}
   
    if filters:
        params.update({'$filter':filters})

    if expand:
        params.update({'$expand':expand})

    if skip:
        params.update({'$skip':skip})
                
    if top:
        params.update({'$top':top})

    if orderby:
        params.update({'$orderby':orderby})
      
    
    url = 'https://api.pensionpro.com/v1/employees'
        
    r = requests.get(url, headers=headers, params=params, timeout=30)
    
    data = check(r)
    
    if get_all:
            
        totalpages = data['TotalPages']
    
        if totalpages > 1:
            results = []
            results.extend(data['Values'])            
            
            for i in range(1, totalpages):
                skip = {'$skip': i*200}
                params.update(skip)
                r = requests.get(url, headers=headers, params=params, timeout=15)
                data = check(r)['Values']
                results.extend(data)

            return results

    return data['Values']

@retry()
def get_plan_contact_roles_by_planid(planid, filters=None, select=None, expand='Contact,RoleType'):

    params={}

    if filters:
        params.update({'$filter':filters})

    if select:
        params.update({'$select':select})

    if expand:
        params.update({'$expand':expand})
    
    sysplanid = get_sysplanid(planid)

    url = 'https://api.pensionpro.com/v1/plans/{}/plancontactroles'.format(sysplanid)

    r = requests.get(url, headers=headers, params=params, timeout=15)

    data = check(r)['Values']

    return data

@retry()
def get_employee_plan_roles_by_planid(planid, filters=None, select=None, expand='Contact,RoleType'):

    params={}

    if filters:
        params.update({'$filter':filters})

    if select:
        params.update({'$select':select})

    if expand:
        params.update({'$expand':expand})
    
    sysplanid = get_sysplanid(planid)

    url = f'https://api.pensionpro.com/v1/plans/{sysplanid}/employeeplanroles'

    r = requests.get(url, headers=headers, params=params, timeout=15)
    
    if not r.ok:
        print(r.json())
    

    data = check(r)['Values']

    return data

@retry()
def add_interaction(payload, filters=None, expand=None):

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
def uncomplete_task(taskid):
    
    url = 'https://api.pensionpro.com/v1/tasks/{}/uncompletetask'.format(taskid)
    
    r = requests.put(url, headers=headers, timeout=5)
    
       
    data = check(r)
    return data

@retry()
def add_time_entry(payload, filters=None, expand=None):

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

    params={}
    
    if filters:
        params.update({'$filter':filters})

    if expand:
        params.update({'$expand':expand})

    url = f'{base}/timeEntries'

    r = requests.post(url, headers=headers, json=payload, params=params, timeout=15)
    
    if not r.ok:
        print(r.json())
   
         
    data = check(r)
    return data


@retry()
def delete_time_entry(timeEntryId):

    url = f'{base}/timeEntries/{timeEntryId}'
    
    r = requests.delete(url, headers=headers, timeout=15)
    
    if not r.ok:
        print(r.json())
    
           
    return r.status_code == 204 

@retry()
def get_contacts(filters=None, select=None, expand=None, top=None, orderby=None, skip=None):

    params = {}

    if filters:
        params.update({'$filter':filters})

    if select:
        params.update({'$select':select})

    if expand:
        params.update({'$expand':expand})

    if top:
        params.update({'$top':top})

    if orderby:
        params.update({'$orderby':orderby})

    if skip:
        params.update({'$skip':skip})


    url = 'https://api.pensionpro.com/v1/contacts'

    r = requests.get(url, headers=headers, params=params, timeout=15)
    
    if not r.ok:
        print(r.json())
        

    data = check(r)

    return data

@retry()
def put_taskitem(payload):
    
    taskitemid = payload['Id']
    
    url = f'https://api.pensionpro.com/v1/taskitems/{taskitemid}'
    
    r = requests.put(url, json=payload, headers=headers, timeout=5)

    if not r.ok:
        print(r.json())
   
   
    data = check(r)
    
    return data

@retry()
def get_taskitems_by_taskid(taskid, filters=None, expand=None, skip=None, top=None, orderby=None):

    params = {}

    if filters:
        params.update({'$filter':filters})

    if expand:
        params.update({'$expand':expand})

    if skip:
        params.update({'$skip':skip})
                
    if top:
        params.update({'$top':top})

    if orderby:
        params.update({'$orderby':orderby})

    url = f'{base}/tasks/{taskid}/taskitems'

    r = requests.get(url, headers=headers, params=params, timeout=15)
    
    if not r.ok:
        print(r.json())


    data = check(r)['Values']

    return data

def delete_services_provided(services_provided_id):
    url = f'{base}/planServicesProvidedLinks/{services_provided_id}'
    r = requests.delete(url, headers=headers, timeout=15)
    if not r.ok:
        print(r.json())
        
@retry()
def get_phone_number_by_contactid(contact_id):
    url = f'{base}/contacts/{contact_id}/numbers'
    params = {}
    params.update({'$expand':'PhoneNumber'})
    r = requests.get(url, headers=headers, params=params, timeout=15)
    if not r.ok:
        print(r.json())
    data = check(r)['Values']

    return data

@retry()
def add_project_file(filepath, ProjectId, ProjectFileTypeId=586, ShowOnWeb=True, Title=None, Comment='', Archived=False, HasBeenWarned=True, ShowOnPSL = False, EffectiveOn = None):
    print('here')
    
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
      "HasBeenWarned": HasBeenWarned,
      "ShowOnPSL" : ShowOnPSL,
      "EffectiveOn": EffectiveOn
    }

    files = {
        'ProjectFile': (None, json.dumps(payload)),
        'file': (filepath , open(filepath, 'rb'), 'multipart/form-data')
    }
    r = requests.post(url, files=files, headers=headers)
    return check(r)

@retry()
def get_project_by_projectid(projectId, filters=None, expand=None, skip=None, top=None, orderby=None):
    
    params = set_params(filters=filters, expand=expand, skip=skip, top=top, orderby=orderby)
    
    url = f'{base}/projects/{projectId}'
    r = requests.get(url, headers=headers, params=params, timeout=15)

    data = check(r)
    return data

@retry()
def update_project(payload, expand=None):
    
    params = set_params(expand=expand)
    
    projid = payload['Id']
    
    url = f'{base}/projects/{projid}'
    r = requests.put(url, json=payload, params=params, headers=headers, timeout=15)

    return check(r)

@retry()
def get_all_services_provided(filters=None, expand=None, skip=None, top=None, orderby=None):
    
    params = set_params(filters=filters, expand=expand, skip=skip, top=top, orderby=orderby)
    
    url = f'{base}/planServicesProvidedLinks'
    r = requests.get(url, headers=headers, params=params, timeout=15)

    data = check(r)['Values']
    return data

@retry()
def update_taskitem(payload, expand=None):
    
    params = set_params(expand=expand)
    
    taskitemid = payload['Id']
    
    url = f'{base}/taskitems/{taskitemid}'
    r = requests.put(url, json=payload, headers=headers, params=params, timeout=5)
    
    return check(r)

@retry()
def get_time_entries(filters=None, expand=None, top=None, orderby=None, skip=None):

    params = set_params(filters=filters, expand=expand, skip=skip, top=top, orderby=orderby)

    url = f'{base}/timeEntries'
    r = requests.get(url, headers=headers, params=params, timeout=15)
    
    return check(r)['Values']

@retry()
def get_investment_providers_by_planid(planid, filters=None, expand='InvestmentProvider', skip=None, top=None, orderby=None):

    params = set_params(filters=filters, expand=expand, skip=skip, top=top, orderby=orderby)
    
    sysplanid = get_sysplanid(planid)

    url = f'{base}/plans/{sysplanid}/investmentproviders'
    r = requests.get(url, headers=headers, params=params, timeout=15)
    
    return check(r)['Values']

@retry()
def complete_task(taskid):
    
    url = f'{base}/tasks/{taskid}/completetask'
    r = requests.put(url, headers=headers, timeout=5)
    
    return check(r)

@retry()
def add_employee_plan_role(planid, ContactId, RoleTypeId, HasBeenWarned=False, IsDeactivated=False, filters=None, expand=None):

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
def update_contact(payload, expand=None):

    params = set_params(expand=expand)
    
    contactid = payload['Id']
    
    url = f'{base}/contacts/{contactid}'
    r = requests.put(url, json=payload, headers=headers, params=params, timeout=15)

    return check(r)

@retry()
def update_plan_contact_role(payload, expand=None):

    params = set_params(expand=expand)
    
    planContactRoleId = payload['Id']
    
    url = f'{base}/plancontactroles/{planContactRoleId}'
    r = requests.put(url, json=payload, headers=headers, params=params, timeout=15)

    return check(r)

@retry()
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
def delete_project_file(projectFileId:int):

    url = f'{base}/projectfiles/{projectFileId}'
    r = requests.delete(url, headers=headers, timeout=15)
    
    return check(r)

@retry()
def get_project_files_by_projectid(projid:int, filters=None, expand=None, skip=None, top=None, orderby=None):
    
    params = set_params(filters=filters, expand=expand, skip=skip, top=top, orderby=orderby)
    
    url = f'{base}/projects/{projid}/projectfiles'
    r = requests.get(url, headers=headers, params=params, timeout=15)
    
    return check(r)['Values']

@retry()
def update_project_file(payload:dict, expand=None):
    
    params = set_params(expand=expand)
    
    projfileid = payload['Id']
    
    url = f'{base}/projectfiles/{projfileid}'
    r = requests.put(url, json=payload, headers=headers, params=params, timeout=5)
    
    return check(r)

