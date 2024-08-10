# Put everything in lowercase

#these were derek's security for TA login

# sec_questions = ['In what city did you meet your spouse/significant other?',
#                  'What was the name of your first pet?',
#                  'What was the name of the street you grew up on?']
# sec_answers   = ['Jakarta','Oreo','Mustang']

# jlee TA answers
sec_questions = ['What is your favorite beverage?', 'What did you want to be when you grew up?', 'What school did you attend for middle school?']
sec_answers   = ['Dr Pepper', 'President', 'Cardiff Jr High']

def OAuth(n):
    Ids = {

     #Dereks token. Valid until 8/13/23
     #'o365_client_id'      :'fe12d3a6-9b60-4764-ab55-868fd4533247',
     #'o365_secret_id'      :'6099ea74-06bf-4288-89ea-ce5564ade157',
     #'o365_secret_value'   :'M4S8Q~dqHSrRjh3QemKHF_aN0VuvzUsPd11uMbIN',
     
     # Token created by Andrew on 7/18/23. Valid until 7/18/2025
     'o365_client_id'      :"fe12d3a6-9b60-4764-ab55-868fd4533247",
     'o365_secret_id'      :"5f4e35c8-0276-4e8d-95a9-d2ef13af271b",
     'o365_secret_value'   :"qtw8Q~HFdlP1RR4yES4e8paQOglCiieXHR8gvbOZ",
     
     'o365_afs_client_id'      :'90e86821-0660-4524-8bc5-a007f21f2731',
     'o365_afs_secret_id'      :'a258e73c-b3e3-4adf-ac53-81129c51e837',
     'o365_afs_secret_value'   :'DUW8Q~_loex3KNPUk2qCff4i_XB3WrbZgLnAnbmR',
     
     'o365_drm_client_id'    :'d1dde77f-460b-458e-a292-7237cdee06e6',
     'o365_drm_secret_id'    :'f3ee6805-cd37-4bae-ba3a-7a7aa1ef03f3',
     'o365_drm_secret_value' :'Lih8Q~U1YzV7qRSGEoOoqPA1NMA9viJuD2IwQbYw',
        
     # set to Josie's emailbox jlee@nova401k.com
     'o365_user_client_id'    :'7365aea0-4cce-4551-8adc-3f68a079ba92',
     'o365_user_secret_id'    :'0966a47f-3303-4e45-8908-df3f09966334',
     'o365_user_secret_value' :'OLL8Q~wJfgexwYkwX2DMVujDh_kFVCcvq3T_1bd2',
      
      # Token created by Anjana on 7/11/2024 
     'o365_form500_client_id'    :'da4c66c2-b719-421b-b040-a9b7cc773990',
     'o365_form500_secret_id'    :'aafa0498-321d-4636-9558-4a553b820965',
     'o365_form500_secret_value' :'6rX8Q~mvPacq4m_NeHNB2pfuVPoURkZ..Wu7odpi',  
      
    }
    return Ids[n.lower()]





#User name: distributions@nova401k.com
#Password: Kam50044

# Calgontakemeaway16!

def Username(n):

    usernames ={

        # Email
        'outlook': 'automation@nova401k.com',
        'afs admin outlook':'admin@afs316.com',
        'afs distributions outlook':'distributions@afs316.com',
        
	'google_drive':'nova401kassociatesdrive',
        'hancock_amp': 'AMPautomation',
        'hancock': 'dmaggard' ,            #'jeremiahbolinsky',
        'transamerica': 'jlee9888',      # 'dmaggard',  # 'LamHoang86',
        'rkd':'DMAGGARD423',                                  #'LAMHOANG86',
        'pbgc': '',
        'dgem': 'Automation Team',
        'voya':         'dmaggard',       # 'jeremiahb',
        'voya pep':'novamation',
        'empower':'jlee9888',      #'K_95ZKG',    # 'LamHoang86',
        'afp' : 'K_9C5H6',      #'1yjlr',         #'msiddiqui1',             # 'LHoang86',
        'chargeover':'dmaggard@nova401k.com',

        'afs empower':'dennromero',
        'afs empower admin'  :'2x714',
        'afs empower admin 2':'53NR5',

        'afs principal':'dromero10',
        'afs hancock': 'araselyvaldez',
        'afs lincoln': 'araselyvaldez',                      #'dennromero',
        'afs paychex': 'adminfsinc',
        'afs voya': 'dennromero',
        'afs voya admin':'araselyv',
        'afs transamerica':'arasely',
        'afs transamerica 2':'dromero06',
       

        'afs_mass_mutual_aviator':'admin@afs316.com',
        'afs_mass_mutual_reflex':'rr86293c',

        'afs paychex':'adminfsinc',
        
        'lincoln_2fa':'dmaggard',
        'afs_pension_pro':'maggardderek',
        'pension_pro':'dmaggard'
    }

    return usernames[n.lower()]        

 

def Password(n):

    passwords ={
        # Email                                          Templar8046%
        
        'outlook': 'Rub73595',
        'afs admin outlook': 'Har00518',   #'Var99435',#'Mot27196', #'Vav15369', #'Cab41236',
        'afs distributions outlook':'Lac29511',   #'Juz10676',
        'janice outlook':'Charlie1106#',
        
	'google_drive':'4C7ZFu@Upa%',
	'hancock_amp': '@Mp0824!',
        'hancock':'gHostfish3!3!',                                           #'&%yMrmsEXQ3OxYgpD1Aa!9D',
        'transamerica': 'q28qfjYPK#te!Fi',      # 'gHostfish3!3!',    # 'dU3zd5vJCY5!qEg',
        'rkd':'fish3!3!',                                                         #'SXx5nCp!WHhUZax',
        'pbgc':'',
        'dgem': 'DKzof8p6CTpkvtkPhXj', #'_Summer_2024', #_Spring_2024_', #'bNdf8hMp1HIJtdtwSzd5', #'EZGLj5mikSoyVzcuLQJw9',#'4fhi3yBB',#'9c6NTsmNGnciH8TLtQWVfs', #'wu_B4M77XA',#
        'voya':        'gHostfish3!3!',       # 'Z9X@348WIrjruFpFv',  
        'voya pep':'v0y@Automation',        
        'empower': 'K8HfXBGSH9Gyy.J',   #'W77WK%W3EYhgh.u',      #'gHostfish2!2!',
        'afp' :   '3DqB*LmcV3r73xw$', #'gHostfish2!2!',          #'dU3zd5vJCY5!qEh1',
        'chargeover':'gHostfish3!3!',
        
        'afs empower': 'PointyDent24!#',   #'Rubicon12!',
        'afs empower admin'  :'Pointydent24!#f',   #'Rubicon05!!',#'Rubicon02!!', #'Pointydent1!',#'Pointydent13!',
        'afs empower admin 2':'Pointydent25!#',   #'Suchfun04!',
        
        'afs principal':'Dennisboy10*',
        'afs hancock': 'eagerPointyDent2024!#',   #'Calgontakemeaway16!','Trytokeepmeunlocked01!','Tryingsomethingnew01!', #'Stoplockingme0525**','HappyMonday0515!','Herewego0512!!','Herewego051!!','Notagain0501!!',
        'afs lincoln':'Pointydent1!',                                     #'Rubicon10!',
        'afs paychex':'Rubicon1',
        'afs voya':'PointyDent24!#',   #'Rubicon10!',
        'afs voya admin':'PointyDent24!#',   #'Rubicon4#',
        'afs transamerica':'PointyDent24!#',   #'Rubicon2023!!',   #'rubicon10a!',
        'afs transamerica 2':'PointyDent24!#',   #'Dennisboy21!',

        'afs_mass_mutual_aviator':'Rubicon11',
        'afs_mass_mutual_reflex':'Pointydent2!',

        'afs paychex':'Rubicon1',
        
        'lincoln_2fa':'gHostfish3!3!',
        'afs_pension_pro':'gHostfish3!3!',
        'pension_pro':'gHostfish3!3!'
    }

   

    return passwords[n.lower()]
