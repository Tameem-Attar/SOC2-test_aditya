import requests
import pandas as pd
import boto3    
import io  
import snowflake 
import sys
from pandas.io.json import json_normalize 
  
''' 
Objective: Makes an API call to smartrecruiter application db to fetch the list of candidates. For each candidate get
           the detailed attributes through API calls. Code Further aggrigates the details into a single csv file to write to s3
'''
 
def test(event, lambda_context):
    import requests 
    import pandas as pd
    import boto3
    '''
    BASE_URL = 'https://api.smartrecruiters.com/jobs'
    token = "DCRA1-4120a0e0db8440329d323390aaad5cf5"
    
    auth_response = requests.get(BASE_URL, headers={'x-smarttoken': 'DCRA1-4120a0e0db8440329d323390aaad5cf5'})
    
    data=auth_response.json()
    content=data["content"]
    total_candidate = len(content)
    
    generate_df_list=[]
    extend_list=[]
    for candidate_details in content:
        cand__id=candidate_details["id"] 
        api_endpoint_candidate='https://api.smartrecruiters.com/jobs/'+cand__id
        print("url: ",api_endpoint_candidate)
        auth_response = requests.get(api_endpoint_candidate, headers={'x-smarttoken': 'DCRA1-4120a0e0db8440329d323390aaad5cf5'})
        print(auth_response.json())
        content=auth_response.json()
        cols_from_json=['id', 'title', 'refNumber', 'status', 'createdOn', 'lastActivityOn', 'location_countryCode', 'location_country', 'location_city', 'location_manual', 'location_remote', 'location_regionCode','actions_details']
        list_for_content=[]
        list_for_content.extend([content['id'], content['title'], content['refNumber'], content['status'], content['createdOn'], content['lastActivityOn'],content['location']['countryCode'], content['location']['country'],content['location']['city'], content['location']['manual'], content['location']['remote'], content['location']['regionCode'], content['actions']['details']])
        generate_df_list.append(list_for_content)
    print("test")
         
    
    api_endpoint_candidate='https://api.smartrecruiters.com/candidates/e493879e-6826-4e2b-a875-983d50e46b4c'
    auth_response = requests.get(api_endpoint_candidate, headers={'x-smarttoken': 'DCRA1-4120a0e0db8440329d323390aaad5cf5'})
    print(auth_response.json())
    
    Candidate_info = pd.DataFrame(generate_df_list,columns=cols_from_json)
    print(Candidate_info)
    print("generate_df_list",generate_df_list)
    print("\n\n\n\n\n")
      
    s3 = boto3.client('s3')
    bucket_name = "smartrecruiterdata" 
    #df.to_csv("JOBID_new.csv")  
    with io.StringIO() as csv_buffer:
        Candidate_info.to_csv(csv_buffer, index=False)
        response = s3.put_object(Bucket=bucket_name,Key='JobID_data.csv',Body=csv_buffer.getvalue())
    print("written to S3")
    return("wow great run - Lambda Executed. {} were sent to Snowflake".format(total_candidate))
    '''
    
    
    import requests
    import pandas as pd
    import boto3
    import io
    import snowflake
    import sys
    import requests
    import boto3
    import os
    import math
    import time
    import multiprocessing as mp
    import concurrent.futures
    
    # print("Number of processors: ", mp.cpu_count())
    print("testing the code")
    
    def get_response_json(base_url1, token1):
        auth_response = requests.get(base_url1, headers={'x-smarttoken': token1})
        return auth_response.json()
    
     
    def extract_job_count(base_url1, token1):
        response = get_response_json(base_url1, token1)
        count = response["totalFound"]
        print("TOTAL COUNT ::: {}".format(count))
        return count
    
      
    def create_offset_values(count):
        count= count//20
        if count % 100 == 0:
            a = math.floor(count // 100)
        else:
            a = math.floor((count // 100) + 1)
    
        m = [i for i in range(0, a * 100, 100)]
        return m
    
    def get_id(base_url1, token1):
        a = []
        total_job_count = extract_job_count(base_url1, token1)
        offset_list = create_offset_values(total_job_count)
        print("offset_list ::: {}".format(offset_list))
    
        for i in offset_list:
     
            if i == 0:
                offset_url = base_url1 + "?limit=100"
                # offset_url = base_url1 + "?limit=5"
            else:
                offset_url = base_url1 + "?limit=100&offset={}".format(i)
            # print("FULL_URL ::: {}".format(offset_url))
            response = get_response_json(offset_url, token1)
            content = response["content"]
            try:
                for j in range(0, 100):
                    # print("ID ::: {}".format(content[j]['id']))
                    #yield content[j]['id']
                    a.append(content[j]['id'])
                return(a)
                 
            except IndexError:
                pass   
        print("ID GENERATED : ",len(a))
    
    
    list_column = ['id',
                       'internal',
                       'firstName',
                       'lastName',
                       'email',
                       'phoneNumber',
                       'createdOn',
                       'updatedOn',
                       'tags',
                       'education',
                       'experience',
                       'location.country',
                       'location.city',
                       'location.region',
                       'location.lat',
                       'location.lng',
                       'web.linkedin',
                       'actions.properties.method',
                       'actions.properties.url',
                       'actions.attachments.method',
                       'actions.attachments.url',
                       'actions.consent.method',
                       'actions.consent.url',
                       'actions.consents.method',
                       'actions.consents.url',
                       'primaryAssignment.status',
                       'primaryAssignment.source',
                       'primaryAssignment.reasonOfRejection.id',
                       'primaryAssignment.reasonOfRejection.label',
                       'primaryAssignment.actions.sourceDetails.url',
                       'primaryAssignment.actions.sourceDetails.method',
                       'primaryAssignment.job.id',
                       'primaryAssignment.job.title',
                       'primaryAssignment.job.actions.details.url',
                       'primaryAssignment.job.actions.details.method',
                       'secondaryAssignments.status',
                       'secondaryAssignments.source',
                       'secondaryAssignments.reasonOfRejection.id',
                       'secondaryAssignments.reasonOfRejection.label',
                       'secondaryAssignments.actions.sourceDetails.url',
                       'secondaryAssignments.actions.sourceDetails.method',
                       'secondaryAssignments.job.id',
                       'secondaryAssignments.job.title',
                       'secondaryAssignments.job.actions.details.url',
                       'secondaryAssignments.job.actions.details.method']
    generate_df = pd.DataFrame(data=[], columns=list_column)
    
    
    def get_id_detail(base_url, token, id):
    
        job_id_url = base_url + '/' + str(id)
        print("url: ", job_id_url)
        content = get_response_json(job_id_url, token)
        df_json_flat = json_normalize(content, max_level=6)
        if "secondaryAssignments" in list(df_json_flat.columns):
            tt = content["secondaryAssignments"]
            df_json_flat = df_json_flat.drop("secondaryAssignments", axis='columns')
            df1 = json_normalize(tt)
            df1 = df1.add_prefix('secondaryAssignments.')
            df_json_flat = pd.concat([df_json_flat, df1], axis=1)
        for i in list_column:
            if i in list(df_json_flat.columns):
                continue
            else: 
                missing_column = pd.DataFrame(data={str(i): ["Null"]})
                # print("TYPE missing column ::: {}".format(missing_column))
                df_json_flat = pd.concat([df_json_flat, missing_column], axis=1)
                # print("TYPE df_json_flat ::: {}".format(df_json_flat))
        return df_json_flat
    
    
    """
    def get_details(base_url, token):
        list_column = ['id',
                       'internal',
                       'firstName',
                       'lastName',
                       'email',
                       'phoneNumber',
                       'createdOn',
                       'updatedOn',
                       'tags',
                       'education',
                       'experience',
                       'location.country',
                       'location.city',
                       'location.region',
                       'location.lat',
                       'location.lng',
                       'web.linkedin',
                       'actions.properties.method',
                       'actions.properties.url',
                       'actions.attachments.method',
                       'actions.attachments.url',
                       'actions.consent.method',
                       'actions.consent.url',
                       'actions.consents.method',
                       'actions.consents.url',
                       'primaryAssignment.status',
                       'primaryAssignment.source',
                       'primaryAssignment.reasonOfRejection.id',
                       'primaryAssignment.reasonOfRejection.label',
                       'primaryAssignment.actions.sourceDetails.url',
                       'primaryAssignment.actions.sourceDetails.method',
                       'primaryAssignment.job.id',
                       'primaryAssignment.job.title',
                       'primaryAssignment.job.actions.details.url',
                       'primaryAssignment.job.actions.details.method',
                       'secondaryAssignments.status',
                       'secondaryAssignments.source',
                       'secondaryAssignments.reasonOfRejection.id',
                       'secondaryAssignments.reasonOfRejection.label',
                       'secondaryAssignments.actions.sourceDetails.url',
                       'secondaryAssignments.actions.sourceDetails.method',
                       'secondaryAssignments.job.id',
                       'secondaryAssignments.job.title',
                       'secondaryAssignments.job.actions.details.url',
                       'secondaryAssignments.job.actions.details.method']
        generate_df = pd.DataFrame(data=[], columns=list_column)
        df_out = []
        extend_list = []
    
        for id in get_id(base_url, token):
            print(id)
            print("id :: {}".format(id))
            job_id_url = base_url + '/' + str(id)
            print("url: ", job_id_url)
            content = get_response_json(job_id_url, token)
            print(content)
    
            df_json_flat = pd.json_normalize(content, max_level=6)
            if "secondaryAssignments" in list(df_json_flat.columns):
                tt = content["secondaryAssignments"]
                df_json_flat = df_json_flat.drop("secondaryAssignments", axis='columns')
                df1 = pd.json_normalize(tt)
                df1 = df1.add_prefix('secondaryAssignments.')
                df_json_flat = pd.concat([df_json_flat, df1], axis=1)
    
            for i in list_column:
                if i in list(df_json_flat.columns):
                    continue
                else:
                    missing_column = pd.DataFrame(data={str(i): ["Null"]})
                    df_json_flat = pd.concat([df_json_flat, missing_column], axis=1)
     
            print(df_json_flat)
            generate_df = generate_df.append(df_json_flat)
            # generate_df_list.append(list_for_content)
            print(generate_df)
        return generate_df
    """
      
    base_url = "https://api.smartrecruiters.com/candidates"
    token = "DCRA1-4120a0e0db8440329d323390aaad5cf5"
    # print(*get_id(base_url, token), sep="\n")
    start = time.perf_counter()
    generate_df_list = []
    ''' 
    with concurrent.futures.ProcessPoolExecutor() as executor:
     
        results = [executor.submit(get_id_detail, base_url, token, id) for id in get_id(base_url, token)]
    
        for f in concurrent.futures.as_completed(results):
            generate_df = generate_df.append(f.result())
    '''  
       
    Ids= get_id(base_url, token)
    print("\n")
    print("IDS generated")
    print("Ids", Ids)
    
    #print(get_id(base_url, token), sep="\n")
       
    details=get_id_detail(base_url, token, Ids) #for id in get_id(base_url, token)
    print("details: ", details)
    
    
    #generate_df.drop_duplicates(subset=['id']).to_csv("C:/Users/VeereshNaidu/OneDrive - Blend 360/Desktop/candidate_details.csv", index=False)
    
    end = time.perf_counter()
    print("fiinished in {} second(s)".format(round(end - start), 2))



      

 
  

