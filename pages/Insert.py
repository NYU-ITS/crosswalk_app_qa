import streamlit as st
import numpy as np
import requests
import json
import time
import pandas as pd
from snowflake.snowpark.context import get_active_session

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col
from snowflake.snowpark.dataframe_reader import *
from snowflake.snowpark.functions import *

from streamlit_extras.customize_running import center_running

session = get_active_session()

st.markdown(
    """
    <style>
        [data-testid=stAppViewContainer] {
            background-color: #f2f2f2;
        }
        [data-testid=stSidebar] {
            background-color: #7b5aa6;
        }
    </style>
    """,
    unsafe_allow_html=True,
)


st.title(f'''New Crosswalk Mapping''')

if 'data' in st.session_state:
    if st.session_state.data['status'] == 200:
        if st.session_state.data['netid'] is not None:
            crosswalk_options = session.sql(f'''
                SELECT DISTINCT REFDATA_CROSSWALK_NAME,
                                REFDATA_CROSSWALK_DOMAIN,
                                REFDATA_CROSSWALK_COMMUNITY,
                                REFDATA_SOURCE_CODESET,
                                REFDATA_TARGET_CODESET,
                                REFDATA_CROSSWALK_RESOURCE_ID,
                                REFDATA_CROSSWALK_DOMAIN_RESOURCE_ID,
                                REFDATA_CROSSWALK_COMMUNITY_RESOURCE_ID, 
                                REFDATA_CROSSWALK_TYPE,
                                REFDATA_CROSSWALK_DATA_CUSTODIAN_NETID,
                                REFDATA_CROSSWALK_DATA_STEWARD_NETID
                FROM NYU_REF_DATA_LND.NYU_REFDATA_XWALK_HEADER
                WHERE REFDATA_CROSSWALK_DATA_CUSTODIAN_NETID LIKE '%{st.session_state.data['netid'].strip()}%'
                    OR REFDATA_CROSSWALK_DATA_STEWARD_NETID LIKE '%{st.session_state.data['netid'].strip()}%'
            ''').to_pandas()

            if len(crosswalk_options) != 0:
                crosswalk_options['types'] = list(zip(crosswalk_options.REFDATA_CROSSWALK_NAME, crosswalk_options.REFDATA_CROSSWALK_TYPE))

                crosswalk_name = st.selectbox(
                    "Select crosswalk to insert",
                    crosswalk_options['types'].unique(), index=0
                )

                df_custodians = crosswalk_options.loc[crosswalk_options.REFDATA_CROSSWALK_NAME == crosswalk_name[0]]
                
                if crosswalk_name[1] == 'Standard':
                    st.write('You are choosing a standard code mapping crosswalk.')
                elif crosswalk_name[1] == 'Cross_Functional':
                    st.write('You are choosing a cross-functional code mapping crosswalk.')


                if crosswalk_name[0] is not None:
                    source_codeset = st.selectbox("Select source codeset", 
                                    crosswalk_options.loc[crosswalk_options.REFDATA_CROSSWALK_NAME == crosswalk_name[0]]['REFDATA_SOURCE_CODESET'].unique(),\
                                    index=0)
                    target_codeset = st.selectbox("Select target codeset", 
                                    crosswalk_options.loc[crosswalk_options.REFDATA_CROSSWALK_NAME == crosswalk_name[0]]['REFDATA_TARGET_CODESET'].unique(),\
                                    index=0)

                    unmapped_source_codes = session.sql(f"""
                    SELECT DISTINCT a.REFDATA_CODE_RESOURCE_ID, a.REFDATA_CODE, a.REFDATA_CODE_DESCRIPTION 
                        FROM NYU_REFDATA_CODESET a                                  
                        LEFT JOIN NYU_REFDATA_XWALK b
                            ON a.REFDATA_CODE_RESOURCE_ID = b.REFDATA_CROSSWALK_REFDATA_SOURCE_CODE_RESOURCE_ID
                    WHERE a.REFDATA_AUTHORITATIVE_FLG = 'N' AND a.REFDATA_CODESET = '{source_codeset}'
                        AND b.REFDATA_CROSSWALK_REFDATA_SOURCE_CODE_RESOURCE_ID IS NULL                                    
                    """).to_pandas()

                    st.write('Source codes currently unmapped:')
                    st.dataframe(unmapped_source_codes.drop(columns=['REFDATA_CODE_RESOURCE_ID']))


                    # Source code
                    source_codes = session.sql(f'''
                    SELECT 
                        DISTINCT REFDATA_CODE,
                        REFDATA_CODE_DESCRIPTION,
                        REFDATA_CODE_DOMAIN,
                        REFDATA_CODE_COMUNITY,
                        REFDATA_CODESET_RESOURCE_ID,
                        REFDATA_CODE_RESOURCE_ID,
                        REFDATA_CODESET,
                        REFDATA_COMMUNITY_RESOURCE_ID,
                        REFDATA_DOMAIN_RESOURCE_ID,
                        REFDATA_AUTHORITATIVE_FLG
                    FROM NYU_REFDATA_CODESET
                    ''').to_pandas()\
                        .query(f'''(REFDATA_AUTHORITATIVE_FLG == "N") & (REFDATA_CODESET == "{source_codeset}")''')\
                        .dropna(subset=['REFDATA_CODE', 'REFDATA_CODE_DESCRIPTION'])

                    source_codes['code'] = list(zip(source_codes.REFDATA_CODE, source_codes.REFDATA_CODE_DESCRIPTION))

                    source_code = st.selectbox(
                        "Select source code",
                        source_codes['code'].values, index=None
                    )


                    # Target code
                    if crosswalk_name[1] == 'Standard':
                        target_codes = session.sql(f'''
                                        SELECT 
                                            DISTINCT REFDATA_CODE, REFDATA_CODESET, REFDATA_CODE_DOMAIN, 
                                            REFDATA_CODE_COMUNITY, REFDATA_CODE_DESCRIPTION, REFDATA_CODE_RESOURCE_ID, REFDATA_AUTHORITATIVE_FLG
                                        FROM NYU_REFDATA_CODESET''')\
                                        .to_pandas().query(f'''(REFDATA_AUTHORITATIVE_FLG == "Y") & (REFDATA_CODESET == "{target_codeset}")''')
                        target_codes['code'] = list(zip(target_codes.REFDATA_CODE, target_codes.REFDATA_CODE_DESCRIPTION))
                        target_code = st.selectbox(
                            "Select authoritative code",
                            target_codes['code'], index=None
                        )
                    elif crosswalk_name[1] == 'Cross_Functional':
                        target_codes = session.sql(f'''
                                        SELECT 
                                            DISTINCT REFDATA_CODE, REFDATA_CODESET, REFDATA_CODE_DOMAIN,
                                            REFDATA_CODE_COMUNITY,REFDATA_CODE_DESCRIPTION, REFDATA_CODE_RESOURCE_ID, REFDATA_AUTHORITATIVE_FLG
                                        FROM NYU_REFDATA_CODESET''')\
                                        .to_pandas().query(f'''(REFDATA_AUTHORITATIVE_FLG == "N") & (REFDATA_CODESET == "{target_codeset}")''')
                        target_codes['code'] = list(zip(target_codes.REFDATA_CODE, target_codes.REFDATA_CODE_DESCRIPTION))
                        target_code = st.selectbox(
                            "Select authoritative code",
                            target_codes['code'], index=None
                        )


                #if source_code is not None:
                xwalk_name = crosswalk_name[0]

                xwalk_domain = crosswalk_options.loc[crosswalk_options.REFDATA_CROSSWALK_NAME == xwalk_name].REFDATA_CROSSWALK_DOMAIN.values[0]
                xwalk_community = crosswalk_options.loc[crosswalk_options.REFDATA_CROSSWALK_NAME == xwalk_name].REFDATA_CROSSWALK_COMMUNITY.values[0]
                xwalk_resource_id = crosswalk_options.loc[crosswalk_options.REFDATA_CROSSWALK_NAME == xwalk_name].REFDATA_CROSSWALK_RESOURCE_ID.values[0]
                xwalk_domain_id = crosswalk_options.loc[crosswalk_options.REFDATA_CROSSWALK_NAME == xwalk_name].REFDATA_CROSSWALK_DOMAIN_RESOURCE_ID.values[0]
                xwalk_community_id = crosswalk_options.loc[crosswalk_options.REFDATA_CROSSWALK_NAME == xwalk_name].REFDATA_CROSSWALK_COMMUNITY_RESOURCE_ID.values[0]

                if source_code is not None and target_code is not None:
                    output_source_code = source_code[0]
                    output_target_code = target_code[0]

                    source_code_domain = source_codes.loc[(source_codes.REFDATA_CODE == output_source_code) & (source_codes.REFDATA_CODESET == source_codeset)]\
                                            .REFDATA_CODE_DOMAIN.values[0]
                    source_code_community = source_codes.loc[(source_codes.REFDATA_CODE == output_source_code) & (source_codes.REFDATA_CODESET == source_codeset)]\
                                            .REFDATA_CODE_COMUNITY.values[0]
                    source_code_resource_id = source_codes.loc[(source_codes.REFDATA_CODE == output_source_code) & (source_codes.REFDATA_CODESET == source_codeset)]\
                                            .REFDATA_CODE_RESOURCE_ID.values[0]

                    target_code_domain = target_codes.loc[(target_codes.REFDATA_CODE == output_target_code) & (target_codes.REFDATA_CODESET == target_codeset)]\
                                            .REFDATA_CODE_DOMAIN.values[0]
                    target_code_community = target_codes.loc[(target_codes.REFDATA_CODE == output_target_code) & (target_codes.REFDATA_CODESET == target_codeset)]\
                                            .REFDATA_CODE_COMUNITY.values[0]
                    target_code_resource_id = target_codes.loc[(target_codes.REFDATA_CODE == output_target_code) & (target_codes.REFDATA_CODESET == target_codeset)]\
                                            .REFDATA_CODE_RESOURCE_ID.values[0]
                    
                if st.button('Insert'):
                    get_duplicates = session.sql(f"""
                        SELECT * FROM NYU_REFDATA_XWALK
                            WHERE 
                                REFDATA_CROSSWALK_RESOURCE_ID = '{xwalk_resource_id}'
                                AND REFDATA_CROSSWALK_REFDATA_SOURCE_CODE_RESOURCE_ID = '{source_code_resource_id}'
                                AND REFDATA_CROSSWALK_REFDATA_TARGET_CODE_RESOURCE_ID = '{target_code_resource_id}'
                    """).to_pandas()

                    get_one_to_many = session.sql(f"""
                        SELECT * FROM NYU_REFDATA_XWALK
                            WHERE 
                                REFDATA_CROSSWALK_RESOURCE_ID = '{xwalk_resource_id}'
                                AND REFDATA_CROSSWALK_REFDATA_SOURCE_CODE_RESOURCE_ID = '{source_code_resource_id}'
                                AND REFDATA_CROSSWALK_REFDATA_TARGET_CODE_RESOURCE_ID != '{target_code_resource_id}'
                    """).to_pandas()

                    if len(get_duplicates) != 0:
                        st.write(f"Mapping {output_source_code} -> {output_target_code} already exists in {xwalk_name} crosswalk in Collibra. No duplicate mapping is added.")
                    elif len(get_one_to_many) != 0:
                        st.write(f"{output_source_code} is already mapped in {xwalk_name} crosswalk in Collibra. Use the update page to make changes instead.")
                    else:
                        # Collibra
                        url = 'https://nyu-test.collibra.com/rest/2.0/import/json-job'

                        headers = {
                            'Authorization': f"Bearer {st.session_state.data['jwt_token']}",
                            'accept': 'application/json'
                        }

                        params = {
                            'simulation': False,
                            'fileName': 'import_file',
                        }

                        import_json = [{
                                "resourceType": "Complex Relation", 
                                "complexRelationType": {"name": "Code Mapping"},
                                "relations": 
                                    {
                                    "00000000-0000-0000-0000-000000007080:TARGET": [{"id": f"{source_code_resource_id}"}],
                                    "00000000-0000-0000-0000-000000007081:TARGET": [{"id": f"{target_code_resource_id}"}],
                                    "00000000-0000-0000-0000-000000007082:TARGET": [{"id": f"{xwalk_resource_id}"}]
                                }
                        }]
                        
                        files = {"file": ("import.json", json.dumps(import_json), "application/json")}
                        
                        x = requests.post(url=url, params=params, headers=headers, files=files)
                        
                        if x.status_code == 200:
                            st.write('Import to Collibra in progress.')
                            center_running()
                            time.sleep(7)
                            y = requests.get(f"https://nyu-test.collibra.com/rest/2.0/import/results/{x.json()['id']}/summary", headers=headers)
                            if y.json()['importJobState'] == 'COMPLETED':
                                st.write('Import to Collibra successful.')
                                tvc = {"TableViewConfig":{"Resources":{"ComplexRelation":{"Id":{"name":"complexRelationId"},"StringAttribute":[{"labelId":"00000000-0000-0000-0000-000000003114","Value":{"name":"description"}},{"labelId":"00000000-0000-0000-0000-000000000249","Value":{"name":"transformationLogic"}}],"Relation":[{"typeId":"00000000-0000-0000-0000-000000007080","type":"SOURCE","Target":{"Signifier":{"name":"sourceFullName"},"DisplayName":{"name":"sourceName"},"Id":{"name":"sourceId"},"Domain":{"Name":{"name":"sourceDomainName"},"Id":{"name":"sourceDomainId"},"Community":{"Name":{"name":"sourceCommunityName"},"Id":{"name":"sourceCommunityId"}}}}},{"typeId":"00000000-0000-0000-0000-000000007081","type":"SOURCE","Target":{"Signifier":{"name":"targetFullName"},"DisplayName":{"name":"targetName"},"Id":{"name":"targetId"},"Domain":{"Name":{"name":"targetDomainName"},"Id":{"name":"targetDomainId"},"Community":{"Name":{"name":"targetCommunityName"},"Id":{"name":"targetCommunityId"}}}}},{"typeId":"00000000-0000-0000-0000-000000007082","type":"SOURCE","Target":{"Signifier":{"name":"crosswalkName"},"Id":{"name":"crosswalkId"},"Domain":{"Name":{"name":"crosswalkDomainName"},"Id":{"name":"crosswalkDomainId"},"Community":{"Name":{"name":"crosswalkCommunityName"},"Id":{"name":"crosswalkCommunityId"}},"Responsibility":[{"roleId":"c0e00000-0010-0010-0000-000000000000","User":{"UserName":{"name":"dataStewardUserName"},"EmailAddress":{"name":"dataStewardEmail"}},"includeInherited":"true"},{"roleId":"00000000-0000-0000-0000-000000005041","User":{"UserName":{"name":"dataCustodianUserName"},"EmailAddress":{"name":"dataCustodianEmail"}},"includeInherited":"true"}]},"Relation":[{"typeId":"00000000-0000-0000-0000-000000007026","type":"TARGET","Source":{"Signifier":{"name":"sourceCodeSetName"},"Id":{"name":"sourceCodeSetId"},"Domain":{"Name":{"name":"sourceCodeSetDomainName"},"Id":{"name":"sourceCodeSetDomainId"},"Community":{"Name":{"name":"sourceCodeSetCommunityName"},"Id":{"name":"sourceCodeSetCommunityId"}}}}},{"typeId":"00000000-0000-0000-0000-000000007027","type":"TARGET","Source":{"Signifier":{"name":"targetCodeSetName"},"Id":{"name":"targetCodeSetId"},"Domain":{"Name":{"name":"targetCodeSetDomainName"},"Id":{"name":"targetCodeSetDomainId"},"Community":{"Name":{"name":"targetCodeSetCommunityName"},"Id":{"name":"targetCodeSetCommunityId"}}}}}]}}],"Filter":{"AND":[{"Field":{"name":"crosswalkId","operator":"IN","value":[f"{xwalk_resource_id}"]}}]},"Order":[{"Field":{"name":"crosswalkName","order":"ASC"}},{"Field":{"name":"sourceName","order":"ASC"}},{"Field":{"name":"targetName","order":"ASC"}}]}},"Columns":[{"Column":{"fieldName":"complexRelationId"}},{"Column":{"fieldName":"sourceId"}},{"Column":{"fieldName":"sourceName"}},{"Column":{"fieldName":"sourceDomainName"}},{"Column":{"fieldName":"sourceCommunityName"}},{"Column":{"fieldName":"targetId"}},{"Column":{"fieldName":"targetName"}},{"Column":{"fieldName":"targetDomainName"}},{"Column":{"fieldName":"targetCommunityName"}},{"Column":{"fieldName":"crosswalkId"}},{"Column":{"fieldName":"crosswalkName"}},{"Column":{"fieldName":"crosswalkDomainName"}},{"Column":{"fieldName":"crosswalkDomainId"}},{"Column":{"fieldName":"crosswalkCommunityName"}},{"Column":{"fieldName":"crosswalkCommunityId"}},{"Column":{"fieldName":"description"}},{"Column":{"fieldName":"transformationLogic"}},{"Group":{"name":"dataStewards","Columns":[{"Column":{"fieldName":"dataStewardUserName"}}]}},{"Group":{"name":"dataCustodians","Columns":[{"Column":{"fieldName":"dataCustodianUserName"}}]}},{"Group":{"name":"sourceCodeSets","Columns":[{"Column":{"fieldName":"sourceCodeSetId"}},{"Column":{"fieldName":"sourceCodeSetName"}},{"Column":{"fieldName":"sourceCodeSetDomainName"}},{"Column":{"fieldName":"sourceCodeSetCommunityName"}}]}},{"Group":{"name":"targetCodeSets","Columns":[{"Column":{"fieldName":"targetCodeSetId"}},{"Column":{"fieldName":"targetCodeSetName"}},{"Column":{"fieldName":"targetCodeSetDomainName"}},{"Column":{"fieldName":"targetCodeSetCommunityName"}}]}}]}}
                                url = "https://nyu-test.collibra.com/rest/2.0/outputModule/export/json"

                                z = requests.post(url=url, \
                                                headers={'Authorization': f"Bearer {st.session_state.data['jwt_token']}",\
                                                        'accept': '*/*'}, \
                                                    json=tvc, params={'validationEnabled': False})
                                if z.status_code == 200:
                                    df = pd.DataFrame(z.json()['aaData'])

                                    df['dataStewards'] = df['dataStewards'].apply(lambda l: ", ".join([entry['dataStewardUserName'].rstrip('@nyu.edu') for entry in l]))
                                    df['dataCustodians'] = df['dataCustodians'].apply(lambda l: ", ".join([entry['dataCustodianUserName'].rstrip('@nyu.edu') for entry in l]))
                                    df['sourceCodeSets'] = df['sourceCodeSets'].apply(lambda x: x[0])
                                    df['targetCodeSets'] = df['targetCodeSets'].apply(lambda x: x[0])

                                    df_flattened = pd.concat([df, pd.json_normalize(df.sourceCodeSets), pd.json_normalize(df.targetCodeSets)], axis=1)

                                    collibra_to_snowflake = {
                                        "crosswalkName": "REFDATA_CROSSWALK_NAME",
                                        "crosswalkDomainName": "REFDATA_CROSSWALK_DOMAIN",
                                        "crosswalkCommunityName": "REFDATA_CROSSWALK_COMMUNITY",
                                        "sourceCodeSetName": "REFDATA_SOURCE_CODESET",
                                        "sourceId": "REFDATA_CROSSWALK_REFDATA_SOURCE_CODE_RESOURCE_ID",
                                        "sourceName": "REFDATA_SOURCE_CODE",
                                        "sourceDomainName": "REFDATA_SOURCE_CODE_DOMAIN",
                                        "sourceCommunityName": "REFDATA_SOURCE_CODE_COMMUNITY",
                                        "targetCodeSetName": "REFDATA_TARGET_CODESET",
                                        "targetId": "REFDATA_CROSSWALK_REFDATA_TARGET_CODE_RESOURCE_ID",
                                        "targetName": "REFDATA_TARGET_CODE",
                                        "targetDomainName": "REFDATA_TARGET_CODE_DOMAIN",
                                        "targetCommunityName": "REFDATA_TARGET_CODE_COMMUNITY",
                                        "crosswalkId": "REFDATA_CROSSWALK_RESOURCE_ID",
                                        "crosswalkDomainId": "REFDATA_CROSSWALK_DOMAIN_RESOURCE_ID" ,
                                        "crosswalkCommunityId": "REFDATA_CROSSWALK_COMMUNITY_RESOURCE_ID",
                                        "dataStewards": "REFDATA_CROSSWALK_DATA_STEWARD_NETID",
                                        "dataCustodians": "REFDATA_CROSSWALK_DATA_CUSTODIAN_NETID",
                                        "complexRelationId": "REFDATA_CROSSWALK_MAPPING_RESOURCE_ID"
                                    }

                                    # rename columns
                                    columns = ["REFDATA_CROSSWALK_NAME", "REFDATA_CROSSWALK_DOMAIN", "REFDATA_CROSSWALK_COMMUNITY", \
                                                "REFDATA_CROSSWALK_REFDATA_SOURCE_CODE_RESOURCE_ID", "REFDATA_SOURCE_CODESET", "REFDATA_SOURCE_CODE", "REFDATA_SOURCE_CODE_DOMAIN", "REFDATA_SOURCE_CODE_COMMUNITY", \
                                                "REFDATA_CROSSWALK_REFDATA_TARGET_CODE_RESOURCE_ID", "REFDATA_TARGET_CODESET", "REFDATA_TARGET_CODE", "REFDATA_TARGET_CODE_DOMAIN", "REFDATA_TARGET_CODE_COMMUNITY", \
                                                "REFDATA_CROSSWALK_RESOURCE_ID", "REFDATA_CROSSWALK_DOMAIN_RESOURCE_ID", "REFDATA_CROSSWALK_COMMUNITY_RESOURCE_ID",\
                                                "REFDATA_CROSSWALK_DATA_STEWARD_NETID", "REFDATA_CROSSWALK_DATA_CUSTODIAN_NETID", "REFDATA_CROSSWALK_MAPPING_RESOURCE_ID"]
                                    df_flattened = df_flattened.rename(columns=collibra_to_snowflake)[columns]

                                    # UPDATE SNOWFLAKE TABLE by merging
                                    # temp table
                                    snowpark_temp_df = session.write_pandas(df_flattened, 'TEMP', auto_create_table=True, overwrite=True)

                                    session.sql(f"""
                                            MERGE INTO NYU_REFDATA_XWALK AS TARGET
                                                    USING TEMP AS SOURCE
                                                    ON (
                                                        TARGET.REFDATA_CROSSWALK_REFDATA_SOURCE_CODE_RESOURCE_ID = SOURCE.REFDATA_CROSSWALK_REFDATA_SOURCE_CODE_RESOURCE_ID
                                                        AND TARGET.REFDATA_CROSSWALK_REFDATA_TARGET_CODE_RESOURCE_ID = SOURCE.REFDATA_CROSSWALK_REFDATA_TARGET_CODE_RESOURCE_ID
                                                        AND TARGET.REFDATA_CROSSWALK_RESOURCE_ID = SOURCE.REFDATA_CROSSWALK_RESOURCE_ID
                                                    )
                                                    WHEN NOT MATCHED THEN
                                                        INSERT (
                                                        REFDATA_CROSSWALK_NAME,
                                                        REFDATA_CROSSWALK_DOMAIN,
                                                        REFDATA_CROSSWALK_COMMUNITY,
                                                        REFDATA_CROSSWALK_STATUS,
                                                        REFDATA_SOURCE_CODESET, 
                                                        REFDATA_SOURCE_CODE, 
                                                        REFDATA_SOURCE_CODE_DOMAIN,
                                                        REFDATA_SOURCE_CODE_COMMUNITY,
                                                        REFDATA_TARGET_CODESET,
                                                        REFDATA_TARGET_CODE,
                                                        REFDATA_TARGET_CODE_DOMAIN,
                                                        REFDATA_TARGET_CODE_COMMUNITY,
                                                        REFDATA_CROSSWALK_RESOURCE_ID,
                                                        REFDATA_CROSSWALK_REFDATA_SOURCE_CODE_RESOURCE_ID,
                                                        REFDATA_CROSSWALK_REFDATA_TARGET_CODE_RESOURCE_ID, 
                                                        REFDATA_CROSSWALK_DOMAIN_RESOURCE_ID,
                                                        REFDATA_CROSSWALK_COMMUNITY_RESOURCE_ID,
                                                        REFDATA_CROSSWALK_DATA_STEWARD_NETID, 
                                                        REFDATA_CROSSWALK_DATA_CUSTODIAN_NETID,
                                                        CREATED_TIMESTAMP, 
                                                        UPDATE_TIMESTAMP,
                                                        RECORD_SOURCE,
                                                        REFDATA_CROSSWALK_TYPE,
                                                        REFDATA_CROSSWALK_MAPPING_RESOURCE_ID           
                                                        )
                                                        VALUES (
                                                            SOURCE.REFDATA_CROSSWALK_NAME,
                                                            SOURCE.REFDATA_CROSSWALK_DOMAIN,
                                                            SOURCE.REFDATA_CROSSWALK_COMMUNITY,
                                                            'Draft',
                                                            SOURCE.REFDATA_SOURCE_CODESET,
                                                            SOURCE.REFDATA_SOURCE_CODE,
                                                            SOURCE.REFDATA_SOURCE_CODE_DOMAIN,
                                                            SOURCE.REFDATA_SOURCE_CODE_COMMUNITY,
                                                            SOURCE.REFDATA_TARGET_CODESET,
                                                            SOURCE.REFDATA_TARGET_CODE,
                                                            SOURCE.REFDATA_TARGET_CODE_DOMAIN,
                                                            SOURCE.REFDATA_TARGET_CODE_COMMUNITY,
                                                            SOURCE.REFDATA_CROSSWALK_RESOURCE_ID,
                                                            SOURCE.REFDATA_CROSSWALK_REFDATA_SOURCE_CODE_RESOURCE_ID,
                                                            SOURCE.REFDATA_CROSSWALK_REFDATA_TARGET_CODE_RESOURCE_ID,
                                                            SOURCE.REFDATA_CROSSWALK_DOMAIN_RESOURCE_ID,
                                                            SOURCE.REFDATA_CROSSWALK_COMMUNITY_RESOURCE_ID,
                                                            SOURCE.REFDATA_CROSSWALK_DATA_STEWARD_NETID,
                                                            SOURCE.REFDATA_CROSSWALK_DATA_CUSTODIAN_NETID,
                                                            CURRENT_TIMESTAMP,
                                                            CURRENT_TIMESTAMP,
                                                            'Collibra',
                                                            '{crosswalk_name[1]}',
                                                            SOURCE.REFDATA_CROSSWALK_MAPPING_RESOURCE_ID
                                                        )
                                            """).collect()
                                    st.write(f'Inserted {output_source_code} -> {output_target_code} to crosswalk {xwalk_name} in Snowflake table NYU_REFDATA_XWALK')
                        else:
                            st.write(f'Collibra import job failed (response {x.status_code}).')
            else:
                st.text('You are not assigned as a Data Steward or Custodian to any existing Data Crosswalks.')
                st.text('Go to Collibra directly if you need to create a new Data Crosswalk.')
                st.text('Contact eim@nyu.edu if you need to be assigned to an existing Data Crosswalk.')
        else:
            st.text('Retrieving User NetID failed. Go back to Main Page to restart the app to try again.')
    elif 'status' not in st.session_state.data or st.session_state.data['status'] != 200:
        st.text('Connection to Collibra is not established. Restart the app to try again.')
else:
    st.text('Connection to Collibra is not established. Restart the app to try again.')