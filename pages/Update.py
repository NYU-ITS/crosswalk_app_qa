import streamlit as st
from snowflake.snowpark.context import get_active_session
import requests
import pandas as pd
import time

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

st.title(f"Update Crosswalk Mapping")

if 'data' in st.session_state:
    if st.session_state.data['status'] == 200:
        if st.session_state.data['netid'] is not None:
            crosswalk_options = session.sql(f'''
                SELECT 
                    DISTINCT REFDATA_CROSSWALK_NAME, REFDATA_SOURCE_CODESET, REFDATA_TARGET_CODESET, REFDATA_CROSSWALK_RESOURCE_ID
                FROM NYU_REF_DATA_LND.NYU_REFDATA_XWALK_HEADER
                WHERE REFDATA_CROSSWALK_DATA_CUSTODIAN_NETID LIKE '%{st.session_state.data['netid'].strip()}%'
                    OR REFDATA_CROSSWALK_DATA_STEWARD_NETID LIKE '%{st.session_state.data['netid'].strip()}%'
            ''').to_pandas()

            if len(crosswalk_options)!= 0:
                crosswalk_name = st.selectbox(
                    "Select crosswalk to update",
                    crosswalk_options['REFDATA_CROSSWALK_NAME'], index=None
                )
                st.markdown(f"Current crosswalk mappings with User :rainbow[{st.session_state.data['netid']}] as :violet[***Data Custodian or Data Steward***].")

                if crosswalk_name is not None:
                    source_codeset = crosswalk_options.loc[crosswalk_options.REFDATA_CROSSWALK_NAME == crosswalk_name].REFDATA_SOURCE_CODESET.values[0]
                    target_codeset = crosswalk_options.loc[crosswalk_options.REFDATA_CROSSWALK_NAME == crosswalk_name].REFDATA_TARGET_CODESET.values[0]
                    xwalk_resource_id = crosswalk_options.loc[crosswalk_options.REFDATA_CROSSWALK_NAME == crosswalk_name].REFDATA_CROSSWALK_RESOURCE_ID.values[0]
                    user_mappings = session.sql(f"""
                        SELECT DISTINCT a.REFDATA_CROSSWALK_MAPPING_RESOURCE_ID,
                                a.REFDATA_SOURCE_CODE, b.REFDATA_CODE_DESCRIPTION AS SOURCE_CODE_DESCRIPTION,
                                a.REFDATA_TARGET_CODE, c.REFDATA_CODE_DESCRIPTION AS TARGET_CODE_DESCRIPTION,
                                a.REFDATA_CROSSWALK_REFDATA_SOURCE_CODE_RESOURCE_ID, a.CREATED_TIMESTAMP, a.UPDATE_TIMESTAMP, a.REFDATA_CROSSWALK_DATA_CUSTODIAN_NETID
                        FROM NYU_REFDATA_XWALK a           
                            LEFT JOIN NYU_REFDATA_CODESET b ON a.REFDATA_CROSSWALK_REFDATA_SOURCE_CODE_RESOURCE_ID = b.REFDATA_CODE_RESOURCE_ID
                            LEFT JOIN NYU_REFDATA_CODESET c ON a.REFDATA_CROSSWALK_REFDATA_TARGET_CODE_RESOURCE_ID = c.REFDATA_CODE_RESOURCE_ID 
                        WHERE 
                            a.REFDATA_CROSSWALK_RESOURCE_ID = '{xwalk_resource_id}'
                            AND SOURCE_CODE_DESCRIPTION IS NOT NULL
                            AND TARGET_CODE_DESCRIPTION IS NOT NULL
                            AND b.REFDATA_CODESET = '{source_codeset}' 
                            AND c.REFDATA_CODESET = '{target_codeset}'
                            AND (a.REFDATA_CROSSWALK_DATA_CUSTODIAN_NETID LIKE '%{st.session_state.data['netid']}%' 
                            OR a.REFDATA_CROSSWALK_DATA_STEWARD_NETID LIKE '%{st.session_state.data['netid']}%')
                        ORDER BY a.UPDATE_TIMESTAMP DESC                                          
                    """).to_pandas()

                    st.dataframe(user_mappings[['REFDATA_SOURCE_CODE', 'SOURCE_CODE_DESCRIPTION', 'REFDATA_TARGET_CODE', 'TARGET_CODE_DESCRIPTION', \
                                                'CREATED_TIMESTAMP', 'UPDATE_TIMESTAMP', 'REFDATA_CROSSWALK_MAPPING_RESOURCE_ID']])

                    update_source_code = st.selectbox(
                        "Select source code to update",
                        user_mappings['REFDATA_SOURCE_CODE'], index=None
                        )

                    target_codes = session.sql(f'''
                        SELECT 
                            DISTINCT REFDATA_CODE, REFDATA_CODE_DESCRIPTION, REFDATA_CODE_RESOURCE_ID
                        FROM NYU_REFDATA_CODESET
                        WHERE REFDATA_CODESET = '{target_codeset}' AND REFDATA_AUTHORITATIVE_FLG = 'Y'
                        ''').to_pandas()

                    target_codes['code'] = list(zip(target_codes.REFDATA_CODE, target_codes.REFDATA_CODE_DESCRIPTION))        
                    update_target_code = st.selectbox(
                        "Select new target code to map to",
                        target_codes['code'], index=None
                    )

                    if update_source_code != None and update_target_code != None and st.button('Update'):
                        # update Collibra
                        mapping_resource_id = user_mappings.loc[user_mappings.REFDATA_SOURCE_CODE == update_source_code].REFDATA_CROSSWALK_MAPPING_RESOURCE_ID.values[0]
                        source_code_resource_id = user_mappings.loc[user_mappings.REFDATA_SOURCE_CODE == update_source_code].REFDATA_CROSSWALK_REFDATA_SOURCE_CODE_RESOURCE_ID.values[0]
                        source_code = update_source_code
                        old_target_code = user_mappings.loc[user_mappings.REFDATA_SOURCE_CODE == update_source_code].REFDATA_TARGET_CODE.values[0]

                        target_code = update_target_code[0]
                        target_code_description = update_target_code[1]
                        target_code_resource_id = target_codes.loc[target_codes.REFDATA_CODE == target_code].REFDATA_CODE_RESOURCE_ID.values[0]


                        url = f"https://nyu-test.collibra.com/rest/2.0/complexRelations/{mapping_resource_id}"

                        headers = {
                            'Authorization': f"Bearer {st.session_state.data['jwt_token']}",
                            'accept': 'application/json',
                            'Content-Type': 'application/json'
                        }

                        update_json = {
                            "relations": {
                                "00000000-0000-0000-0000-000000007080:TARGET": [
                                {
                                    "id": f"{source_code_resource_id}"
                                }
                                ],
                                "00000000-0000-0000-0000-000000007081:TARGET": [
                                {
                                    "id": f"{target_code_resource_id}"
                                }
                                ],
                                "00000000-0000-0000-0000-000000007082:TARGET": [
                                {
                                    "id": f"{xwalk_resource_id}"
                                }
                                ]
                            }
                        }

                        x = requests.patch(url=url, headers=headers, json=update_json)
                        st.write('Update to Collibra in progress.')
                        time.sleep(5)
                        if x.status_code == 200:
                            st.write('Update to Collibra successful.')
                            session.sql(f'''
                            UPDATE
                                NYU_REFDATA_XWALK
                            SET
                                REFDATA_TARGET_CODE = '{target_code}',
                                REFDATA_CROSSWALK_REFDATA_TARGET_CODE_RESOURCE_ID = '{target_code_resource_id}',
                                UPDATE_TIMESTAMP = CURRENT_TIMESTAMP
                            WHERE
                                REFDATA_CROSSWALK_MAPPING_RESOURCE_ID = '{mapping_resource_id}'
                            ''').collect()

                            st.write(f'Updated {source_code} -> {target_code} from previously {old_target_code} in Snowflake table NYU_REFDATA_XWALK.')
                            time.sleep(3)
                            st.rerun()
                        else:
                            st.write('Update to Collibra failed.')
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