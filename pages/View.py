import streamlit as st
from snowflake.snowpark.context import get_active_session

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

if 'data' in st.session_state and bool(st.session_state.data['netid']):
    st.title(f'''View Crosswalk Mappings''')

    crosswalk_options = session.sql('''
        SELECT DISTINCT REFDATA_CROSSWALK_RESOURCE_ID, REFDATA_CROSSWALK_NAME
        FROM NYU_REFDATA_XWALK
    ''').to_pandas()

    crosswalk_name = st.selectbox(
        "Select crosswalk to view",
        crosswalk_options['REFDATA_CROSSWALK_NAME'], index=None
    )

    if crosswalk_name is not None:
        current_crosswalk = session.sql(f"""
            SELECT  
                a.REFDATA_SOURCE_CODE, b.REFDATA_CODE_DESCRIPTION AS REFDATA_SOURCE_CODE_DESCRIPTION, 
                a.REFDATA_TARGET_CODE, c.REFDATA_CODE_DESCRIPTION AS REFDATA_TARGET_CODE_DESCRIPTION,
                a.REFDATA_CROSSWALK_DATA_STEWARD_NETID, a.REFDATA_CROSSWALK_DATA_CUSTODIAN_NETID, 
                a.CREATED_TIMESTAMP, a.UPDATE_TIMESTAMP
            FROM NYU_REFDATA_XWALK a
            LEFT JOIN NYU_REFDATA_CODESET b ON a.REFDATA_SOURCE_CODE = b.REFDATA_CODE AND a.REFDATA_SOURCE_CODESET = b.REFDATA_CODESET
            LEFT JOIN NYU_REFDATA_CODESET c ON a.REFDATA_TARGET_CODE = c.REFDATA_CODE AND a.REFDATA_TARGET_CODESET = c.REFDATA_CODESET
            WHERE a.REFDATA_CROSSWALK_NAME = '{crosswalk_name}'
            ORDER BY a.UPDATE_TIMESTAMP DESC             
        """).to_pandas().dropna()

        st.dataframe(current_crosswalk.rename(columns={
            'REFDATA_SOURCE_CODE': 'Source Code',
            'REFDATA_SOURCE_CODE_DESCRIPTION': 'Source Description',
            'REFDATA_TARGET_CODE': 'Target Code',
            'REFDATA_TARGET_CODE_DESCRIPTION': 'Target Description',
            'REFDATA_CROSSWALK_DATA_STEWARD_NETID': 'Crosswalk Data Steward(s)',
            'REFDATA_CROSSWALK_DATA_CUSTODIAN_NETID': 'Crosswalk Data Custodian(s)',
            'CREATED_TIMESTAMP': 'Created',
            'UPDATE_TIMESTAMP': 'Last Updated'
        }), column_config=(st.column_config.Column(width='large')), use_container_width=True)
else:
    st.text('Retrieving User NetID failed. Go back to Main Page to restart the app to try again.')