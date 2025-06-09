import streamlit as st
from snowflake.snowpark.context import get_active_session
import pandas as pd
import requests
import base64
import ast
import _snowflake

from streamlit_extras.app_logo import add_logo

session = get_active_session()
st.set_page_config(layout='wide')

st.title('Crosswalk Maintenance')

filepath = 'logo.png'
mime_type = filepath.split('.')[-1:][0].lower()
with open(filepath, "rb") as f:
    content_bytes = f.read()
    content_b64encoded = base64.b64encode(content_bytes).decode()
    image_string = f'data:image/{mime_type};base64,{content_b64encoded}'
    st.image(image_string, width=200)

st.markdown('Welcome to the :blue-background[Data Crosswalk Maintenance Interface].')
st.markdown('You are here because you are identified as either a :violet[***Data Steward***] or :violet[***Data Custodian***] for an NYU Data Crosswalk Mapping.')


def connect_collibra():
    st.write('Connecting to Collibra...')
    pw = ast.literal_eval(session.sql('SELECT get_test_pw()').collect()[0][0])
    username = pw['Username']
    password = pw['Password']

    client_pw = ast.literal_eval(session.sql('SELECT get_client_test_pw()').collect()[0][0])
    client_id = client_pw['Username']
    client_secret = client_pw['Password']

    auth_url = 'https://qa.auth.it.nyu.edu/oauth2/token'
    collibra_host = "nyu-test.collibra.com"
    collibra_url = f'https://{collibra_host}:443/rest/2.0/outputModule/export/json'

    auth_header = f'{client_id}:{client_secret}'
    auth_header_bytes = auth_header.encode('ascii')
    base64_bytes = base64.b64encode(auth_header_bytes)
    base64_auth_header = base64_bytes.decode('ascii')

    # Set up the payload and headers for the auth request
    auth_payload = {
        'grant_type': 'password',
        'username': username,
        'password': password,
        'scope': 'openid'
    }
    auth_headers = {
    'Authorization': f'Basic {base64_auth_header}',
    'Content-Type': 'application/x-www-form-urlencoded'
    }


    auth_response = requests.post(auth_url, data=auth_payload, headers=auth_headers, verify=False)

    #st.write(auth_response.status_code)
    #st.write(auth_response.json())

    st.session_state.data['status'] = auth_response.status_code

    # Check the response and extract the token
    if auth_response.status_code == 200:
        jwt_token = auth_response.json().get('id_token')
        st.session_state.data['jwt_token'] = jwt_token
    else:
        st.write(f"Failed to retrieve token: {auth_response.status_code}")
        st.write(auth_response.text)
        jwt_token = None

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


if 'data' not in st.session_state:
    st.session_state.data = {'netid': st.experimental_user.user_name.lower()}
    connect_collibra()
    st.rerun()
else:
    st.markdown(f"Viewing the app as User :rainbow[{st.session_state.data['netid']}].")
    if st.session_state.data['status'] == 200:
        st.write('Connected to Collibra.')

        st.subheader('Insert')
        st.write('Select a crosswalk from the drop-down menu to insert a mapping to. All unmapped codes from the corresponsing source codeset will be displayed. Scroll down to select source code and target code to insert a mapping.')

        st.subheader('Update')
        st.write('Select a crosswalk from the drop-down menu of which an existing mapping the user would like to update. Select a new target code from the drop-down menu to update the mapping.')

        st.subheader('View')
        st.write('Select from the drop-down menu to view all mappings within a crosswalk.') 
    else:
        st.write('Connection to Collibra failed.')