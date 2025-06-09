import requests
import base64
import streamlit as st
from snowflake.snowpark.context import get_active_session
import pandas as pd

session = get_active_session()

username = 'nyu10191'
password = 'SnowflakeCollibraIntegration0207!'
client_id = 'WLCpSLmXRXkboHt7vdkVg8ODwI4a'
client_secret = '_DWudeAAvmWXsehT__3FDB46I98a'
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

st.write(auth_response.status_code)
st.write(auth_response.json())

# Check the response and extract the token
if auth_response.status_code == 200:
    jwt_token = auth_response.json().get('id_token')
    st.write("JWT Token: ", jwt_token)
else:
    st.write(f"Failed to retrieve token: {auth_response.status_code}")
    st.write(auth_response.text)
    jwt_token = None
# If the token was retrieved successfully, use it to make the Collibra request

# Use the JWT token to invoke the Collibra endpoint
if jwt_token:
    collibra_headers = {
        'Authorization': f'Bearer {jwt_token}',
        'Content-Type': 'application/json' # Adjust the Content-Type as needed
    }


tvc = {"TableViewConfig":{"displayLength":-1,"displayStart":0,"Resources":{"Term":{"Signifier":{"name":"tableFullName"},"Id":{"name":"tableId"},"DisplayName":{"name":"tableName"},"Status":{"Signifier":{"name":"tableStatus"}},"ConceptType":[{"Id":{"name":"assetTypeId"},"Signifier":{"name":"assetTypeName"}}],"Vocabulary":{"Id":{"name":"tableLibraryId"},"Name":{"name":"tableLibraryName"},"Community":{"Id":{"name":"tableCommunityId"},"Name":{"name":"tableCommunityName"}}},"StringAttribute":[{"labelId":"00000000-0000-0000-0000-000000003114","LongExpression":{"name":"tableDescription"}}],"Attribute":[{"labelId":"65df38c5-d820-4631-bfe6-ac1799ad471d","Value":{"name":"tableIsMonitored"}}],"Relation":[{"typeId":"00000000-0000-0000-0000-000000007043","type":"TARGET","Source":{"Signifier":{"name":"schemaFullName"},"DisplayName":{"name":"schemaName"},"Id":{"name":"schemaId"}}},{"typeId":"00000000-0000-0000-0000-000000007042","type":"TARGET","Source":{"Signifier":{"name":"columnFullName"},"DisplayName":{"name":"columnName"},"Id":{"name":"columnId"},"StringAttribute":[{"labelId":"00000000-0000-0000-0000-000000003114","LongExpression":{"name":"columnDescription"}}],"Attribute":[{"labelId":"3ab4fa3c-86c4-4af6-988f-84c42789289e","Value":{"name":"columnIsDisplay"}},{"labelId":"65df38c5-d820-4631-bfe6-ac1799ad471d","Value":{"name":"columnIsMonitored"}},{"labelId":"00000000-0000-0000-0000-000000000219","value":{"name":"columnDataType"}},{"labelId":"00000000-0000-0000-0001-000500000015","value":{"name":"columnIsKeyFlag"}},{"labelId":"00000000-0000-0000-0001-000500000020","value":{"name":"columnOrder"}}],"Relation":[{"typeId":"00000000-0000-0000-0000-000000007066","type":"TARGET","Source":{"Signifier":{"name":"dqRuleFullName"},"Id":{"name":"dqRuleId"},"DisplayName":{"name":"dqRuleName"},"LastModified":{"name":"dqRuleModifiedTime"},"Status":{"Signifier":{"name":"dqRuleStatus"}},"Tag":{"Name":{"name":"tagName"},"AssetsCount":{"name":"assetsCount"}}}}],"Filter":{"OR":[{"Field":{"name":"columnIsKeyFlag","operator":"EQUALS","value":"true"}},{"Field":{"name":"columnIsMonitored","operator":"EQUALS","value":"true"}},{"Field":{"name":"columnIsDisplay","operator":"EQUALS","value":"true"}}]}}}],"Filter":{"AND":[{"Field":{"name":"tableIsMonitored","operator":"EQUALS","value":"true"}},{"Field":{"name":"assetTypeName","operator":"IN","values":["Table","Database View"]}},{"Field":{"name":"tagName","operator":"EQUALS","value":"Census_DQ"}}]}}},"Columns":[{"Column":{"fieldName":"tableId"}},{"Column":{"fieldName":"tableFullName"}},{"Column":{"fieldName":"tableName"}},{"Column":{"fieldName":"tableStatus"}},{"Column":{"fieldName":"tableLibraryName"}},{"Column":{"fieldName":"tableDescription"}},{"Group":{"name":"columns","Columns":[{"Column":{"fieldName":"columnFullName"}},{"Column":{"fieldName":"columnName"}},{"Column":{"fieldName":"columnDataType"}},{"Column":{"fieldName":"columnOrder"}},{"Column":{"fieldName":"columnDescription"}},{"Column":{"fieldName":"columnIsKeyFlag"}},{"Column":{"fieldName":"columnId"}}]}},{"Column":{"fieldName":"schemaFullName"}},{"Column":{"fieldName":"schemaName"}},{"Column":{"fieldName":"schemaId"}}]}}
exportPath = 'outputModule/export/json'
results = requests.post(collibra_url,  headers=collibra_headers, json=tvc)

st.dataframe(pd.DataFrame(data=results.json()['aaData']))

if ( int(results.status_code / 100) == 4 ):
    st.write('|-COLLIBRA-EXTRACT-| Unable to get post request ('+results.text+')')
elif (int(results.status_code / 100) == 2 ):
    st.write('|-COLLIBRA-EXTRACT-| extract  Successful')

# asset count
jsonData = results.json()
count = jsonData['iTotalRecords']

st.dataframe(pd.DataFrame(data=jsonData['aaData']))