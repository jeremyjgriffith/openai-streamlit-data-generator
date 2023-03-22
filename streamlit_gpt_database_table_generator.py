import streamlit as st
import pandas as pd
import json

from io import StringIO

import openai
openai.api_key_path='./env/openai.key'

from snowflake.snowpark.session import Session

# Streamlit config
st.set_page_config(
   page_title="AI Database Table Generator",
   page_icon="🤖",
   initial_sidebar_state="expanded",
)

def create_session():
    session = Session.builder.configs({
        'account'     : st.session_state['sf_account'],
        'user'        : st.session_state['sf_user'],
        'password'    : st.session_state['sf_password'],
        'role'        : st.session_state['sf_role'],
        'warehouse'   : st.session_state['sf_warehouse'],
        'database'    : st.session_state['sf_database'],
        'schema'      : st.session_state['sf_schema'],
    }).create()
    st.session_state['snowpark_session'] = session

def get_session_parameters():
    snowflake_environment = st.session_state['snowpark_session'].sql('select current_user(), current_role(), current_database(), current_schema(), current_version(), current_warehouse()').to_pandas()
    snowflake_environment = snowflake_environment.T
    snowflake_environment.columns = ['VALUE']
    return snowflake_environment

with st.sidebar:
    # load snowflake connection variables from ./env/connection.json
    try: 
        connection_params = json.load(open("./env/connection.json"))
        st.session_state['sf_account'] = connection_params['account']
        st.session_state['sf_user'] = connection_params['user']
        st.session_state['sf_password'] = connection_params['password']
        st.session_state['sf_warehouse'] = connection_params['warehouse']
        st.session_state['sf_role'] = connection_params['role']
        st.session_state['sf_database'] = connection_params['database']
        st.session_state['sf_schema'] = connection_params['schema']
    except:
        pass 

    st.text_input("Snowflake Account:", key='sf_account')
    st.text_input("User Name:", key='sf_user')
    st.text_input("Password:", type='password' , key='sf_password')
    st.text_input("Role:", key='sf_role')
    st.text_input("Warehouse:", key='sf_warehouse')
    st.text_input("Database:", key='sf_database')
    st.text_input("Schema:", key='sf_schema')
    st.button("Connect", on_click=create_session)

    if 'snowpark_session' in st.session_state:
        st.dataframe(get_session_parameters())

def update_column_name_editor():
    columns_df = pd.DataFrame(st.session_state['response_df'].columns, columns=['Original Column Names']).astype(str)
    columns_df['New Column Names'] = columns_df['Original Column Names']
    st.session_state['column_names'] = columns_df

def ai_data(prompt):
    messages = [{"role":"user","content":prompt}]
    response = openai.ChatCompletion.create(model="gpt-3.5-turbo", messages=messages, temperature=0, max_tokens=2000)
    
    raw_response = st.expander("See raw GPT response")
    raw_response.write(response)
    
    response_text = response['choices'][0]['message']['content']
    response_df = pd.read_csv(StringIO(response_text), sep='|', header=None)

    st.session_state['response_df'] = response_df

    update_column_name_editor()

def update_column_names():
    new_data = pd.DataFrame(data_editor)
    new_data.columns = column_name_data_editor['New Column Names']
    st.session_state['response_df'] = new_data



st.title("AI Database Table Generator")
raw_response = st.empty() 
column_name_data_editor = st.empty()
data_editor = st.empty()

with st.form("ai_generator"):
    prompt = st.text_input("Data Prompt:")
    submitted = st.form_submit_button("Get Data")

    if submitted:
        ai_data(prompt=prompt)

if "response_df" in st.session_state: 
    st.markdown("Modify the column names by editing the `New Column Name` field and click `Update Column Names`")                   
    col1, col2 = st.columns(2)
    with col1:
        column_name_data_editor = st.experimental_data_editor(st.session_state['column_names'])
    with col2:
        
        st.button("Update Column Names", on_click=update_column_names)
    
    st.markdown("Data to save to a table. You may add or delete rows.")
    data_editor = st.experimental_data_editor(st.session_state['response_df'], use_container_width=True, num_rows='dynamic')

    table_name = st.text_input("Table name:")

    load_table = st.button("Load Table")

    if load_table:
        try: 
            session = st.session_state['snowpark_session']
            sdf = session.create_dataframe(data_editor)
            sdf.write.save_as_table(table_name)
        except:
            st.write("Are you connected to Snowflake?")
