import streamlit as st
import pandas as pd
from functions import transform, apply_filter, aggregate, draw

#settings
max_edge_width = 20

first_activities_filter =[]
goals = ['']
max_path_num = 0
min_edge_count = 0



# step = st.sidebar.slider('Hide Paths with users less than')

st.sidebar.title('Non-Linear Funnel Analyser')
csv_file = st.sidebar.file_uploader("Choose a CSV file", accept_multiple_files=False)

if csv_file is not None:
    raw_data = pd.read_csv(csv_file)

    data, first_activities, all_activities = transform(raw_data)

    first_activities_filter = st.sidebar.multiselect(
        "Only show paths that start from?",
        first_activities,
        default=[],
    ) 
    goals = st.sidebar.multiselect(
        "Select steps that shows funnel completion",
        all_activities,
        default=[],
    ) 

    data, route_num =  apply_filter(data, first_activities_filter, goals)
    
    max_routes = st.sidebar.slider('Maximum paths to show', min_value=1, max_value=route_num, value=route_num) 
 
    data = aggregate(data, max_routes)

    show_drop = st.sidebar.checkbox("Show dropped users", value = True)

    general_file_name = csv_file.name.split('.')[0]

    draw(data, goals, min_edge_count, max_edge_width, 'general_file_name', show_drop, ['svg','pdf'])
    st.image(general_file_name+'.svg',width=1000)
    with open(general_file_name+'.pdf', 'rb') as file:
        st.sidebar.download_button(
            label='Download Graph',
            data=file,
            file_name=general_file_name+'.pdf',
            mime='image/pdf',
            icon=':material/download:',
        )
else:
    st.info('Please load a csv file from left sidebar.', icon="ℹ️")



