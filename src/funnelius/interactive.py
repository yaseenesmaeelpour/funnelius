import streamlit as st
import pandas as pd
from functions import transform, apply_filter, aggregate, draw

# setting initial parameters //////////////////////////////////////////////////////
max_edge_width = 20
first_activities_filter =[]
goals = ['']
max_path_num = 0
min_edge_count = 0
gradient_metric = 'conversion-rate'
gradient = [[255,255,255],[255,255,255],[255,255,255]]
gradient_lookup = {
    'Red -> White -> Green':[[255,205,205],[255,255,255],[205,255,205]],
    'Green - > White -> Red':[[205,255,205],[255,255,255],[255,205,205]],
    'Red -> White':[[255,205,205],[255,230,230],[255,255,255]],
    'White -> Green':[[255,255,255],[230,255,230],[205,255,205]]
}

metric_lookup = {
    'Conversion Rate':'conversion-rate',
    'Duration':'duration-median',
    '% of Total Users':'percent-of-total',
    'Users':'users'
}


st.sidebar.title('Funnelius')


st.sidebar.subheader("Load Data", divider="gray")
csv_file = st.sidebar.file_uploader("Choose a CSV file", accept_multiple_files=False)
if csv_file is not None:
    raw_data = pd.read_csv(csv_file)


    compare = st.sidebar.checkbox("Compare with another file", value = False)
    if compare == True:
        compare_file = st.sidebar.file_uploader("Choose a CSV file to compare", accept_multiple_files=False)
        if compare_file is not None:
            compare_data = pd.read_csv(compare_file)
            # data = compare(data, compare_data)


    data, first_activities, all_activities = transform(raw_data)


    st.sidebar.subheader("Filters", divider="gray")
    first_activities_filter = st.sidebar.multiselect(
        "Only paths that start from?",
        first_activities,
        default=[],
    ) 
    goals = st.sidebar.multiselect(
        "Steps that show funnel completion",
        all_activities,
        default=[],
    ) 

    data, route_num =  apply_filter(data, first_activities_filter, goals)
    
    max_routes = st.sidebar.slider('Maximum paths to show', min_value=1, max_value=route_num, value=route_num) 
 
    data = aggregate(data, max_routes)

    show_drop = st.sidebar.checkbox("Show drops", value = True)

    general_file_name = csv_file.name.split('.')[0]

    
    # Conditional formating settings///////////////////////////////////////////////////////////////
    st.sidebar.subheader("Conditional Formatting", divider="gray")
    with st.sidebar.expander("See explanation"):
        conditional = st.sidebar.checkbox("Apply Conditional Formatting", value = False)
        if conditional == True:
            gradient = st.sidebar.selectbox('Gradient Color', ('Red -> White -> Green', 'Green - > White -> Red',  'Red -> White', 'White -> Green'))
            gradient = gradient_lookup[gradient]
            
            html = '<div width="100%" style="background: #FFDCDC;background: linear-gradient(90deg'
            for i in range(0,3):
                html += ',rgba('
                html += ', '.join(str(gradient[i][j]) for j in range(0,3))
                html += ', 1) '+str(i*50)+'%'
            html += ');"> &nbsp;</div>'

            st.sidebar.html(html)
            gradient_metric = st.sidebar.selectbox('Metric', ('Conversion Rate', 'Duration', '% of Total Users', 'Users'))
            gradient_metric = metric_lookup[gradient_metric]
        
    # Draw chart and load it into sttreamlit //////////////////////////////////////
    draw(data, goals, min_edge_count, max_edge_width, general_file_name, show_drop, ['svg','pdf'], gradient, gradient_metric)
    st.image(general_file_name+'.svg',width=1000)

    #export part of sidebar///////////////////////////////////////////////////////////////
    st.sidebar.subheader("Export", divider="gray")
    with open(general_file_name+'.pdf', 'rb') as file:
        st.sidebar.download_button(
            label='Download PDF',
            data=file,
            file_name=general_file_name+'.pdf',
            mime='image/pdf',
            icon=':material/download:',
        )
else:
    st.info('Please load a csv file from left sidebar.', icon="ℹ️")



