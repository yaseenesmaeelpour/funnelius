import streamlit as st
import pandas as pd
import numpy as np
from functions import transform, apply_filter, aggregate, draw

# setting initial parameters //////////////////////////////////////////////////////
max_edge_width = 20
first_actions_filter =[]
goals = ['']
max_path_num = 0
min_edge_count = 0
has_compare = 0
gradient_metric = 'conversion-rate'
gradient = [[255,255,255],[255,255,255],[255,255,255]]
gradient_lookup = {
    'Red -> White -> Green':[[255,205,205],[255,255,255],[205,255,205]],
    'Green - > White -> Red':[[205,255,205],[255,255,255],[255,205,205]],
    'Red -> White':[[255,205,205],[255,230,230],[255,255,255]],
    'White -> Green':[[255,255,255],[230,255,230],[205,255,205]]
}

metric_lookup = {
    'conversion-rate':'Conversion Rate',
    'duration-median':'Duration',
    'percent-of-total':'% of Total Users',
    'users':'Users'
}


st.sidebar.title('Funnelius')


st.sidebar.subheader("Load Data", divider="gray")
csv_file = st.sidebar.file_uploader("Choose a CSV file", accept_multiple_files=False)
if csv_file is not None:
    raw_data = pd.read_csv(csv_file)


    # compare file //////////////////////////////////////////////////////////////////////////////////
    compare = st.sidebar.checkbox("Compare with another file", value = False)
    if compare == True:
        compare_file = st.sidebar.file_uploader("Choose a CSV file to compare", accept_multiple_files=False)
        if compare_file is not None:
            compare_data = pd.read_csv(compare_file)
            has_compare = 1

    data, first_actions, all_actions = transform(raw_data)
    if has_compare == 1:
        data_compare, __v1, all_actions_compare = transform(compare_data)
    


    st.sidebar.subheader("Filters", divider="gray")
    first_actions_filter = st.sidebar.multiselect(
        "Only paths that start from?",
        first_actions,
        default=[],
    ) 
    goals = st.sidebar.multiselect(
        "Steps that show funnel completion",
        all_actions,
        default=[],
    ) 

    data, route_num =  apply_filter(data, first_actions_filter, goals)
    if has_compare == 1:
        data_compare, route_num_compare = apply_filter(data_compare, first_actions_filter, goals)
        route_num = max(route_num, route_num_compare)
    
    max_routes = st.sidebar.slider('Maximum paths to show', min_value=1, max_value=route_num, value=route_num) 
 
    data_node, data_edge, data_answer = aggregate(data, max_routes)
    if has_compare == 1:
        data_compare_node, data_compare_edge, data_compare_answer = aggregate(data_compare, max_routes)


        #add compare data to original data   
        
        #merge data with compare_data
        data_node = pd.merge(data_node, data_compare_node[['action','conversion_rate','users','duration_median','duration_mean','percent_of_total']], on='action', how='left', suffixes =('','_compare')) #add nodes compare data
        data_edge = pd.merge(data_edge, data_compare_edge[['action','action_next','edge_count']], on=['action','action_next'], how='left', suffixes =('','_compare')) #add edge compare data
        data_answer  = pd.merge(data_answer, data_compare_answer[['action','answer','answer_percent']], on=['action','answer'], how='left', suffixes =('','_compare')) #add edge compare data
        
        #calculate increase/decrease percentages for nodes
        metrics = ['conversion_rate','duration_median','percent_of_total','users']
        for metric in metrics:
            data_node[metric+'_change'] = data_node[metric]/data_node[metric+'_compare'] - 1

        #calculate increase/decrease percentages for answers
        data_answer['answer_percent_change'] = data_answer['answer_percent']/data_answer['answer_percent_compare'] - 1

        #calculate increase/decrease percentages for edges
        data_edge['edge_count_change'] = data_edge['edge_count']/data_edge['edge_count_compare'] - 1
        
        #add nodes that were present in comparison data but not in original data
        nodes_only_in_comparison = data_compare_node[~data_compare_node['action'].isin(data_node['action'])]
        nodes_only_in_comparison = nodes_only_in_comparison[['action']]
        nodes_only_in_comparison[['users', 'percent_of_total']] = 0
        nodes_only_in_comparison[['conversion_rate', 'duration_median', 'duration_mean',
        'conversion_rate_change', 'duration_median_change']] = np.nan
        nodes_only_in_comparison[['users_change', 'percent_of_total_change']] = -1
        data_node = pd.concat([data_node, nodes_only_in_comparison])

        #add edges that were present in comparison data but not in original data
        edges_only_in_comparison = data_compare_edge.merge(data_edge, on=['action','action_next'], how='left', indicator=True)
        edges_only_in_comparison = edges_only_in_comparison[edges_only_in_comparison['_merge'] == 'left_only'].drop(columns=['_merge'])
        edges_only_in_comparison = edges_only_in_comparison[['action', 'action_next']]
        edges_only_in_comparison['edge_count'] = 0
        edges_only_in_comparison['edge_count_change'] = -1
        data_edge = pd.concat([data_edge, edges_only_in_comparison])


    metrics = st.sidebar.pills('Metrics to show', ['users','conversion-rate','percent-of-total','duration-median'], selection_mode = 'multi', 
    default = ['users','conversion-rate','percent-of-total','duration-median'], format_func = lambda option: metric_lookup[option])
    
    show_drop = st.sidebar.checkbox("Show drops", value = True)
    show_answer = st.sidebar.checkbox("Show Answer contriburion", value = False)

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
            gradient_metric = st.sidebar.selectbox('Metric', ('conversion-rate', 'duration-median', 'percent-of-total', 'users'), format_func = lambda option: metric_lookup[option] )
      
    # Draw chart and load it into sttreamlit //////////////////////////////////////
    draw(data_node, data_edge, data_answer, goals, min_edge_count, max_edge_width, general_file_name, show_drop, show_answer, ['svg','pdf'], gradient, gradient_metric, metrics = metrics)
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



