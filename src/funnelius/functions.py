import pandas as pd
import graphviz
import numpy as np

def transform(df):
    #convert action start to timeformat in python
    df['action_start'] = pd.to_datetime(df['action_start'])

    #create a column to show order of happened actions
    df['action_order'] = df.groupby('user_id')['action_start'].rank(method='first').astype(int)

    #create a dataframe for the very first action of every user
    df_first_action = df[df['action_order']==1][['user_id','action']]
    df_first_action.rename(columns={'action': 'first_action'}, inplace=True)

    #join it to data frame to have first action of every user. this will be used to filter based on first action
    data = pd.merge(df, df_first_action, on='user_id', how='left')

    #create_next_action dataframe
    data_next = data.copy()
    data_next['action_order'] -= 1

    #join with original data to have next action for every action
    data = pd.merge(data, data_next[['user_id', 'action_order', 'action', 'action_start']],
                on=['user_id','action_order'], how='left', suffixes =('','_next'))

    #caculate time spent by user_id in the action
    data['duration'] = data.apply( lambda row: np.nan if pd.isna(row['action_next'])
                               else (row['action_start_next'] - row['action_start']).total_seconds(), axis =1 )


    #Drop action_start_next column as it is no longer needed
    data.drop(columns=['action_start_next'], inplace=True)

    #add start Node
    start_rows = data.groupby('user_id').agg({'action_start': 'min', 'first_action': 'first'})
    start_rows = start_rows.reset_index()
    start_rows['action'] = 'Start'
    start_rows['action_order'] = 0
    start_rows['action_next'] = start_rows['first_action']
    data = pd.concat([data, start_rows[['user_id', 'action', 'action_start', 'action_order', 'first_action', 'action_next']]])
    
    #calculate filter variables 
    first_actions = data[data['action_order']==1]['action'].drop_duplicates().tolist()
    all_actions = data['action'].drop_duplicates().tolist()
    
    
    return data, first_actions, all_actions


def apply_filter(df, first_actions_filter, goals):
    #filter based on first actions
    if first_actions_filter != []:
        df = df[df['first_action'].isin(first_actions_filter) ]

    #decide how to show end point for user
    df['action_next'] = df.apply( lambda row: 'End' if pd.isna(row['action_next']) and row['action'] in goals else 
                                    ('Drop'+row['action'] if pd.isna(row['action_next']) else row['action_next']), axis=1)

    #Calculate priority of route
    df_user_route = df.sort_values(['user_id','action_order']).groupby('user_id')['action'].apply(lambda x: ' '.join(x)).reset_index(name='actions')
    df_route_priority = df_user_route.groupby('actions').agg(user_count = ('user_id','count')).reset_index().sort_values(by=['user_count'], ascending = False)
    df_route_priority['route_order'] = df_route_priority['user_count'].sort_values(ascending = False).rank(method='first', ascending=False).astype(int)
    df_user_route = pd.merge(df_user_route, df_route_priority[['actions', 'route_order']], on = 'actions', how = 'left')[['user_id','route_order']]
    df = pd.merge(df,df_user_route, on = 'user_id', how = 'left')
    route_num = df_user_route['route_order'].max()
    
    return df, route_num

def aggregate(df, route_num):
    # filter maximum routes
    df = df[df['route_order'] <= route_num]

    #calculte nodes aggregated data///////////////////////////////////////////////////////////////////////////////
    node_agg_data = df.groupby('action').agg(
        duration_median = ('duration', 'median'),
        duration_mean = ('duration', 'mean'),
        users = ('user_id', 'count'),
        conversion_rate = ('user_id', lambda x: 1.0 - sum(df.loc[x.index, 'action_next'].str[:4] == 'Drop') / len(x)),
    ).reset_index()   

    #some nodes only appear in action_next column but still we should draw them
    missing_end_points = df[~df['action_next'].isin(df['action'])]
    missing_end_points = missing_end_points.groupby('action_next').agg(
        users = ('user_id', 'count')
    ).reset_index()  
    missing_end_points.rename(columns={'action_next':'action'}, inplace=True)
    
    node_agg_data = pd.concat([node_agg_data, missing_end_points])
    

    #calcualte total users
    total_users = int(df[df['action'] == 'Start']['user_id'].count())
    node_agg_data['percent_of_total'] =  node_agg_data.apply( lambda row: row['users']/total_users , axis=1)
    #//////////////////////////////////////////////////////////////////////////////////////////////////////////
    
    #Calculate aggregated answer data ////////////////////////////////////////////////////////////////////////
    answer_agg_data = df.groupby(['action','answer']).agg(answer_count = ('answer','count'))
    answer_agg_data = answer_agg_data.reset_index()
    answer_agg_data = answer_agg_data.sort_values(['action', 'answer_count'], ascending=[True, False])
    answer_agg_data['answer_order'] = answer_agg_data.groupby('action').cumcount() + 1
    answer_agg_data.loc[answer_agg_data['answer_order'] > 5, 'answer'] = 'Other items'
    answer_agg_data = answer_agg_data.groupby(['action','answer']).agg(answer_count = ('answer_count','sum'))
    answer_agg_data = answer_agg_data.reset_index()
    answer_agg_data = answer_agg_data.sort_values(['action', 'answer_count'], ascending=[True, False])
    answer_total = answer_agg_data.groupby('action').agg(total = ('answer_count','sum')).reset_index()
    answer_agg_data = pd.merge(answer_agg_data,answer_total,on='action',how='left')
    answer_agg_data['answer_percent'] = answer_agg_data['answer_count']/answer_agg_data['total']
    answer_agg_data = answer_agg_data.drop('total', axis=1)
    #/////////////////////////////////////////////////////////////////////////////////////////////////////////

    #calculate edges aggregated data 
    edge_agg_data = df.groupby(['action', 'action_next']).agg(edge_count=('user_id', 'count')).reset_index()

    #merge action and edge data
    # agg_data = pd.merge(edge_agg_data, node_agg_data, on='action', how='left')

    return node_agg_data, edge_agg_data, answer_agg_data


def draw(node_data, edge_data, answer_data, goals, min_edge_count, max_edge_width, title, show_drop, show_answer, 
export_formats, conditional_format_gradient=[[255,200,200],[255,255,255],[200,255,200]], 
conditional_format_metric = 'conversion-rate', metrics=['conversion-rate','users','percent-of-total','duration-median']):
    #set parameters
    excluded_actions = ['Start', 'End'] + goals
    bgcolor = '#%02x%02x%02x' % (255, 255, 255)
    
    #check if dataframe hase comparison data
    has_comparison_data = 0
    if 'users_compare' in node_data.columns:
        has_comparison_data = 1

    #initialize graphvize engine
    dot = graphviz.Digraph(comment='')
    dot.attr(label='', labelloc='top', fontsize='20', fontcolor='black', bgcolor=bgcolor)

    # Draw nodes //////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    for index, node in node_data.iterrows():
        #prepare label variables
        label_users = str(node['users'])
        label_percent_of_total = format(node['percent_of_total'], '.0%')
        label_conversion_rate = format(node['conversion_rate'], '.0%')
        label_duration_median = format(node['duration_median'], '.1f')
        if has_comparison_data == 1:
            change_vars = {}
            change_metrics = ['users','conversion_rate','duration_median','percent_of_total']
            for metric in change_metrics:
                if node[metric+'_change'] == 0:
                    text_format = '.0%'
                else:
                    text_format = '+.0%'
                change_vars['label_'+metric+'_change'] = format(node[metric+'_change'], text_format)
                #set label colors
                if node[metric+'_change'] > 0:
                    change_vars['color_'+metric+'_change'] = '#006400'
                elif node[metric+'_change'] < 0:
                    change_vars['color_'+metric+'_change'] =  '#8b0000'
                else:
                    change_vars['color_'+metric+'_change'] = '#000'

        if node['action'] == 'Start' or node['action'] == 'End':
            color = '#fff'
            shape = 'circle'
            label = node['action']

        elif node['action'][:4] == 'Drop':
            color = '#ffc8c8'
            shape = 'cds'
            label = label_percent_of_total+' ('+label_users+')'

        else:
            shape = 'box'
            color = "#fff"
            if has_comparison_data == 1:
                colspan = '3'
                users_change_code = '<TD><B><FONT COLOR="'+change_vars['color_users_change']+'">(' + change_vars['label_users_change'] +')</FONT></B></TD>'
                conversion_rate_change_code = '<TD><B><FONT COLOR="'+change_vars['color_conversion_rate_change']+'">('+ change_vars['label_conversion_rate_change']+')</FONT></B></TD>'
                percent_of_total_change_code = '<TD><B><FONT COLOR="'+change_vars['color_percent_of_total_change']+'">('+ change_vars['label_percent_of_total_change']+')</FONT></B></TD>'
                duration_median_change_code = '<TD><B><FONT COLOR="'+change_vars['color_duration_median_change']+'">('+ change_vars['label_duration_median_change']+')</FONT></B></TD>'
            else:
                colspan = '2'
                users_change_code = ''
                conversion_rate_change_code = ''
                percent_of_total_change_code = ''
                duration_median_change_code = ''

            label = '<<TABLE BORDER="0" CELLBORDER="0" CELLPADDING="1" CELLSPACing="0" BGCOLOR="transparent" STYLE="rounded">'
            label += '<TR><TD COLSPAN="' + colspan + '" ALIGN="CENTER"><B>' + node['action'] + '</B></TD></TR>'
            label += '<TR><TD COLSPAN="' + colspan + '" ALIGN="CENTER" BGCOLOR="#aaa"></TD></TR>'
            
            if 'users' in metrics:
                label += '<TR><TD ALIGN="LEFT">Users:</TD><TD>' + label_users + '</TD>' + users_change_code + '</TR>'
            if 'percent-of-total' in metrics:
                label += '<TR><TD ALIGN="LEFT">% of total users:</TD><TD>' + label_percent_of_total + '</TD>' + percent_of_total_change_code + '</TR>'
            if not node['action'] in goals:
                if 'conversion-rate' in metrics:
                    label += '<TR><TD ALIGN="LEFT">Conversion:</TD><TD>' + label_conversion_rate + '</TD>' + conversion_rate_change_code + '</TR>'
                if 'duration-median' in metrics:
                    label += '<TR><TD ALIGN="LEFT">Duration (sec):</TD><TD>' + label_duration_median + '</TD>' + duration_median_change_code +'</TR>'

            #add answers//////////////////////////////////////////////////////////////////////////////////////////////////////////////////
            if show_answer == True:
                action_answers = answer_data[answer_data['action'] == node['action']]
                if len(action_answers) > 0:
                    label += '<TR><TD COLSPAN="' + colspan + '" ALIGN="CENTER"><B>Answers</B></TD></TR>'
                    label += '<TR><TD COLSPAN="' + colspan + '" ALIGN="CENTER" BGCOLOR="#aaa"></TD></TR>'
                    
                    action_answers = answer_data[answer_data['action'] == node['action']]
                    for index,answer in action_answers.iterrows():
                        label_answer_percent = format(answer['answer_percent'], '.0%')
                        if has_comparison_data == 1:
                            answer_percent_change_formatted = format(answer['answer_percent_change'], '+.0%')
                            #set label colors
                            if answer['answer_percent_change'] > 0:
                                answer_change_color = '#006400'
                            elif answer['answer_percent_change'] < 0:
                                answer_change_color =  '#8b0000'
                            else:
                                answer_change_color = '#000'
                            answer_percent_change_code = '<TD><B><FONT COLOR="'+answer_change_color+'">(' + answer_percent_change_formatted +')</FONT></B></TD>'
                        else:
                            answer_percent_change_code = ''
                        label += '<TR><TD ALIGN="LEFT">'+answer['answer']+'</TD><TD>'+label_answer_percent+'</TD>' + answer_percent_change_code + '</TR>'
            #/////////////////////////////////////////////////////////////////////////////////////////////////////////////
            
            label += '</TABLE>>'

            #calculate conditional format colors //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
            conditional_format_metric = conditional_format_metric.replace('-','_')
            max_metric = node_data.query('action not in @excluded_actions')[conditional_format_metric].max()
            min_metric = node_data.query('action not in @excluded_actions')[conditional_format_metric].min()
            
            if max_metric == min_metric or np.isnan(node[conditional_format_metric]):
                color_distance = 0
            else:
                color_distance = (node[conditional_format_metric]- min_metric)/(max_metric- min_metric)
            
            if color_distance < 0.5:
                color_red_part   = conditional_format_gradient[0][0] + (conditional_format_gradient[1][0]-conditional_format_gradient[0][0])*color_distance/0.5
                color_green_part = conditional_format_gradient[0][1] + (conditional_format_gradient[1][1]-conditional_format_gradient[0][1])*color_distance/0.5
                color_blue_part  = conditional_format_gradient[0][2] + (conditional_format_gradient[1][2]-conditional_format_gradient[0][2])*color_distance/0.5
            else:
                color_red_part   = conditional_format_gradient[1][0] + (conditional_format_gradient[2][0]-conditional_format_gradient[1][0])*(color_distance-0.5)/0.5
                color_green_part = conditional_format_gradient[1][1] + (conditional_format_gradient[2][1]-conditional_format_gradient[1][1])*(color_distance-0.5)/0.5
                color_blue_part  = conditional_format_gradient[1][2] + (conditional_format_gradient[2][2]-conditional_format_gradient[1][2])*(color_distance-0.5)/0.5                 
            color = '#%02x%02x%02x' % ( int(color_red_part), int(color_green_part), int(color_blue_part) )
            #/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        
        
        if (node['action'][:4] == 'Drop' and show_drop) or node['action'][:4] != 'Drop':    
            dot.node(node['action'], shape = shape, label = label, style = 'filled, rounded', fillcolor=color,\
            href='', penwidth ='0.2', tooltip = node['action'])

    #draw edges
    for index, edge in edge_data.iterrows():
        edge_style = 'solid'
        if (edge['action_next'][:4] == 'Drop' and show_drop) or edge['action_next'][:4] != 'Drop':  
            if edge['edge_count'] >= min_edge_count:
                #calculate edge width
                edge_width = (edge['edge_count']-min_edge_count)/(edge_data['edge_count'].max()-min_edge_count)*max_edge_width
                edge_width = max(0.5, edge_width) # ensure line is not super thin and is visible
                edge_width = str(edge_width)
                
                #set edge color
                if edge['action_next'][:4] == 'Drop':
                    edge_color = '#%02x%02x%02x' % (255,200,200)
                else:
                    edge_color = '#%02x%02x%02x' % (200,200,200)

                #only show edge labels for funnel not drops
                if edge['action_next'][:4] == 'Drop':
                    label = ''
                    edge_dir = 'forward'
                    head_port = 'center'
                    tailport = 'center'
                    weight = '1'
                else:
                    label = str(edge['edge_count'])
                    edge_dir = 'forward'
                    tailport = 'center'
                    head_port='center'
                    weight = str(edge['edge_count'])

                dot.edge(str(edge['action']), str(edge['action_next']), label=label , penwidth = edge_width, 
                color = edge_color, style = edge_style, dir = edge_dir, tailport = tailport, 
                headport = head_port, weight = weight)

    #render graph 
    for ext in export_formats:       
        dot.render(title, view=False, format=ext)

def render(df, title='export', first_actions_filter = [], goals = [], max_path_num = 0, show_drop = True , show_answer=False, comparison_df = None, 
gradient = [[255,205,205],[255,255,255],[205,255,205]], gradient_metric = 'conversion-rate', metrics = ['conversion-rate','users','percent-of-total','duration-median']):
    max_edge_width = 20
    min_edge_count = 0

    if comparison_df is None:
        has_comparison_df = False
    else:
        has_comparison_df = True

    data, first_actions, all_actions = transform(df)
    if has_comparison_df == True:
        data_compare, __v1, all_actions_compare = transform(comparison_df)

    data, route_num =  apply_filter(data, first_actions_filter, goals)
    if has_comparison_df == True:
        data_compare, route_num_compare = apply_filter(data_compare, first_actions_filter, goals)
        oute_num = max(route_num, route_num_compare)

    if max_path_num > 0:
        route_num = min(route_num,max_path_num)
    
    data_node, data_edge, data_answer = aggregate(data, route_num)
    if has_comparison_df == True:
        data_compare_node, data_compare_edge, data_compare_answer = aggregate(data_compare, route_num)

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
        data_answer

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

    draw(data_node, data_edge, data_answer, goals, min_edge_count, max_edge_width, title, show_drop, show_answer, ['pdf'], gradient , gradient_metric, metrics)

def interactive():
    import subprocess
    import os
    package_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(package_dir, "interactive.py")
    result = subprocess.run(["streamlit", "run", file_path], capture_output=True, text=True)
    print(result.stdout)

