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
        df = df[df['first_action'].isin(first_actions_filter) ] # & (data['country']=='nl')

    #decide how to show end point
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

    #calculte node level aggregated data
    node_agg_data = df.groupby('action').agg(
        duration_median = ('duration', 'median'),
        duration_mean = ('duration', 'mean'),
        users = ('user_id', 'count'),
        conversion_rate = ('user_id', lambda x: 1.0 - sum(df.loc[x.index, 'action_next'].str[:4] == 'Drop') / len(x)),
    ).reset_index()   

    #calcualte total users
    total_users = int(df[df['action'] == 'Start']['user_id'].count())
    node_agg_data['percent_of_total'] =  node_agg_data.apply( lambda row: row['users']/total_users , axis=1)

    #calculate number for edges
    edge_agg_data = df.groupby(['action', 'action_next']).agg(edge_count=('user_id', 'count')).reset_index()

    #merge action and edge data
    agg_data = pd.merge(edge_agg_data, node_agg_data, on='action', how='left')

    #add drop to end data
    # drop_to_end = pd.DataFrame({
    #     'action': ['Drop'],
    #     'action_next': ['End'],
    #     'edge_count': [-1],
    #     'users': [agg_data[agg_data['action_next'] == 'Drop']['edge_count'].sum()],
    #     'conversion_rate': [0],
    #     'percent_of_total': [agg_data[agg_data['action_next'] == 'Drop']['edge_count'].sum()/total_users]
    # })
    # agg_data = pd.concat([agg_data, drop_to_end])

    return agg_data

def draw(agg_data, goals, min_edge_count, max_edge_width, title, show_drop, export_formats):
    #set parameters
    excluded_actions = ['Start', 'Drop'] + goals
    bgcolor = '#%02x%02x%02x' % (255, 255, 255)
    conditional_format_gradiant_color = [[255,200,200],[255,255,255],[200,255,200]] #red-->white-->green gradient

    #initialize graphvize engine
    dot = graphviz.Digraph(comment='')
    dot.attr(label='', labelloc='top', fontsize='20', fontcolor='black', bgcolor=bgcolor)

    nodes_start_data = agg_data[['action','users','conversion_rate','duration_median', 'percent_of_total']].drop_duplicates()
    nodes_end_data = agg_data[~agg_data['action_next'].isin(agg_data['action'])]
    nodes_end_data = nodes_end_data[['action_next', 'conversion_rate', 'users']]
    nodes_end_data.rename(columns={'action_next':'action'}, inplace=True)
    nodes_end_data.drop_duplicates(inplace=True)
    nodes_data = pd.concat([nodes_start_data,nodes_end_data])

    for index, row in nodes_data.iterrows():
        if row['action'] == 'Start' or row['action'] == 'End':
            color = '#fff'
            shape = 'circle'
            label = row['action']

        elif row['action'][:4] == 'Drop':
            color = '#ffc8c8'
            shape = 'cds'
            label = format(1-row['conversion_rate'], '.0%')+' ('+str(round((1-row['conversion_rate'])*row['users']))+')'

        else:
            shape = 'box'
            color = "#fff"
            label = """<
            <TABLE BORDER="0" CELLBORDER="0" CELLPADDING="1" CELLSPACing="0" BGCOLOR="transparent" STYLE="rounded">
                <TR><TD COLSPAN="2" ALIGN="CENTER"><B>"""+row['action']+"""</B></TD></TR>
                <TR><TD COLSPAN="2" ALIGN="CENTER" BGCOLOR="#aaa"></TD></TR>
                <TR><TD ALIGN="LEFT">Users:</TD><TD>"""+str(row['users'])+"""</TD></TR>
                <TR><TD ALIGN="LEFT">% of total users:</TD><TD>"""+format((row['percent_of_total']),'.0%')+"""</TD></TR>
                <TR><TD ALIGN="LEFT">Conversion:</TD><TD>"""+format(row['conversion_rate'], '.0%')+"""</TD></TR>
                <TR><TD ALIGN="LEFT">Duration (sec):</TD><TD>"""+str(int('0' if pd.isna(row['duration_median']) else row['duration_median'] ))+"""</TD></TR>
            </TABLE>
            >"""

            #calculate conditional format colors
            max_conversion_rate = agg_data.query('action not in @excluded_actions and edge_count >= @min_edge_count')['conversion_rate'].max()
            min_conversion_rate = agg_data.query('action not in @excluded_actions and edge_count >= @min_edge_count')['conversion_rate'].min()
            color_distance = (row['conversion_rate']- min_conversion_rate)/(max_conversion_rate- min_conversion_rate)
            if color_distance < 0.5:
                color_red_part   = conditional_format_gradiant_color[0][0] + (conditional_format_gradiant_color[1][0]-conditional_format_gradiant_color[0][0])*color_distance/0.5
                color_green_part = conditional_format_gradiant_color[0][1] + (conditional_format_gradiant_color[1][1]-conditional_format_gradiant_color[0][1])*color_distance/0.5
                color_blue_part  = conditional_format_gradiant_color[0][2] + (conditional_format_gradiant_color[1][2]-conditional_format_gradiant_color[0][2])*color_distance/0.5
            else:
                color_red_part   = conditional_format_gradiant_color[1][0] + (conditional_format_gradiant_color[2][0]-conditional_format_gradiant_color[1][0])*(color_distance-0.5)/0.5
                color_green_part = conditional_format_gradiant_color[1][1] + (conditional_format_gradiant_color[2][1]-conditional_format_gradiant_color[1][1])*(color_distance-0.5)/0.5
                color_blue_part  = conditional_format_gradiant_color[1][2] + (conditional_format_gradiant_color[2][2]-conditional_format_gradiant_color[1][2])*(color_distance-0.5)/0.5                 
            color = '#%02x%02x%02x' % ( int(color_red_part), int(color_green_part), int(color_blue_part) )
        
        if (row['action'][:4] == 'Drop' and show_drop) or row['action'][:4] != 'Drop':    
            dot.node(row['action'], shape = shape, label = label, style = 'filled, rounded', fillcolor=color,\
            href='', penwidth ='0.2', tooltip = row['action'])

    #draw edges
    for index, row in agg_data.iterrows():
        edge_style = 'solid'
        if (row['action_next'][:4] == 'Drop' and show_drop) or row['action_next'][:4] != 'Drop':  
            if row['edge_count'] > min_edge_count: ## and not row['action_next']=='End':
                edge_width = str(max(0.5,(row['edge_count']-min_edge_count)/(agg_data['edge_count'].max()-min_edge_count)*max_edge_width))
                if row['action_next'][:4] == 'Drop':
                    edge_color = '#%02x%02x%02x' % (255,200,200)
                elif row['action_next'] == 'Posted':
                    edge_color = '#%02x%02x%02x' % (200,255,200)
                else:
                    edge_color = '#%02x%02x%02x' % (200,200,200)

                #only show edge labels for funnel not drops
                if row['action_next'][:4] == 'Drop':
                    label = ''
                    edge_dir = 'forward'
                    head_port = 'center'
                    tailport = 'center'
                    weight = '1'
                else:
                    label = str(row['edge_count'])
                    edge_dir = 'forward'
                    tailport = 'center'
                    head_port='center'
                    weight = '7'

                dot.edge(str(row['action']), str(row['action_next']), label=label , penwidth = edge_width, color = edge_color, style = edge_style, dir = edge_dir, tailport = tailport, headport = head_port, weight = weight)

    #render graph 
    for ext in export_formats:       
        dot.render(title, view=False, format=ext)

def render(df, title='export', first_activities_filter = [], goals = [], max_path_num = 0, show_drop = True):
    max_edge_width = 20
    min_edge_count = 0

    data, first_activities, all_activities = transform(df)
    data, route_num =  apply_filter(data, first_activities_filter, goals)
    if max_path_num > 0:
        route_num = min(route_num,max_path_num)
    data = aggregate(data, route_num)
    draw(data, goals, min_edge_count, max_edge_width, title, show_drop, ['pdf'])

def interactive():
    import subprocess
    import os
    package_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(package_dir, "interactive.py")
    result = subprocess.run(["streamlit", "run", file_path], capture_output=True, text=True)
    print(result.stdout)

