import pandas as pd
import polars as pl
import graphviz
import numpy as np
from pandas.api.types import is_datetime64_any_dtype as is_datetime
df_backend = 'pandas'

def format_change_percent(value):
    if np.isnan(value):
        return '-'
    else:
        if value == 0:
            text_format = '.0%'
        else:
            text_format = '+.0%'
        return format(value, text_format)    

def format_metric(value, text_format):
    if np.isnan(value):
        return '-'
    else:
        return format(value, text_format)    

def hex_to_rgb(hex_values): 
    # this function converts gradienthex colors to rgb list that can be used to interpolate node's color
    rgb_list = []
    for hex_value in hex_values:
        striped = hex_value.lstrip('#')
        if len(striped) == 3:
            striped += striped
        rgb = tuple(int(striped[i:i+2], 16) for i in (0, 2, 4))
        rgb_list.append(rgb)
    return rgb_list

def export_to_csv(node_data, edge_data, answer_data):
    global df_backend
    node_data_export, edge_date_export = friendly_drop_names(node_data, edge_data)
    if df_backend == 'polars':
        node_data_export.write_csv('node_data.csv')
        edge_date_export.write_csv('edge_data.csv')
        answer_data.write_csv('answer_data.csv')
    else:
        node_data_export.to_csv('node_data.csv', index=False)
        edge_date_export.to_csv('edge_data.csv', index=False)
        answer_data.to_csv('answer_data.csv', index=False)

def friendly_drop_names_pd(node_data, edge_data):
    node_data_export = node_data.copy()
    edge_data_export = edge_data.copy()
    
    node_data_export['action'] = node_data_export['action'].str.replace('Funnelius-Drop', 'Drop:', regex = False)
    edge_data_export['action_next'] = edge_data_export['action_next'].str.replace('Funnelius-Drop', 'Drop:', regex = False)
    return node_data_export, edge_data_export

def friendly_drop_names_pl(node_data, edge_data):
    node_data_export = node_data.clone()
    edge_data_export = edge_data.clone()

    node_data_export = node_data_export.with_columns(
        pl.col('action').str.replace('Funnelius-Drop', 'Drop:').alias('action')
    )
    edge_data_export = edge_data_export.with_columns(
        pl.col('action_next').str.replace('Funnelius-Drop', 'Drop:').alias('action_next')
    )
    return node_data_export, edge_data_export

def friendly_drop_names(node_data, edge_data):
    #check if it is a polars dataframe
    global df_backend
    if df_backend == 'polars':
        return friendly_drop_names_pl(node_data, edge_data)
    else:
        return friendly_drop_names_pd(node_data, edge_data)

def transform_pd(df):
    #if answer column ommited, create it with empty values
    if 'answer' not in df.columns:
        df[required_column] = np.nan

    #convert action start to timeformat in python
    if not is_datetime(df['action_start']):
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

def transform_pl(df):
    #if answer column ommited, create it with empty values
    if 'answer' not in df.columns:
        df = df.with_columns(pl.lit(None).alias(required_column))
        
    #convert action start to timeformat in python
    if df["action_start"].dtype != pl.Datetime:
        df = df.with_columns(
            pl.col("action_start").str.to_datetime().alias("action_start")
        )    
    #create a column to show order of happened actions
    df = df.with_columns(
        pl.col('action_start').rank(method="ordinal").over(pl.col('user_id')).alias('action_order')
    )
    #create a dataframe for the very first action of every user
    df_first_action = df.filter(
        pl.col('action_order') == 1
    ).select(
        pl.col('user_id'), pl.col('action').alias('first_action')
    )
    #join it to data frame to have first action of every user. this will be used to filter based on first action
    data = df.join(
        df_first_action, on='user_id', how='left')
    #create_next_action dataframe
    data_next = data.clone()
    data_next = data_next.with_columns(
        (pl.col('action_order')-1).alias('action_order')
    )
    #join with original data to have next action for every action
    data = data.join(data_next['user_id', 'action_order','action', 'action_start'], on=['user_id', 'action_order'], how='left', suffix='_next')
    #caculate time spent by user_id in the action
    data = data.with_columns(
        pl.when(pl.col('action_next').is_null()).
        then(np.nan).
        otherwise((pl.col('action_start_next')-pl.col('action_start')).dt.total_seconds()).
        alias('duration')
    )
    #Drop action_start_next column as it is no longer needed
    data = data.drop('action_start_next')
    #add start Node
    start_rows = data.group_by(
        pl.col('user_id')
    ).agg(
        pl.col('action_start').min().alias('action_start'),
        pl.col('first_action').first().alias('first_action')
    )
    start_rows = start_rows.with_columns(
        pl.lit('Start').alias('action'),
        pl.lit(0).alias('action_order'),
        pl.lit(0).alias('duration'),
        pl.lit('-').alias('answer'),
        pl.col('first_action').alias('action_next')
    )
    data = pl.concat([data, start_rows['user_id', 'action', 'action_start', 'answer', 'action_order', 'first_action', 'action_next','duration']], how='vertical_relaxed')
    #calculate filter variables 
    first_actions = data.filter(pl.col('action_order') == 1).select('action').unique().to_series().to_list()
    all_actions = data.select('action').unique().to_series().to_list()

    return data, first_actions, all_actions

def transform(df):
    #check if it is a polars dataframe
    global df_backend
    if isinstance(df, pl.DataFrame):
        df_backend = 'polars'
        return transform_pl(df)
    else:
        df_backend = 'pandas'
        return transform_pd(df)

def apply_filter_pd(df, first_actions_filter, goals):
    #filter based on first actions
    if first_actions_filter != []:
        df = df[df['first_action'].isin(first_actions_filter) ]

    #decide how to show end point for user
    df['action_next'] = df.apply( lambda row: 'End' if pd.isna(row['action_next']) and row['action'] in goals else 
                                    ('Funnelius-Drop'+row['action'] if pd.isna(row['action_next']) else row['action_next']), axis=1)

    #Calculate priority of route
    df_user_route = df.sort_values(['user_id','action_order']).groupby('user_id')['action'].apply(lambda x: ' '.join(x)).reset_index(name='actions')
    df_route_priority = df_user_route.groupby('actions').agg(user_count = ('user_id','count')).reset_index().sort_values(by=['user_count'], ascending = False)
    df_route_priority['route_order'] = df_route_priority['user_count'].sort_values(ascending = False).rank(method='first', ascending=False).astype(int)
    df_user_route = pd.merge(df_user_route, df_route_priority[['actions', 'route_order']], on = 'actions', how = 'left')[['user_id','route_order']]
    df = pd.merge(df,df_user_route, on = 'user_id', how = 'left')
    route_num = df_user_route['route_order'].max()
    
    return df, route_num

def apply_filter_pl(df, first_actions_filter, goals):
    #filter based on first actions
    if first_actions_filter != []:
        df = df.filter(
            pl.col('first_action').is_in(first_actions_filter)
        )

    #decide how to show end point for user
    df = df.with_columns([
        pl.when(pl.col('action_next').is_null() & pl.col('action').is_in(goals))
        .then(pl.lit('End'))
        .when(pl.col('action_next').is_null())
        .then(pl.lit('Funnelius-Drop') + pl.col('action'))
        .otherwise(pl.col('action_next'))
        .alias('action_next')
    ])

    #Calculate priority of route
    df_user_route = (
        df.sort(['user_id', 'action_order'])
        .group_by('user_id')
        .agg(pl.col('action').alias('actions'))
        .with_columns([
            pl.col('actions').list.join(' ').alias('actions')
        ])
    )

    # Count users per unique route
    df_route_priority = (
        df_user_route
        .group_by('actions')
        .agg(pl.count('user_id').alias('user_count'))
        .sort('user_count', descending=True)
        .with_columns([
            pl.col('user_count').rank("ordinal", descending=True).alias('route_order')
        ])
    )

    # Join route priority back to users
    df_user_route = df_user_route.join(df_route_priority.select(['actions', 'route_order']), on='actions', how='left').select(['user_id', 'route_order'])

    # Merge route order back to main df
    df = df.join(df_user_route, on='user_id', how='left')

    # Get max route number
    route_num = df_user_route['route_order'].max()

    return df, route_num

def apply_filter(df, first_actions_filter, goals):
    #check if it is a polars dataframe
    global df_backend
    if df_backend == 'polars':
        return apply_filter_pl(df, first_actions_filter, goals)
    else:
        return apply_filter_pd(df, first_actions_filter, goals)


def aggregate_pd(df, route_num, max_visible_answers):
    # filter maximum routes
    df = df[df['route_order'] <= route_num]

    #calculte nodes aggregated data///////////////////////////////////////////////////////////////////////////////
    node_agg_data = df.groupby('action').agg(
        duration_median = ('duration', 'median'),
        duration_mean = ('duration', 'mean'),
        users = ('user_id', 'count'),
        conversion_rate = ('user_id', lambda x: 1.0 - sum(df.loc[x.index, 'action_next'].str[:14] == 'Funnelius-Drop') / len(x)),
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
    answer_agg_data.loc[answer_agg_data['answer_order'] > max_visible_answers, 'answer'] = 'Other items'
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

    return node_agg_data, edge_agg_data, answer_agg_data

def aggregate_pl(df, route_num, max_visible_answers):
    # filter maximum routes
    df = df.filter(pl.col('route_order') <= route_num)
    #calculte nodes aggregated data///////////////////////////////////////////////////////////////////////////////
    node_agg_data = (
        df.group_by('action')
        .agg(
            pl.median('duration').alias('duration_median'),
            pl.mean('duration').alias('duration_mean'),
            pl.count('user_id').alias('users'),
            (
                1.0 - (
                    pl.col('action_next').str.starts_with('Funnelius-Drop').sum() 
                    / pl.count('user_id')
                )
            ).alias('conversion_rate')
        )
    )
    #some nodes only appear in action_next column but still we should draw them
    missing_end_points = df.filter(~pl.col('action_next').is_in(pl.col('action').implode()))

    missing_end_points = missing_end_points.group_by('action_next').agg(
        pl.lit(np.nan).alias('duration_median'),
        pl.lit(np.nan).alias('duration_mean'),
        pl.count('user_id').alias('users'),
        pl.lit(np.nan).alias('conversion_rate'),
    )
    missing_end_points = missing_end_points.rename({'action_next': 'action'})

    node_agg_data = pl.concat([node_agg_data, missing_end_points])

    #calcualte total users
    total_users = df.filter(pl.col("action") == "Start").select(pl.count("user_id")).item()
    node_agg_data = node_agg_data.with_columns(
        (pl.col("users") / total_users).alias("percent_of_total")
    )
    #//////////////////////////////////////////////////////////////////////////////////////////////////////////
    #Calculate aggregated answer data ////////////////////////////////////////////////////////////////////////
    # Step 1: Group by ['action', 'answer'] and count
    answer_agg_data = (
        df.group_by(['action', 'answer'])
        .agg(pl.count('answer').alias('answer_count'))
    )

    # Step 2: Sort by 'action' and descending 'answer_count'
    answer_agg_data = answer_agg_data.sort(['action', 'answer_count'], descending=[False, True])

    # Step 3: Assign answer_order within each 'action'
    answer_agg_data = answer_agg_data.with_columns(
        pl.cum_count('answer').over('action').alias('answer_order')
    )

    # Step 4: Replace answers with 'Other items' if answer_order > max_visible_answers

    answer_agg_data = answer_agg_data.with_columns(
        pl.when(pl.col('answer_order') > max_visible_answers)
        .then(pl.lit('Other items'))
        .otherwise('answer')
        .alias('answer')
    )

    # Step 5: Regroup to sum answer_count after collapsing into 'Other items'
    answer_agg_data = (
        answer_agg_data.group_by(['action', 'answer'])
                    .agg(pl.sum('answer_count').alias('answer_count'))
    )

    # Step 6: Sort again
    answer_agg_data = answer_agg_data.sort(['action', 'answer_count'], descending=[False, True])

    # Step 7: Compute total per action
    answer_total = (
        answer_agg_data.group_by('action')
                    .agg(pl.sum('answer_count').alias('total'))
    )

    # Step 8: Join totals back to main DataFrame
    answer_agg_data = answer_agg_data.join(answer_total, on='action', how='left')

    # Step 9: Calculate percentage
    answer_agg_data = answer_agg_data.with_columns(
        (pl.col('answer_count') / pl.col('total')).alias('answer_percent')
    )

    # Step 10: Drop 'total' column
    answer_agg_data = answer_agg_data.drop('total')
    #/////////////////////////////////////////////////////////////////////////////////////////////////////////

    #calculate edges aggregated data 
    edge_agg_data = (
        df.group_by(['action', 'action_next'])
        .agg(pl.count('user_id').alias('edge_count'))
    )

    return node_agg_data, edge_agg_data, answer_agg_data

def aggregate(df, route_num, max_visible_answers):
    #check if it is a polars dataframe
    global df_backend
    if df_backend == 'polars':
        return aggregate_pl(df, route_num, max_visible_answers)
    else:
        return aggregate_pd(df, route_num, max_visible_answers)

def draw_nodes_pd(node_data, answer_data, has_comparison_data, dot, metrics, goals, show_answer, conditional_format_metric, conditional_format_gradient, max_metric, min_metric, show_drop):
    for index, node in node_data.iterrows():
        draw_single_node(node, node_data, answer_data, has_comparison_data, dot, metrics, goals, show_answer, conditional_format_metric, conditional_format_gradient, max_metric, min_metric, show_drop)

def draw_nodes_pl(node_data, answer_data, has_comparison_data, dot, metrics, goals, show_answer, conditional_format_metric, conditional_format_gradient, max_metric, min_metric, show_drop):
    for node in node_data.iter_rows(named=True):
        draw_single_node(node, node_data, answer_data, has_comparison_data, dot, metrics, goals, show_answer, conditional_format_metric, conditional_format_gradient, max_metric, min_metric, show_drop)

def generate_answer_label(answer, has_comparison_data):
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
        
        return '<TR><TD ALIGN="LEFT">'+answer['answer']+'</TD><TD>'+label_answer_percent+'</TD>' + answer_percent_change_code + '</TR>'

def generate_answers_text(action_answers, has_comparison_data):
    label = ''
    global df_backend
    if df_backend == 'pandas':
        for index,answer in action_answers.iterrows():
            label += generate_answer_label(answer, has_comparison_data) 
    else:
        for answer in action_answers.iter_rows(named=True):
            label += generate_answer_label(answer, has_comparison_data) 
    return label

def conditional_metric_min_max(node_data, conditional_format_metric, excluded_actions):
    global df_backend
    if df_backend == 'pandas':
        max_metric = node_data.query('action not in @excluded_actions')[conditional_format_metric].max()
        min_metric = node_data.query('action not in @excluded_actions')[conditional_format_metric].min()
    else:
        max_metric = node_data.filter(~pl.col("action").is_in(excluded_actions))[conditional_format_metric].max()
        min_metric = node_data.filter(~pl.col("action").is_in(excluded_actions))[conditional_format_metric].min()
    return max_metric, min_metric

def draw_nodes(node_data, answer_data, has_comparison_data, dot, metrics, goals, show_answer, conditional_format_metric, conditional_format_gradient, excluded_actions, show_drop):
    conditional_format_metric = conditional_format_metric.replace('-','_')
    max_metric,min_metric = conditional_metric_min_max(node_data, conditional_format_metric, excluded_actions)
    
    #check if it is a polars dataframe
    global df_backend
    if df_backend == 'polars':
        draw_nodes_pl(node_data, answer_data, has_comparison_data, dot, metrics, goals, show_answer, conditional_format_metric, conditional_format_gradient, max_metric, min_metric, show_drop)
    else:
        draw_nodes_pd(node_data, answer_data, has_comparison_data, dot, metrics, goals, show_answer, conditional_format_metric, conditional_format_gradient, max_metric, min_metric, show_drop)   

def draw_single_node(node, node_data, answer_data, has_comparison_data, dot, metrics, goals, show_answer, conditional_format_metric, conditional_format_gradient, max_metric, min_metric, show_drop):
    global df_backend

    #prepare label variables
    label_users = str(node['users'])
    label_percent_of_total = format_metric(node['percent_of_total'], '.0%')
    label_conversion_rate = format_metric(node['conversion_rate'], '.0%')
    label_duration_median = format_metric(node['duration_median'], '.1f')
    label_duration_mean = format_metric(node['duration_mean'], '.1f')

    if has_comparison_data == 1:
        change_vars = {}
        change_metrics = ['users','conversion_rate','duration_median','duration_mean','percent_of_total']
        for metric in change_metrics:
            change_vars['label_'+metric+'_change'] = format_change_percent(node[metric+'_change'])
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

    elif node['action'][:14] == 'Funnelius-Drop':
        color = '#ffc8c8'
        shape = 'cds'
        if df_backend == 'pandas':
            action_previous_users = node_data[node_data['action'] == node['action'].replace('Funnelius-Drop','')]['users'].iloc[0]
        else:
            action_previous_users = node_data.filter(pl.col("action") == node["action"].replace("Funnelius-Drop", "")).select("users").to_series()[0]
        label = format(node['users']/action_previous_users, '.0%')+' ('+label_users+')'

    else:
        shape = 'box'
        color = "#fff"
        if has_comparison_data == 1:
            colspan = '3'
            users_change_code = '<TD><B><FONT COLOR="'+change_vars['color_users_change']+'">(' + change_vars['label_users_change'] +')</FONT></B></TD>'
            conversion_rate_change_code = '<TD><B><FONT COLOR="'+change_vars['color_conversion_rate_change']+'">('+ change_vars['label_conversion_rate_change']+')</FONT></B></TD>'
            percent_of_total_change_code = '<TD><B><FONT COLOR="'+change_vars['color_percent_of_total_change']+'">('+ change_vars['label_percent_of_total_change']+')</FONT></B></TD>'
            duration_median_change_code = '<TD><B><FONT COLOR="'+change_vars['color_duration_median_change']+'">('+ change_vars['label_duration_median_change']+')</FONT></B></TD>'
            duration_mean_change_code = '<TD><B><FONT COLOR="'+change_vars['color_duration_mean_change']+'">('+ change_vars['label_duration_mean_change']+')</FONT></B></TD>'
        else:
            colspan = '2'
            users_change_code = ''
            conversion_rate_change_code = ''
            percent_of_total_change_code = ''
            duration_median_change_code = ''
            duration_mean_change_code = ''

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
                label += '<TR><TD ALIGN="LEFT">Duration Median (sec):</TD><TD>' + label_duration_median + '</TD>' + duration_median_change_code +'</TR>'
            if 'duration-mean' in metrics:
                label += '<TR><TD ALIGN="LEFT">Duration Mean (sec):</TD><TD>' + label_duration_mean + '</TD>' + duration_mean_change_code +'</TR>'
        #add answers//////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        if show_answer == True:
            if not node['action'] in goals:
                if df_backend == 'pandas':
                    action_answers = answer_data[answer_data['action'] == node['action']]
                else:
                    action_answers = answer_data.filter(pl.col('action') == node['action'])
                    
                if len(action_answers) > 0:
                    label += '<TR><TD COLSPAN="' + colspan + '" ALIGN="CENTER"><B>Answers</B></TD></TR>'
                    label += '<TR><TD COLSPAN="' + colspan + '" ALIGN="CENTER" BGCOLOR="#aaa"></TD></TR>'
                    
                    label += generate_answers_text(action_answers, has_comparison_data)
        #/////////////////////////////////////////////////////////////////////////////////////////////////////////////
        
        label += '</TABLE>>'

        #calculate conditional format colors //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        
        if max_metric == min_metric or np.isnan(node[conditional_format_metric]):
            color_distance = 0
        else:
            color_distance = (node[conditional_format_metric]- min_metric)/(max_metric- min_metric)

        rgb_list = hex_to_rgb(conditional_format_gradient)
        
        if color_distance < 0.5:
            color_red_part   = rgb_list[0][0] + (rgb_list[1][0]-rgb_list[0][0])*color_distance/0.5
            color_green_part = rgb_list[0][1] + (rgb_list[1][1]-rgb_list[0][1])*color_distance/0.5
            color_blue_part  = rgb_list[0][2] + (rgb_list[1][2]-rgb_list[0][2])*color_distance/0.5
        else:
            color_red_part   = rgb_list[1][0] + (rgb_list[2][0]-rgb_list[1][0])*(color_distance-0.5)/0.5
            color_green_part = rgb_list[1][1] + (rgb_list[2][1]-rgb_list[1][1])*(color_distance-0.5)/0.5
            color_blue_part  = rgb_list[1][2] + (rgb_list[2][2]-rgb_list[1][2])*(color_distance-0.5)/0.5                 
        color = '#%02x%02x%02x' % ( int(color_red_part), int(color_green_part), int(color_blue_part) )
        #/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    
    
    if (node['action'][:14] == 'Funnelius-Drop' and show_drop) or node['action'][:14] != 'Funnelius-Drop':    
        dot.node(node['action'], shape = shape, label = label, style = 'filled, rounded', fillcolor=color,\
        href='', penwidth ='0.2', tooltip = node['action'])

def draw_single_edge(edge, max_edge_count, min_edge_count, max_edge_width, dot, has_comparison_data, show_drop):
    edge_style = 'solid'
    if (edge['action_next'][:14] == 'Funnelius-Drop' and show_drop) or edge['action_next'][:14] != 'Funnelius-Drop':  
        if edge['edge_count'] >= min_edge_count:
            #calculate edge width
            edge_width = (edge['edge_count']-min_edge_count)/(max_edge_count-min_edge_count)*max_edge_width
            edge_width = max(0.5, edge_width) # ensure line is not super thin and is visible
            edge_width = str(edge_width)
            
            #set edge color
            if edge['action_next'][:14] == 'Funnelius-Drop':
                edge_color = '#%02x%02x%02x' % (255,200,200)
            else:
                edge_color = '#%02x%02x%02x' % (200,200,200)

            #only show edge labels for funnel not drops
            if edge['action_next'][:14] == 'Funnelius-Drop':
                label = ''
                edge_dir = 'forward'
                head_port = 'center'
                tailport = 'center'
                weight = '1'
            else:
                label_text = str(edge['edge_count'])
                if has_comparison_data == 1:
                    label_text += ' (' + format_change_percent(edge['edge_count_change']) + ')'
                label = label_text
                edge_dir = 'forward'
                tailport = 'center'
                head_port='center'
                weight = str(edge['edge_count'])

            dot.edge(str(edge['action']), str(edge['action_next']), label=label , penwidth = edge_width, 
            color = edge_color, style = edge_style, dir = edge_dir, tailport = tailport, 
            headport = head_port, weight = weight)

def draw_edges(edge_data, min_edge_count, max_edge_width, dot, has_comparison_data, show_drop):
    max_edge_count = edge_data['edge_count'].max()

    #check if it is a polars dataframe
    global df_backend
    if df_backend == 'polars':
        for edge in edge_data.iter_rows(named=True):
            draw_single_edge(edge, max_edge_count, min_edge_count, max_edge_width, dot, has_comparison_data, show_drop)
    else:
        for index, edge in edge_data.iterrows(): 
            draw_single_edge(edge, max_edge_count, min_edge_count, max_edge_width, dot, has_comparison_data, show_drop)

def draw(node_data, edge_data, answer_data, goals, min_edge_count, max_edge_width, title, show_drop, show_answer, max_visible_answers,
export_formats, conditional_format_gradient=['#ffc8c8','#fff','#c8ffc8'], 
conditional_format_metric = 'conversion-rate', metrics=['conversion-rate','users','percent-of-total','duration-median']):

    #export data to csv
    export_to_csv(node_data, edge_data, answer_data)

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
    draw_nodes(node_data, answer_data, has_comparison_data, dot, metrics, goals, show_answer, conditional_format_metric, conditional_format_gradient, excluded_actions, show_drop)

    #draw edges////////////////////////////////////////////
    draw_edges(edge_data, min_edge_count, max_edge_width, dot, has_comparison_data, show_drop)

    #render graph 
    for ext in export_formats:       
        dot.render(title, view=False, format=ext)

def merge_with_compare(data_node,data_compare_node,data_edge,data_compare_edge,data_answer,data_compare_answer):
    global df_backend
    if df_backend == 'pandas':
        data_node = pd.merge(data_node, data_compare_node[['action','conversion_rate','users','duration_median','duration_mean','percent_of_total']], on='action', how='left', suffixes =('','_compare')) #add nodes compare data
        data_edge = pd.merge(data_edge, data_compare_edge[['action','action_next','edge_count']], on=['action','action_next'], how='left', suffixes =('','_compare')) #add edge compare data
        data_answer  = pd.merge(data_answer, data_compare_answer[['action','answer','answer_percent']], on=['action','answer'], how='left', suffixes =('','_compare')) #add answer compare data
    else:
        data_node = pdata_node = data_node.join(data_compare_node.select(["action","conversion_rate","users","duration_median","duration_mean","percent_of_total"]),on="action",how="left",suffix="_compare") #add nodes compare data
        data_edge = data_edge.join(data_compare_edge.select(["action", "action_next", "edge_count"]), on=["action", "action_next"], how="left", suffix="_compare") #add edge compare data
        data_answer = data_answer.join(data_compare_answer.select(["action", "answer", "answer_percent"]), on=["action", "answer"], how="left", suffix="_compare") #add answer compare data         
    
    return data_node,data_edge,data_answer

def add_compare_only_nodes(data_node, data_compare_node):
    global df_backend
    if df_backend == 'pandas':
        nodes_only_in_comparison = data_compare_node[~data_compare_node['action'].isin(data_node['action'])]
        nodes_only_in_comparison = nodes_only_in_comparison[['action']]
        nodes_only_in_comparison[['users', 'percent_of_total']] = 0
        nodes_only_in_comparison[['conversion_rate', 'duration_median', 'duration_mean',
        'conversion_rate_change', 'duration_median_change', 'duration_mean_change']] = np.nan
        nodes_only_in_comparison[['users_change', 'percent_of_total_change']] = -1
        data_node = pd.concat([data_node, nodes_only_in_comparison])
    else:
        nodes_only_in_comparison = data_compare_node[~data_compare_node['action'].isin(data_node['action'])]
        nodes_only_in_comparison = nodes_only_in_comparison.select(pl.col('action'))
        nodes_only_in_comparison = nodes_only_in_comparison.with_columns(
            pl.lit(0).alias('users'), 
            pl.lit(0).alias('percent_of_total'),
            pl.lit(np.nan).alias('conversion_rate'), 
            pl.lit(np.nan).alias('duration_median'), 
            pl.lit(np.nan).alias('duration_mean'),
            pl.lit(np.nan).alias('conversion_rate_change'), 
            pl.lit(np.nan).alias('duration_median_change'), 
            pl.lit(np.nan).alias('duration_mean_change'),
            pl.lit(-1).alias('users_change'), 
            pl.lit(-1).alias('percent_of_total_change')
        )
        data_node = pl.concat([data_node, nodes_only_in_comparison])   

    return data_node     

def add_compare_only_edges(data_edge, data_compare_edge):
    global df_backend
    if df_backend == 'pandas':
        edges_only_in_comparison = data_compare_edge.merge(data_edge, on=['action','action_next'], how='left', indicator=True)
        edges_only_in_comparison = edges_only_in_comparison[edges_only_in_comparison['_merge'] == 'left_only'].drop(columns=['_merge'])
        edges_only_in_comparison = edges_only_in_comparison[['action', 'action_next']]
        edges_only_in_comparison['edge_count'] = 0
        edges_only_in_comparison['edge_count_change'] = -1
        data_edge = pd.concat([data_edge, edges_only_in_comparison])
    else:
        edges_only_in_comparison = data_compare_edge.join(data_edge, on=["action", "action_next"], how="left", indicator=True)
        edges_only_in_comparison = edges_only_in_comparison.filter(pl.col("_merge") == "left_only").drop("_merge")
        edges_only_in_comparison = edges_only_in_comparison.select(pl.col('action'), pl.col('action_next'))
        edges_only_in_comparison = edges_only_in_comparison.with_columns(pl.lit(0).alias('edge_count'), pl.lit(-1).alias('edge_count_change'))
        data_edge = pl.concat([data_edge, edges_only_in_comparison])

    return data_edge  

def render(df, title='export', first_actions_filter = [], goals = [], max_path_num = 0, show_drop = True , show_answer=False, max_visible_answers=5, comparison_df = None, 
gradient = ['#ffcdcd','#fff','#cdffcd'], gradient_metric = 'conversion-rate', metrics = ['conversion-rate','users','percent-of-total','duration-median']):
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
    
    data_node, data_edge, data_answer = aggregate(data, route_num, max_visible_answers)
    if has_comparison_df == True:
        data_compare_node, data_compare_edge, data_compare_answer = aggregate(data_compare, route_num, max_visible_answers)

        #merge data with compare_data
        data_node, data_edge, data_answer = merge_with_compare(data_node,data_compare_node,data_edge,data_compare_edge,data_answer,data_compare_answer) #add compare data
        
        #calculate increase/decrease percentages for nodes
        metrics = ['conversion_rate','duration_median','percent_of_total','users']
        for metric in metrics:
            data_node[metric+'_change'] = data_node[metric]/data_node[metric+'_compare'] - 1

        #calculate increase/decrease percentages for answers
        data_answer['answer_percent_change'] = data_answer['answer_percent']/data_answer['answer_percent_compare'] - 1

        #calculate increase/decrease percentages for edges
        data_edge['edge_count_change'] = data_edge['edge_count']/data_edge['edge_count_compare'] - 1
        
        #add nodes that were present in comparison data but not in original data
        data_node = add_compare_only_nodes(data_node, data_compare_node)

        #add edges that were present in comparison data but not in original data
        data_edge = add_compare_only_edges(data_edge, data_compare_edge)

    draw(data_node, data_edge, data_answer, goals, min_edge_count, max_edge_width, title, show_drop, show_answer, max_visible_answers, ['pdf'], gradient , gradient_metric, metrics)

def interactive():
    import subprocess
    import os
    package_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(package_dir, "interactive.py")
    result = subprocess.run(["streamlit", "run", file_path], capture_output=True, text=True)
    print(result.stdout)

