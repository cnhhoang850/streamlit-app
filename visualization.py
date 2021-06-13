
import streamlit as st
import json
import requests
import sys
import os
import pandas as pd
import numpy as np
import re
import matplotlib.pyplot as plt
import altair as alt 
from datetime import datetime as dt

def black_marks():
    return {
        'config': {
            'view': {
                'height': 600,
                'width': 400,
            },
            'mark': {
                'color': '#FBDCB8',
                'fill': '#FBDCB8'
            }
        }
    }

# register the custom theme under a chosen name
alt.themes.register('black_marks', black_marks)

# enable the newly registered theme
alt.themes.enable('black_marks')



st.set_page_config(layout="wide")

st.title('DataCracy ATOM Learner Homework Report')
with open('./env_variables.json','r') as j:
    json_data = json.load(j)

#SLACK_BEARER_TOKEN = os.environ.get('SLACK_BEARER_TOKEN') ## Get in setting of Streamlit Share
SLACK_BEARER_TOKEN = json_data['SLACK_BEARER_TOKEN']
DTC_GROUPS_URL = ('https://raw.githubusercontent.com/anhdanggit/atom-assignments/main/data/datacracy_groups.csv')
#st.write(json_data['SLACK_BEARER_TOKEN'])

@st.cache
def load_users_df():
    # Slack API User Data
    endpoint = "https://slack.com/api/users.list"
    headers = {"Authorization": "Bearer {}".format(json_data['SLACK_BEARER_TOKEN'])}
    response_json = requests.post(endpoint, headers=headers).json() 
    user_dat = response_json['members']

    # Convert to CSV
    user_dict = {'user_id':[],'name':[],'display_name':[],'real_name':[],'title':[],'is_bot':[]}
    for i in range(len(user_dat)):
      user_dict['user_id'].append(user_dat[i]['id'])
      user_dict['name'].append(user_dat[i]['name'])
      user_dict['display_name'].append(user_dat[i]['profile']['display_name'])
      user_dict['real_name'].append(user_dat[i]['profile']['real_name_normalized'])
      user_dict['title'].append(user_dat[i]['profile']['title'])
      user_dict['is_bot'].append(int(user_dat[i]['is_bot']))
    user_df = pd.DataFrame(user_dict) 
    # Read dtc_group hosted in github
    dtc_groups = pd.read_csv(DTC_GROUPS_URL)
    user_df = user_df.merge(dtc_groups, how='left', on='name')
    return user_df

@st.cache
def load_channel_df():
    endpoint2 = "https://slack.com/api/conversations.list"
    data = {'types': 'public_channel,private_channel'} # -> CHECK: API Docs https://api.slack.com/methods/conversations.list/test
    headers = {"Authorization": "Bearer {}".format(SLACK_BEARER_TOKEN)}
    response_json = requests.post(endpoint2, headers=headers, data=data).json() 
    channel_dat = response_json['channels']
    channel_dict = {'channel_id':[], 'channel_name':[], 'is_channel':[],'creator':[],'created_at':[],'topics':[],'purpose':[],'num_members':[]}
    for i in range(len(channel_dat)):
        channel_dict['channel_id'].append(channel_dat[i]['id'])
        channel_dict['channel_name'].append(channel_dat[i]['name'])
        channel_dict['is_channel'].append(channel_dat[i]['is_channel'])
        channel_dict['creator'].append(channel_dat[i]['creator'])
        channel_dict['created_at'].append(dt.fromtimestamp(float(channel_dat[i]['created'])))
        channel_dict['topics'].append(channel_dat[i]['topic']['value'])
        channel_dict['purpose'].append(channel_dat[i]['purpose']['value'])
        channel_dict['num_members'].append(channel_dat[i]['num_members'])
    channel_df = pd.DataFrame(channel_dict) 
    return channel_df

@st.cache(allow_output_mutation=True)
def load_msg_dict():
    endpoint3 = "https://slack.com/api/conversations.history"
    headers = {"Authorization": "Bearer {}".format(SLACK_BEARER_TOKEN)}
    msg_dict = {'channel_id':[],'msg_id':[], 'msg_ts':[], 'user_id':[], 'latest_reply':[],'reply_user_count':[],'reply_users':[],'github_link':[],'text':[]}
    for channel_id, channel_name in zip(channel_df['channel_id'], channel_df['channel_name']):
        print('Channel ID: {} - Channel Name: {}'.format(channel_id, channel_name))
        try:
            data = {"channel": channel_id} 
            response_json = requests.post(endpoint3, data=data, headers=headers).json()
            msg_ls = response_json['messages']
            for i in range(len(msg_ls)):
                if 'client_msg_id' in msg_ls[i].keys():
                    msg_dict['channel_id'].append(channel_id)
                    msg_dict['msg_id'].append(msg_ls[i]['client_msg_id'])
                    msg_dict['msg_ts'].append(dt.fromtimestamp(float(msg_ls[i]['ts'])))
                    msg_dict['latest_reply'].append(dt.fromtimestamp(float(msg_ls[i]['latest_reply'] if 'latest_reply' in msg_ls[i].keys() else 0))) ## -> No reply: 1970-01-01
                    msg_dict['user_id'].append(msg_ls[i]['user'])
                    msg_dict['reply_user_count'].append(msg_ls[i]['reply_users_count'] if 'reply_users_count' in msg_ls[i].keys() else 0)
                    msg_dict['reply_users'].append(msg_ls[i]['reply_users'] if 'reply_users' in msg_ls[i].keys() else 0) 
                    msg_dict['text'].append(msg_ls[i]['text'] if 'text' in msg_ls[i].keys() else 0) 
                    ## -> Censor message contains tokens
                    text = msg_ls[i]['text']
                    github_link = re.findall('(?:https?://)?(?:www[.])?github[.]com/[\w-]+/?', text)
                    msg_dict['github_link'].append(github_link[0] if len(github_link) > 0 else None)
        except:
            print('====> '+ str(response_json))
    msg_df = pd.DataFrame(msg_dict)
    return msg_df

def process_msg_data(msg_df, user_df, channel_df):
    ## Merge to have a nice name displayed
    msg_df = msg_df.merge(user_df[['user_id','real_name','DataCracy_role']].rename(columns={'real_name':'submit_name'}), \
        how='left',on='user_id')
    ## Merge for nice channel name
    msg_df = msg_df.merge(channel_df[['channel_id','channel_name','created_at']], how='left',on='channel_id')
    ## Format datetime cols
    msg_df['created_at'] = msg_df['created_at'].dt.strftime('%Y-%m-%d')
    msg_df['msg_date'] = msg_df['msg_ts'].dt.strftime('%Y-%m-%d')
    msg_df['msg_time'] = msg_df['msg_ts'].dt.strftime('%H:%M')
    msg_df['wordcount'] = msg_df.text.apply(lambda s: len(s.split()))
    
    msg_df = msg_df.dropna(subset=['channel_name'])
    return msg_df


# Table data
user_df = load_users_df()
channel_df = load_channel_df()
msg_df = load_msg_dict()

#st.write(process_msg_data(msg_df, user_df, channel_df))


# Input
st.sidebar.markdown('## Thông tin')
user_id = st.sidebar.text_input("Nhập Mã Số Người Dùng", 'U01xxxx')

user_df = user_df[user_df['DataCracy_role'].str.contains('Learner', na=False)]	
user_cols = ['user_id', 'name', 'DataCracy_role']
channel_df = channel_df[channel_df['channel_name'].str.contains('assignment', na=False)]

p_msg_df = process_msg_data(msg_df, user_df, channel_df)
p_msg_df = p_msg_df[p_msg_df.channel_name.str.contains('assignment', na=False)]
p_msg_df = p_msg_df[p_msg_df.DataCracy_role.str.contains('Learner', na=False)]
p_msg_df = p_msg_df.rename(columns={'channel_name':'assignment'})

# Submisison
submits_df= p_msg_df
latest_ts = submits_df.groupby(['assignment', 'user_id']).msg_ts.idxmax() ## -> Latest ts
submits_df = submits_df.loc[latest_ts]


deadlines = {
    "assignment1": '2022-04-24',
    "assignment2": '2021-05-16',
    "assignment3": '2021-05-23',
    "assignment4": '2021-05-30',
    "assignment5": '2021-06-06',
    "assignment6": '2021-06-13'
}
status = []

for i in range(len(submits_df)):
    learner = submits_df.iloc[i]['msg_date']
    deadline = deadlines[submits_df.iloc[i]['assignment'][5:]]
    if learner > deadline: 
        status.append('late')
    else: 
        status.append('not-late')
submits_df['status'] = status
dis_cols1 = ['user_id','submit_name','assignment','status','msg_date','reply_user_count']

# Users
st.markdown(
"""
#### Chú thích:
1. status: nộp bài quá hạn hoặc đúng hạn
2. msg_date: ngày nộp bài
3. submitted: số bài đã nộp 
4. reviewed: số bài đã được review
5 reviews_percentage: phần trăm số bài đã được 
"""
)
st.write("Tổng hợp bài được nộp:")    
st.write(submits_df[dis_cols1])

# Pie chart 
data = submits_df[submits_df.status == 'late']
data_late = data.groupby('assignment')['status'].count()
data_late = data_late.rename('late')

data = submits_df[submits_df.status == 'not-late']
data = data.groupby('assignment')['status'].count()
data = data.rename('not-late')
    
pie = pd.concat([data, data_late], axis= 1)
pie = pie.fillna(0)

not_submitted = []
for i in range(len(pie)):
    not_submitted.append(len(user_df) - pie.iloc[i].values[0] + pie.iloc[i].values[1])
    
pie['missing'] = not_submitted

st.subheader("Số lượng bài tập nộp cho mỗi assignment")	
st.bar_chart(data=pie, width=100, height=300)




def summarize_data(msg_df, user_df, channel_df):
    ## Extract 2 reply_users
    msg_df['reply_user1'] = msg_df['reply_users'].apply(lambda x: x[0] if x != 0 else '')
    msg_df['reply_user2'] = msg_df['reply_users'].apply(lambda x: x[1] if x != 0 and len(x) > 1 else '')
    ## Merge to have a nice name displayed
    msg_df = msg_df.merge(user_df[['user_id','name','DataCracy_role']].rename(columns={'name':'submit_name'}), \
        how='left',on='user_id')
    msg_df = msg_df.merge(user_df[['user_id','name']].rename(columns={'name':'reply1_name','user_id':'reply1_id'}), \
        how='left', left_on='reply_user1', right_on='reply1_id')
    msg_df = msg_df.merge(user_df[['user_id','name']].rename(columns={'name':'reply2_name','user_id':'reply2_id'}), \
        how='left', left_on='reply_user2', right_on='reply2_id')
    ## Merge for nice channel name
    msg_df = msg_df.merge(channel_df[['channel_id','channel_name','created_at']], how='left',on='channel_id')
    ## Format datetime cols
    # msg_df['created_at'] = msg_df['created_at'].dt.strftime('%Y-%m-%d')
    # msg_df['msg_date'] = msg_df['msg_ts'].dt.strftime('%Y-%m-%d')
    # msg_df['msg_time'] = msg_df['msg_ts'].dt.strftime('%H:%M')
    msg_df['created_at'] = pd.to_datetime(msg_df['created_at'])
    msg_df['msg_date'] = pd.to_datetime(msg_df['msg_ts'])
    msg_df['msg_ts'] = pd.to_datetime(msg_df['msg_ts'])
    msg_df['weekdat'] = msg_df['msg_ts'].dt.dayofweek
    msg_df['hour_msg'] = msg_df['msg_ts'].dt.hour
    msg_df['wordcount'] = msg_df.text.apply(lambda s: len(s.split()))
    return msg_df
    
summary_df = summarize_data(msg_df, user_df, channel_df) 
 
submissions = summary_df[((summary_df.github_link.notnull() & summary_df.channel_name != 'atom-assignment1'))\
                            & summary_df.DataCracy_role.str.contains('Learner')\
                            & summary_df.channel_name.str.contains('assignment')]

last_submissions = submissions.groupby(['channel_name','user_id']).msg_date.idxmax()


submissions['reviewed'] = submissions[['reply1_id','reply2_id','user_id','reply_user_count']].apply(lambda x: (0,1) 
                                        [(x['reply_user_count'] > 0) & ((x['reply1_id'] != x['user_id']) | (x['reply2_id'] != x['user_id'])\
                                        | ((x['reply1_id'] != x['user_id']) & (x['reply2_id'] != x['user_id'])))], axis=1)
                                        
learner = submissions.loc[last_submissions].groupby(['submit_name','DataCracy_role']).agg({ 'channel_name': 'count' ,
                                                                  'reviewed': 'sum',
                                                                  'wordcount': 'sum'})
learner = learner.reset_index()
learner = learner.rename(columns={'channel_name':'submitted'})
learner['reviewed_percentage'] = round(learner['reviewed'] * 100 / learner['submitted'],2)
learner = learner.drop(['DataCracy_role'], axis = 1)
learner = pd.merge(user_df[user_df.DataCracy_role.notnull() & user_df.DataCracy_role.str.contains('Learner')][['user_id','name','DataCracy_role']], learner, how= 'left', left_on='name', right_on='submit_name')
learner = learner.drop('submit_name', axis= 1)
learner = learner.fillna(0)
learner.rename(columns={"channel_name":"assignment"})

learner = learner.merge(submits_df[['user_id', 'submit_name']].drop_duplicates(subset=['user_id']), how='left', on='user_id')


st.write("Tổng hợp learners:")
st.write(learner[['user_id', 'submit_name', 'submitted', 'reviewed', 'reviewed_percentage', 'wordcount']])
st.subheader('Thống kê mô tả')
st.write(learner.describe())

options = st.multiselect('Chọn số liệu muốn thống kê', ['submitted', 'reviewed', 'wordcount', 'reviewed_percentage'])


mixed = learner.merge(submits_df[['user_id', 'assignment']], how='left', on='user_id')


for i in options:
    col1, col2 = st.beta_columns(2)
    data = learner[['user_id', i]]
    if i == 'wordcount':
        col1.write(alt.Chart(learner).mark_bar().encode(
            x='sum({}):Q'.format(i),
            y=alt.Y('submit_name:N', sort='-x')
        ))
    else:
        col1.write(alt.Chart(learner).mark_bar().encode(
            x='{}:Q'.format(i),
            y=alt.Y('submit_name:N', sort='-x')
        ))
    col2.write(data, user_column_width=True)


for i in range(len(pie)):
    info = pie.iloc[i]
    late = int(info.values[1])
    total = int(info.values[0]) + int(info.values[1])
    percent = int(late/total)
    pop = len(user_df)
    st.sidebar.markdown(f'''<div class="card text-info bg-info mb-3" style="width: 18rem">
    <div class="card-body">
    <h5 class="card-title">Assignment {i + 1}</h5>
    <p class="card-text">Nộp trễ: {late:02d}</p>
    <p>Chưa nộp: {int(info.values[2]):02d}</p>
    <p class="card-text">Tổng: {total:02d}</p>
    </div>
    </div>''', unsafe_allow_html=True)



    

