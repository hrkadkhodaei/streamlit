# Import python packages
import streamlit as st
import datetime as dt
from os import environ
environ['AWS_PROFILE'] = 'vopak-ds-prd'
from vopak_ds_library.snowpark import get_snowpark_session
from snowflake.snowpark.context import get_active_session
import plotly.express as px
import plotly.graph_objects as go
import numpy as np

def standardize_df(df, should_scale):
    if not should_scale:
        return df
    min_ = df['DATA_VALUE'].min()
    max_ = df['DATA_VALUE'].max()
    if (max_-min_) == 0:
        return df
    df['DATA_VALUE'] = (df['DATA_VALUE'] - min_) / (max_ - min_)
    return df

st.set_page_config(layout="wide")
session = get_snowpark_session(user="DS_PRD", database="DATALAKE_PRD", schema="EDGE_STREAMING", role="F_PRD_DATALAKE_ETL_DS")
# session = get_active_session()
st.title("Tag viewer")
today = dt.datetime.now()
from_date = today - dt.timedelta(days=3)
this_year = today.year

# st.sidebar.divider()

d = st.sidebar.date_input(
    "Select the date range:", (from_date, today),
    dt.date(this_year-8, 1, 1), dt.date(this_year+1, 1, 1))

dt_0 = d[0].strftime("%Y-%m-%d")   
dt_1 = (d[1] if len(d) > 1 else dt.datetime.now()).strftime("%Y-%m-%d")

lst_locations = ['nlbot','nleur1','nlvld','nlttr','nlche','usdex1','nleem1','mypgg1','beanr1','mxatm1','sgseb','usdex2','sgsak','nlvls1','thttt1','sgban','beanr4','mxcoa1','myket','sgtpn','zajnb1','mypgg2','cnsgh2','mxver1','beanr3','ussav','uscrp1','cnqzh','zadur1','aefjr']
lst_selected_locations = st.sidebar.multiselect(f"Select terminals from {len(lst_locations)} options:", lst_locations)
str_selected_locations = "'" + "','".join(lst_selected_locations) + "'"
# session.sql("ALTER SESSION SET SESSION_TIMEOUT = 2").collect()
df_tags = session.sql(f"""
select distinct DATA_TAGID from DATALAKE_PRD.EDGE_STREAMING.SENSOR_MEASUREMENTS as S1
where 1=1
    and DATA_DEVICELOCATION IN ({str_selected_locations})
    and to_date(DATA_SOURCETIMESTAMP) between '{dt_0}' and '{dt_1}'
    and LOWER(DATA_STATUSCODE) = 'good'
order by 1    
""").to_pandas()
tag_list = df_tags["DATA_TAGID"].unique().tolist()
lst_selected_tags = st.multiselect(f"Select tags from {len(df_tags)} options:", tag_list)
str_selected_tags = "'" + "','".join(lst_selected_tags) + "'"
if len(lst_selected_tags) > 0:   
    df_p = session.sql(f"""
    select DATA_TAGID, DATA_LOCAL_SOURCETIMESTAMP, cast(DATA_VALUE as float) AS DATA_VALUE 
    ,DATA_UNITOFMEASURE, DATA_EM_FLOC, DATA_MEASUREMENTTYPE, DATA_MEASUREMENTMETHOD, DATA_MEASUREMENTSPECIFICATION 
    from DATALAKE_PRD.EDGE_STREAMING.SENSOR_MEASUREMENTS as S1
    where 1=1
        and DATA_DEVICELOCATION in ({str_selected_locations})
        and to_date(DATA_SOURCETIMESTAMP) between '{dt_0}' and '{dt_1}'
        and DATA_TAGID in ({str_selected_tags})
        --and LOWER(DATA_STATUSCODE) = 'good'
    order by DATA_LOCAL_SOURCETIMESTAMP
    """).to_pandas()
        #    and to_date(DATA_SOURCETIMESTAMP) between '2023-01-01' and '2023-01-08'
    
    st.sidebar.divider()
    st.sidebar.write(f"Total points: {len(df_p)}")
    should_scale = st.sidebar.checkbox("Scale values between 0 and 1", value=False)   
    times_minus_one = st.sidebar.checkbox("Multiply values by -1 (this can be useful for visualizing the flowrates)", value=False)   
    dfs = {tag: standardize_df(df_p[df_p["DATA_TAGID"] == tag], should_scale) for tag in lst_selected_tags}

    layout = go.Layout(
        legend=dict(orientation='h', x=0.0, y=1.1),  # 'h' for horizontal legend placement
        title='',
        width=np.inf
    )
    fig = go.Figure(layout=layout)
    # fig = go.Figure()
    for tag, df in dfs.items():
        if times_minus_one:
            df["DATA_VALUE"] = df["DATA_VALUE"] * -1
        fig = fig.add_trace(go.Scatter(x=df["DATA_LOCAL_SOURCETIMESTAMP"], y=df["DATA_VALUE"], name=tag))
    st.plotly_chart(fig)
    
    # st.write(f"total rows: {len(df_p)}")
    # # st.subheader("Number of high-fives")
    # st.line_chart(df_p, x="DATA_LOCAL_SOURCETIMESTAMP", y=["DATA_VALUE"])
    
    st.subheader("Underlying data")
    st.dataframe(df_p, use_container_width=True)
