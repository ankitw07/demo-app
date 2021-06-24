import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output, State
import plotly.express as px
import pandas as pd
import dash_table
import dash_bootstrap_components as dbc
from clickhouse_driver import Client
from datetime import date, datetime, timedelta
import dash_table as dt
from datetime import datetime as dt1

query_columns = """describe(select epoch,
       plant,
       group,
       fk_id_plant,
       assignee_name,
       any(creation_date)         as creation_date,
       closind_date,
       planned_date,
       planned_to,
       CASE invertername WHEN '' THEN 'Plant' ELSE invertername END as inverter_name,
       comment,
       reason_code,
       description,
       sum(irradiation_validated) as irradiation_validated,
       sum(capacity_operational)  as capacity_operational,
       any(pr_operational)        as pr_operational,
       sum(energy_validated)      as energy_validated
from (
         select startdatetime                          as epoch,
                s.name                                 as plant,
                sg.name                                as group,
                s.id                                   as fk_id_plant,
                concat(u.first_name, ' ', u.last_name) as assignee_name
                 ,
                mc.create_at                           as creation_date,
                mc.enddatetime                         as closind_date,
                startdatetime                          as planned_date,
                enddatetime                            as planned_to,
                mc.invertername as invertername,
                ''                                     as comment,
                m.name                                 as reason_code,
                mc.reason                              as description,
                any(api.irradiation_value)             as irradiation_validated,
                any(api.capacity_operational)          as capacity_operational,
                any(api.pr_acquisition)                as pr_operational,
                any(api.energy_validated)              as energy_validated
         from amp_from_mysql.monitorcomment mc
                  inner join amp_dw.solarplant s on s.id = mc.solarplant_id
                  inner join amp_dw.user u on u.id = mc.createdby_user_id
                  left join amp_dw.monitorsubcategory m on mc.monitorsubcategory_id = m.id
                  inner join amp_dw.api1_monitor api on api.solarplant_id = s.id and toDate(api.pr_datetime) = toDate(mc.meterdate)
                  inner join amp_dw.sitegroup_link sgl on sgl.solarplant_id = s.id
                  inner join amp_dw.sitegroup sg on sg.id = sgl.sitegroup_id
        where (
                                   (
                                           toMonth(toDate(mc.startdatetime)) between 1 and 12 and
                                           toMonth(toDate(mc.enddatetime)) between 1 and 12
                                   )
                                   and toYear(startdatetime) in (2020,2021)
                               )
--                            and s.name = 'Bowerhouse' and invertername = 'WR 106'
                              and sg.name = 'NESF UK'
         group by startdatetime, plant,group,s.id, concat(u.first_name, ' ', u.last_name), mc.create_at,mc.enddatetime, startdatetime, enddatetime, '', reason_code, mc.reason,mc.invertername
         order by epoch desc) as int
         group by epoch, plant, group,fk_id_plant, assignee_name, closind_date, planned_date, planned_to, comment, reason_code,description,invertername)"""
query = """select epoch,
       plant,
       group,
       fk_id_plant,
       assignee_name,
       any(creation_date)         as creation_date,
       closind_date,
       planned_date,
       planned_to,
       CASE invertername WHEN '' THEN 'Plant' ELSE invertername END as inverter_name,
       comment,
       reason_code,
       description,
       sum(irradiation_validated) as irradiation_validated,
       sum(capacity_operational)  as capacity_operational,
       any(pr_operational)        as pr_operational,
       sum(energy_validated)      as energy_validated
from (
         select startdatetime                          as epoch,
                s.name                                 as plant,
                sg.name                                as group,
                s.id                                   as fk_id_plant,
                concat(u.first_name, ' ', u.last_name) as assignee_name
                 ,
                mc.create_at                           as creation_date,
                mc.enddatetime                         as closind_date,
                startdatetime                          as planned_date,
                enddatetime                            as planned_to,
                mc.invertername as invertername,
                ''                                     as comment,
                m.name                                 as reason_code,
                mc.reason                              as description,
                any(api.irradiation_value)             as irradiation_validated,
                any(api.capacity_operational)          as capacity_operational,
                any(api.pr_acquisition)                as pr_operational,
                any(api.energy_validated)              as energy_validated
         from amp_from_mysql.monitorcomment mc
                  inner join amp_dw.solarplant s on s.id = mc.solarplant_id
                  inner join amp_dw.user u on u.id = mc.createdby_user_id
                  left join amp_dw.monitorsubcategory m on mc.monitorsubcategory_id = m.id
                  inner join amp_dw.api1_monitor api on api.solarplant_id = s.id and toDate(api.pr_datetime) = toDate(mc.meterdate)
                  inner join amp_dw.sitegroup_link sgl on sgl.solarplant_id = s.id
                  inner join amp_dw.sitegroup sg on sg.id = sgl.sitegroup_id
                          where mc.startdatetime > {0}
                          and mc.enddatetime < {1}
--                            and s.name = 'Bowerhouse' and invertername = 'WR 106'
         group by startdatetime, plant,group,s.id, concat(u.first_name, ' ', u.last_name), mc.create_at,mc.enddatetime, startdatetime, enddatetime, '', reason_code, mc.reason,mc.invertername
         order by epoch desc) as int
         group by epoch, plant, group,fk_id_plant, assignee_name, closind_date, planned_date, planned_to, comment, reason_code,description,invertername;"""
query = query.format("date_sub(day,7,now())", "now()")
client = Client(host='51.140.223.167', port=9000)
query_df = client.execute(query)
columns_plant = pd.DataFrame(client.execute(query_columns))
columns_plant = columns_plant.iloc[:,0]
df_temp_plant = pd.DataFrame(query_df, columns=columns_plant)
plants = df_temp_plant.to_dict('records')
# query_plant = """select name, id from amp_dw.sitegroup"""
# query_plant_columns = """describe(select name, id from amp_dw.sitegroup)"""
# plant_df = client.execute(query_plant)
# columns_spv = pd.DataFrame(client.execute(query_plant_columns))
# columns_spv = columns_spv.iloc[:,0]
# df_temp_spv = pd.DataFrame(plant_df, columns=columns_spv)
# df_temp_spv = df_temp_spv.rename(columns={'name':'label', 'id':'value'})
# group_spv = df_temp_spv.to_dict('records')
df_temp_plant.set_axis(["Intervention Start", "Project Name", "Group Name", "Plant ID", "Technical Analyst",
                 "Intervention Creation", "Intervention End", "Intervention Planned Start", "Intervention Planned End",
                 "Component", "Intervention Notes", "Intervention Category", "Intervention Description",
                 "Validated Irradiation", "Capacity", "PR", "Validated Energy"], axis='columns', inplace=True)
df = pd.DataFrame(df_temp_plant, columns=["Project Name", "Plant ID", "Technical Analyst", "Group Name", "Intervention Creation",
             "Intervention Start", "Intervention End", "Intervention Planned Start", "Intervention Planned End",
             "Component", "Capacity", "Intervention Category", "Intervention Description", "Intervention Notes",
             "Validated Irradiation", "Validated Energy", "PR"]
)
group = df['Group Name'].unique()
FONT_AWESOME = (
    "https://cdnjs.cloudflare.com/ajax/libs/font-awesome/4.7.0/css/font-awesome.min.css"
)
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.SOLAR, FONT_AWESOME],
                meta_tags=[{'name':'viewport',
                            'content':'width=device-width, initial-scale=1'}])
app.title = "Plant Interventions"
application = app.server
app.layout = html.Div([
html.Div([
    html.Div([
        html.Div([
            html.H5('Plant Interventions',
                    className='text-xl-center text-warning border-info font-weight-normal display-4 mb-4'),
            ])],className='create_container1 four columns',id='title'),
    ]),
html.P('Select Date Range',className='fix_label',style={'color':'white'}),
html.Div([
    dcc.DatePickerRange(
        id='my-date-picker-range',
        calendar_orientation='horizontal',
        day_size=39,
        end_date_placeholder_text="End date",
        with_portal=False,
        first_day_of_week=0,
        reopen_calendar_on_clear=True,
        is_RTL=False,
        clearable=True,
        number_of_months_shown=1,
        min_date_allowed=dt1(2018, 1, 1),
        max_date_allowed=date.today(),
        initial_visible_month=date.today(),
        start_date=date.today()-timedelta(days=7),
        end_date=date.today()-timedelta(days=1),
        display_format='MMMM Y, DD',
        month_format='MMMM Y, DD',
        minimum_nights=0,
        persistence=True,
        persisted_props=['start_date'],
        persistence_type='session',
        updatemode='singledate'
    ),

        # max_date_allowed= date.today(),
        # initial_visible_month=date.today(),
        # end_date = date.today(),
        # start_date = date.today() - timedelta(days=30),)
    ]),
    # html.Div([
    #     dcc.Dropdown(id='plant',
    #                  options=spv,
    #                  optionHeight=25,
    #                  # value='Aller Court',
    #                  multi=True,
    #                  searchable=True,
    #                  placeholder='Select Plant',
    #                  clearable=True,
    #                  style={'width':"60%"},
    #                  className='select_box',
    #                  )]),
    html.Div([
         html.P('Select Group',className='fix_label',style={'color':'white'}),
         dcc.Dropdown(id='group',
                           options = [{'label': x, 'value': x } for x in (df['Group Name'].unique())],
                           value='NESF UK',
                           optionHeight = 25,
                           multi = True,
                           searchable = True,
                           # placeholder = 'Select Group',
                           clearable = True,
                           style = {'width': "48%"},
                           className = 'dcc_compon'),

         html.P('Select Plant',className='fix_label',style={'color':'white'}),
         dcc.Dropdown(id='plant',
                           options = [],
                           optionHeight = 25,
                           multi = True,
                           searchable = True,
                           # placeholder = 'Select Plant',
                           clearable = True,
                           style = {'width': "48%"},
                           className = 'dcc_compon',
                           )], className='create_container three columns'),
    # dbc.Button(id='btn',
    #            children=[html.I(className="fa fa-download mr-2"),'Download'],
    #            color='info',
    #            className='mt-1'),
    # dcc.Download(id="download-component"),

html.Div([dash_table.DataTable(
    id="table",
    data=[],
    # data=df.to_dict('records'),
    columns=[{"name": i, "id": i} for i in df],
    export_format="xlsx",
    editable=True,
    filter_action="native",
    sort_mode="single",
    column_selectable="multi",
    row_deletable=False,
    selected_columns=[],
    selected_rows=[],
    page_action="native",
    page_current=0,
    sort_action='native',
    page_size=10,
    style_cell={'height': "auto",
                    'minWidth':100, 'maxWidth':100, 'width':100,
                    'whiteSpace':'normal','padding':'5px'},
    style_header={'backgroundColor':'rgb(230,230,230)',
                      'fontWeight':'bold',
                      'border':'1px solid red'},
    style_cell_conditional=[
            {
                'if': {'column_id': c},
                'textAlign': 'left'
            } for c in ['name']
        ],
    style_data={
            'whiteSpace': 'normal',
            'height': 'auto',
            'border':'0.5px solid blue'},
    ),
]),
])
# @app.callback(
#     Output('table','data'),
#     [Input('group','value')]
# )
# def display_table(select_group):
#     data_table = df[df['Group Name'].isin(select_group)]
#     return data_table

@app.callback(Output('plant', 'options'),
              [Input('group', 'value')])
def update_country(group):
    group_df = df[df['Group Name'] == group]
    return [{'label':i,'value':i} for i in group_df['Project Name'].unique()]

@app.callback(Output('plant', 'value'),
              [Input('plant', 'options')])
def update_country(plant):
   return [k['value'] for k in plant][0]

@app.callback (Output(component_id="table", component_property='data'),
              [Input(component_id='my-date-picker-range', component_property='start_date'),
               Input(component_id='my-date-picker-range', component_property='end_date')])

def set_value(start_date, end_date):
    client = Client(host='51.140.223.167', port=9000)
    query = """select epoch,
           plant,
           group,
           fk_id_plant,
           assignee_name,
           any(creation_date)         as creation_date,
           closind_date,
           planned_date,
           planned_to,
           CASE invertername WHEN '' THEN 'Plant' ELSE invertername END as inverter_name,
           comment,
           reason_code,
           description,
           sum(irradiation_validated) as irradiation_validated,
           sum(capacity_operational)  as capacity_operational,
           any(pr_operational)        as pr_operational,
           sum(energy_validated)      as energy_validated
    from (
             select startdatetime                          as epoch,
                    s.name                                 as plant,
                    sg.name                                as group,
                    s.id                                   as fk_id_plant,
                    concat(u.first_name, ' ', u.last_name) as assignee_name
                     ,
                    mc.create_at                           as creation_date,
                    mc.enddatetime                         as closind_date,
                    startdatetime                          as planned_date,
                    enddatetime                            as planned_to,
                    mc.invertername as invertername,
                    ''                                     as comment,
                    m.name                                 as reason_code,
                    mc.reason                              as description,
                    any(api.irradiation_value)             as irradiation_validated,
                    any(api.capacity_operational)          as capacity_operational,
                    any(api.pr_acquisition)                as pr_operational,
                    any(api.energy_validated)              as energy_validated
             from amp_from_mysql.monitorcomment mc
                      inner join amp_dw.solarplant s on s.id = mc.solarplant_id
                      inner join amp_dw.user u on u.id = mc.createdby_user_id
                      left join amp_dw.monitorsubcategory m on mc.monitorsubcategory_id = m.id
                      inner join amp_dw.api1_monitor api on api.solarplant_id = s.id and toDate(api.pr_datetime) = toDate(mc.meterdate)
                      inner join amp_dw.sitegroup_link sgl on sgl.solarplant_id = s.id
                      inner join amp_dw.sitegroup sg on sg.id = sgl.sitegroup_id
                              where mc.startdatetime > '{0} 00:00:00'
                              and mc.enddatetime < '{1} 00:00:00'
    --                            and s.name = 'Bowerhouse' and invertername = 'WR 106'
             group by startdatetime, plant,group,s.id, concat(u.first_name, ' ', u.last_name), mc.create_at,mc.enddatetime, startdatetime, enddatetime, '', reason_code, mc.reason,mc.invertername
             order by epoch desc) as int
             group by epoch, plant, group,fk_id_plant, assignee_name, closind_date, planned_date, planned_to, comment, reason_code,description,invertername;"""
    query = query.format(start_date, end_date)
    df_temp_plant = pd.DataFrame(client.execute(query))
    df_temp_plant.set_axis(["Intervention Start", "Project Name", "Group Name", "Plant ID", "Technical Analyst",
                            "Intervention Creation", "Intervention End", "Intervention Planned Start",
                            "Intervention Planned End",
                            "Component", "Intervention Notes", "Intervention Category", "Intervention Description",
                            "Validated Irradiation", "Capacity", "PR", "Validated Energy"], axis='columns',
                           inplace=True)
    df = pd.DataFrame(df_temp_plant, columns=["Project Name", "Plant ID", "Technical Analyst", "Group Name",
                                              "Intervention Creation",
                                              "Intervention Start", "Intervention End", "Intervention Planned Start",
                                              "Intervention Planned End",
                                              "Component", "Capacity", "Intervention Category",
                                              "Intervention Description", "Intervention Notes",
                                              "Validated Irradiation", "Validated Energy", "PR"])
    data = df.to_dict('records')
    return data
if __name__ == '__main__':
  application.run(debug=True, host='127.0.0.1', port='4000')