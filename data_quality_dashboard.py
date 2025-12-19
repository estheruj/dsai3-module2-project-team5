import dash
from dash import dcc, html, dash_table
import dash_bootstrap_components as dbc
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import json

def load_results():
    with open("Great_Expectation/dq_results_detailed.json", "r") as f:
        detailed = json.load(f)
    with open("Great_Expectation/dq_summary.json", "r") as f:
        summary = json.load(f)
    df = pd.read_csv("Great_Expectation/dq_results_summary.csv")
    return detailed, summary, df

detailed_results, summary, csv_df = load_results()

app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

# Summary Cards
summary_cards = dbc.Row([
    dbc.Col(dbc.Card([
        dbc.CardBody([
            html.H4(f"{summary['overall_pass_rate']}%", className="text-danger" if summary['overall_pass_rate'] < 50 else "text-warning" if summary['overall_pass_rate'] < 80 else "text-success"),
            html.P("Overall Pass Rate")
        ])
    ]), width=3),
    dbc.Col(dbc.Card([
        dbc.CardBody([
            html.H4(f"{summary['passed_columns']}", className="text-success"),
            html.P("Passed Columns")
        ])
    ]), width=3),
    dbc.Col(dbc.Card([
        dbc.CardBody([
            html.H4(f"{summary['failed_columns']}", className="text-danger"),
            html.P("Failed Columns")
        ])
    ]), width=3),
    dbc.Col(dbc.Card([
        dbc.CardBody([
            html.H4(f"{summary['threshold']}%", className="text-info"),
            html.P("Threshold")
        ])
    ]), width=3),
])

# Pass/Fail Chart
fig_pass_fail = go.Figure(data=[go.Pie(
    labels=['Passed', 'Failed'],
    values=[summary['passed_columns'], summary['failed_columns']],
    marker_colors=['#28a745', '#dc3545'],
    hole=0.3
)])
fig_pass_fail.update_layout(title="Overall Pass/Fail Distribution")

# Quality Dimensions - Average across all columns (excluding uniqueness from overall)
dimensions = ['Completeness', 'Validity', 'Accuracy', 'Consistency', 'Conformity']
avg_scores = []

for dim in dimensions:
    col_name = f"{dim} Score"
    if col_name in csv_df.columns:
        avg_scores.append(csv_df[col_name].mean())
    else:
        avg_scores.append(0)

fig_dimensions = go.Figure()
fig_dimensions.add_trace(go.Scatterpolar(
    r=avg_scores,
    theta=dimensions,
    fill='toself',
    name='Quality Scores',
    line_color='rgb(0, 123, 255)'
))
fig_dimensions.update_layout(
    polar=dict(
        radialaxis=dict(
            visible=True,
            range=[0, 100],
            tickfont=dict(size=10)
        )
    ),
    showlegend=False,
    title="Data Quality Dimensions (5 dimensions used for threshold)"
)

# Table-wise scores
table_data = []
for table_name, table_info in summary['tables'].items():
    table_data.append({
        'Table': table_name,
        'Total Columns': table_info['total_columns'],
        'Passed': table_info['passed_columns'],
        'Failed': table_info['failed_columns'],
        'Avg Score': table_info['average_score'],
        'Status': '✅ PASS' if table_info['passed'] else '❌ FAIL'
    })
df_tables = pd.DataFrame(table_data)

fig_table_scores = px.bar(
    df_tables, 
    x='Table', 
    y='Avg Score',
    title='Average Quality Score by Table',
    color='Status',
    color_discrete_map={'✅ PASS': '#28a745', '❌ FAIL': '#dc3545'},
    text='Avg Score'
)
fig_table_scores.update_traces(texttemplate='%{text:.1f}%', textposition='outside')
fig_table_scores.update_layout(yaxis_range=[0, 110])

# Dimension breakdown bar chart
dim_data = []
for dim in dimensions:
    col_name = f"{dim} Score"
    if col_name in csv_df.columns:
        avg_score = csv_df[col_name].mean()
        dim_data.append({'Dimension': dim, 'Average Score': avg_score})

# Add uniqueness separately (informational only)
if 'Uniqueness Score' in csv_df.columns:
    dim_data.append({'Dimension': 'Uniqueness*', 'Average Score': csv_df['Uniqueness Score'].mean()})

df_dimensions = pd.DataFrame(dim_data)
fig_dimensions_bar = px.bar(
    df_dimensions,
    x='Dimension',
    y='Average Score',
    title='Average Score by Quality Dimension (*Uniqueness not in threshold)',
    color='Average Score',
    color_continuous_scale=['#dc3545', '#ffc107', '#28a745'],
    text='Average Score'
)
fig_dimensions_bar.update_traces(texttemplate='%{text:.1f}%', textposition='outside')
fig_dimensions_bar.update_layout(yaxis_range=[0, 110])

app.layout = dbc.Container([
    html.H1("📊 Data Quality Dashboard - Olist E-commerce Dataset", className="my-4"),
    html.Hr(),
    
    html.H3("Executive Summary", className="my-3"),
    html.P("Note: Overall scores calculated from 5 dimensions (Completeness, Validity, Accuracy, Consistency, Conformity). Uniqueness is tracked but not included in pass/fail threshold.", 
           className="text-muted small"),
    summary_cards,
    html.Br(),
    
    dbc.Row([
        dbc.Col(dcc.Graph(figure=fig_pass_fail), width=6),
        dbc.Col(dcc.Graph(figure=fig_table_scores), width=6),
    ]),
    
    html.Hr(),
    html.H3("Quality Dimensions Analysis", className="my-3"),
    dbc.Row([
        dbc.Col(dcc.Graph(figure=fig_dimensions), width=6),
        dbc.Col(dcc.Graph(figure=fig_dimensions_bar), width=6),
    ]),
    
    html.Hr(),
    html.H3("Table-wise Summary", className="my-3"),
    dash_table.DataTable(
        data=df_tables.to_dict('records'),
        columns=[{"name": i, "id": i} for i in df_tables.columns],
        style_cell={'textAlign': 'center', 'padding': '10px'},
        style_header={'backgroundColor': '#007bff', 'color': 'white', 'fontWeight': 'bold'},
        style_data_conditional=[
            {'if': {'filter_query': '{Status} contains "PASS"'}, 'backgroundColor': '#d4edda'},
            {'if': {'filter_query': '{Status} contains "FAIL"'}, 'backgroundColor': '#f8d7da'},
        ]
    ),
    
    html.Hr(),
    html.H3("Detailed Column Results", className="my-3"),
    dash_table.DataTable(
        data=csv_df.to_dict('records'),
        columns=[{"name": i, "id": i} for i in csv_df.columns],
        page_size=20,
        filter_action="native",
        sort_action="native",
        style_cell={
            'textAlign': 'left', 
            'padding': '8px',
            'minWidth': '80px',
            'maxWidth': '180px',
            'overflow': 'hidden',
            'textOverflow': 'ellipsis',
        },
        style_header={
            'backgroundColor': '#007bff', 
            'color': 'white',
            'fontWeight': 'bold',
            'textAlign': 'center'
        },
        style_data_conditional=[
            {'if': {'filter_query': '{Status} = "PASS"', 'column_id': 'Status'}, 
             'backgroundColor': '#28a745', 'color': 'white', 'fontWeight': 'bold'},
            {'if': {'filter_query': '{Status} = "FAIL"', 'column_id': 'Status'}, 
             'backgroundColor': '#dc3545', 'color': 'white', 'fontWeight': 'bold'},
        ],
        style_table={'overflowX': 'auto'},
    ),
    
    html.Footer([
        html.Hr(),
        html.P(f"📅 Last updated: {summary['timestamp']}", className="text-muted text-center"),
        html.P(f"📊 Analyzed {summary['total_tables']} tables with {summary['total_columns']} columns", 
               className="text-muted text-center small")
    ])
], fluid=True)

if __name__ == "__main__":
    print("\n" + "="*80)
    print("🚀 Starting Data Quality Dashboard - Olist E-commerce Analysis")
    print("="*80)
    print("\n🌐 Dashboard URL: http://127.0.0.1:8051/")
    print("⌨️  Press Ctrl+C to stop the server")
    print("\n📊 Quality dimensions tracked:")
    print("   ✅ Used in threshold: Completeness, Validity, Accuracy, Consistency, Conformity")
    print("   ℹ️  Informational only: Uniqueness")
    print("="*80 + "\n")
    app.run(debug=True, host='127.0.0.1', port=8051)