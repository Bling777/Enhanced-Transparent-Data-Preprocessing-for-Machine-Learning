import dash
from dash import dcc, html, Input, Output, State
import dash_table
import pandas as pd
import json
import base64
import io

# Import the functions from the existing scripts
from examples.data_logging import deduplicate, impute_missing_values, PipelineRun
from examples.data_profile_generation import DataProfile
from examples.data_diff_generation import impute_missing_values as diff_impute

app = dash.Dash(__name__)

# Layout of the app
app.layout = html.Div([
    html.H1("Data Processing Dashboard"),
    
    # Upload section
    html.H2("Upload Dataset"),
    dcc.Upload(
        id='upload-data',
        children=html.Div(['Drag and Drop or ', html.A('Select a File')]),
        style={
            'width': '100%',
            'height': '60px',
            'lineHeight': '60px',
            'borderWidth': '1px',
            'borderStyle': 'dashed',
            'borderRadius': '5px',
            'textAlign': 'center',
            'margin': '10px'
        },
        multiple=False
    ),
    
    # Table to display uploaded data
    html.H2("Uploaded Data Preview"),
    dash_table.DataTable(id='data-table', page_size=10),
    
    # Button to run profiling
    html.Button("Generate Data Profile", id='generate-profile', n_clicks=0),
    
    # Data profile result
    html.H2("Data Profile"),
    html.Div(id='data-profile-output'),
    
    # Log section
    html.H2("Pipeline Logs"),
    html.Div(id='pipeline-logs'),
    
    # Button to generate data difference
    html.Button("Compare Data (Original vs Processed)", id='compare-data', n_clicks=0),
    
    # Data diff result
    html.H2("Data Difference"),
    html.Div(id='data-diff-output'),
])

# Helper function to parse uploaded files
def parse_contents(contents, filename):
    content_type, content_string = contents.split(',')
    decoded = base64.b64decode(content_string)
    try:
        if 'csv' in filename:
            df = pd.read_csv(io.StringIO(decoded.decode('utf-8')))
        return df
    except Exception as e:
        print(e)
        return None

# Callback to display uploaded data
@app.callback(
    Output('data-table', 'data'),
    Input('upload-data', 'contents'),
    State('upload-data', 'filename')
)
def update_table(contents, filename):
    if contents is not None:
        df = parse_contents(contents, filename)
        if df is not None:
            return df.to_dict('records')
    return []

# Callback for logging
@app.callback(
    Output('pipeline-logs', 'children'),
    Input('upload-data', 'contents'),
    State('upload-data', 'filename')
)
def generate_logs(contents, filename):
    if contents is not None:
        df = parse_contents(contents, filename)
        
        # Initialize the pipeline run object
        run = PipelineRun(df)
        
        # Run deduplication and imputation
        df = deduplicate(df)
        df = impute_missing_values(df)
        
        # Log the results and display them
        logs = run.get_logs()  # Assuming this returns a list of log entries
        return html.Pre("\n".join(logs))
    
    return "No logs generated yet."

# Callback for data profiling
@app.callback(
    Output('data-profile-output', 'children'),
    Input('generate-profile', 'n_clicks'),
    State('data-table', 'data')
)
def generate_data_profile(n_clicks, table_data):
    if n_clicks > 0 and table_data is not None:
        df = pd.DataFrame(table_data)
        # Generate data profile
        data_profile = DataProfile(dataset=df)
        profile_json = data_profile.as_dict()  # Convert to dict
        
        # Display the profile in the app
        return html.Pre(json.dumps(profile_json, indent=4))
    return ""

# Callback for data difference calculation
@app.callback(
    Output('data-diff-output', 'children'),
    Input('compare-data', 'n_clicks'),
    State('data-table', 'data')
)
def compare_data(n_clicks, table_data):
    if n_clicks > 0 and table_data is not None:
        df = pd.DataFrame(table_data)
        
        # Run the imputation process
        processed_df = diff_impute(df)
        
        # Generate data profiles for both original and processed data
        original_profile = DataProfile(df)
        processed_profile = DataProfile(processed_df)
        
        # Calculate the difference between original and processed datasets
        diff = original_profile.calculate_diff(processed_profile)
        
        # Display the differences
        return html.Pre(json.dumps(diff, indent=4))
    
    return ""

if __name__ == '__main__':
    app.run_server(debug=True)
