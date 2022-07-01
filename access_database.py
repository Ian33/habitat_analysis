from PyPDF2 import PdfFileMerger
import pandas as pd
import plotly.graph_objects as go
pd.reset_option('max_columns')
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pdfkit
from scipy.stats import ttest_ind
#import xlsxwriter
import plotly.io as pio
import plotly.express as px
import pyodbc

gdata =  "KCITSQLPRNRPX01"
conn = pyodbc.connect('Driver={SQL Server};'
                          'Server='+gdata+';'
                          'Database=gData;'
                          'Trusted_Connection=yes;')

# access database
access_database_path = "P:/JimBower/FRMP/CAO database/Backend/CAO_habitat_data_be"
conn = pyodbc.connect(r'Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ='+access_database_path+'.accdb;')
cursor = conn.cursor()
'''
table = "Transects"
cursor.execute(f'select * from {table}')w
   
for row in cursor.fetchall():
    print (row)

'''
# get list of watersheds
# table = "Survey_events"
# columns = "Watershed"
# watersheds = cursor.execute(f'select {columns} from {table}')
# print(watersheds)

# access_df = pd.read_sql_query(f'select {columns} from {table};',conn)



def query_access(columns, table):
    access_df = pd.read_sql_query(f'select {columns} from {table};',conn)
    return access_df