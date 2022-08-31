# -*- coding: utf-8 -*-
"""
Created on Thu Apr 22 08:48:07 2021

@author: IHiggins
"""
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
#import imgkit

#from fpdf import FPDF
pio.renderers.default = "browser"
pd.set_option('display.max_columns', None)
#IMPORT DATA
''' OLD DATA '''
from access_database import all_data

# recreate dataframe from access database
DATA_2018 = all_data(2018)
DATA_2019 = all_data(2019)
DATA_2020 = all_data(2020)
DATA_2021 = all_data(2021)

#W:\STS\ScienceCAO\2_Stream Complexity\03_Results\Physical Habitat\Physical_Stream_Surveys
DATA_2009 = pd.read_excel(r'W:\STS\ScienceCAO\2_Stream Complexity\03_Results\Physical Habitat\Physical_Stream_Surveys.xls', sheet_name='Master Summary Table', header=0, nrows=27, usecols = "B,C,D,H,L,P,T,X,AB,AF,AJ,AN,AR,AV,AZ,BD,BH,BL,BP")
DATA_2010 = pd.read_excel(r'W:\STS\ScienceCAO\2_Stream Complexity\03_Results\Physical Habitat\Physical_Stream_Surveys.xls', sheet_name='Master Summary Table', header=0, nrows=27, usecols = "B,C,E,I,M,Q,U,Y,AC,AG,AK,AO,AS,AW,BA,BE,BI,BM,BQ")
DATA_2011 = pd.read_excel(r'W:\STS\ScienceCAO\2_Stream Complexity\03_Results\Physical Habitat\Physical_Stream_Surveys.xls', sheet_name='Master Summary Table', header=0, nrows=27, usecols = "B,C,F,J,N,R,V,Z,AD,AH,AL,AP,AT,AX,BB,BF,BJ,BN,BR")
DATA_2012 = pd.read_excel(r'W:\STS\ScienceCAO\2_Stream Complexity\03_Results\Physical Habitat\Physical_Stream_Surveys.xls', sheet_name='Master Summary Table', header=0, nrows=27, usecols = "A,C,G,K,O,S,W,AA,AE,AI,AM,AQ,AU,AY,BC,BG,BK,BO,BS")
#df.reindex(columns=['Name', 'Gender', 'Age', 'City', 'Education'])
DATA_2009["SITE"] = ['WRU','WRL','WRC','NSU','NSL','NSC','SSU','SSL','SSC','TRU','TRL','TRC','CYU','CYL','CYC','FRU','FRL','FRC','THU','THL','THC','WSU','WSL','WSC','JDU','JDL','JDC']
DATA_2009 = DATA_2009.reindex([12,13,14,3,4,5,6,7,8,15,16,17,24,25,26,18,19,20,9,10,11,0,1,2,21,22,23])
DATA_2009 = DATA_2009.iloc[:, [19,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18]]
# Filter out for just the combind reaches
DATA_2009 = DATA_2009[DATA_2020.Reach == 'Combined']

DATA_2010["SITE"] = ['WRU','WRL','WRC','NSU','NSL','NSC','SSU','SSL','SSC','TRU','TRL','TRC','CYU','CYL','CYC','FRU','FRL','FRC','THU','THL','THC','WSU','WSL','WSC','JDU','JDL','JDC']
DATA_2010 = DATA_2010.reindex([12,13,14,3,4,5,6,7,8,15,16,17,24,25,26,18,19,20,9,10,11,0,1,2,21,22,23])
DATA_2010 = DATA_2010.iloc[:, [19,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18]]
# Filter out for just the combind reaches
DATA_2010 = DATA_2010[DATA_2010.Reach == 'Combined']

DATA_2011["SITE"] = ['WRU','WRL','WRC','NSU','NSL','NSC','SSU','SSL','SSC','TRU','TRL','TRC','CYU','CYL','CYC','FRU','FRL','FRC','THU','THL','THC','WSU','WSL','WSC','JDU','JDL','JDC']
DATA_2011 = DATA_2011.reindex([12,13,14,3,4,5,6,7,8,15,16,17,24,25,26,18,19,20,9,10,11,0,1,2,21,22,23])
DATA_2011 = DATA_2011.iloc[:, [19,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18]]
# Filter out for just the combind reaches
DATA_2011 = DATA_2011[DATA_2011.Reach == 'Combined']

DATA_2012["SITE"] = ['WRU','WRL','WRC','NSU','NSL','NSC','SSU','SSL','SSC','TRU','TRL','TRC','CYU','CYL','CYC','FRU','FRL','FRC','THU','THL','THC','WSU','WSL','WSC','JDU','JDL','JDC']
DATA_2012 = DATA_2012.reindex([12,13,14,3,4,5,6,7,8,15,16,17,24,25,26,18,19,20,9,10,11,0,1,2,21,22,23])
DATA_2012 = DATA_2012.iloc[:, [19,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18]]
# Filter out for just the combind reaches
DATA_2012 = DATA_2012[DATA_2012.Reach == 'Combined']


'''ORIGONAL '''

ORIGIONAL = pd.read_excel(r'P:\JimBower\FRMP\CAO Origional Habitat Data\Original Habitat Data.xls',sheet_name='Reach averages by site',header=0)

def SUBSTRATE_SITES():
    def SUBSTRATE():
        ''' returns total count of substrate class for site per year'''
        #column_names = ["SILT", "SAND", "FINE_GRAVEL", "COURSE_GRAVEL","COBBLE","SMALL_BOULDER","LARGE_BOULDER","HARDPAN"]

        #WATER_YEAR_SUBSTRATE = pd.DataFrame(columns = column_names)
       
        #CHERRY_SUBSTRATE = pd.read_excel(r'P:\JimBower\FRMP\CAO Habitat Data_2018\CAO Physical Stream Surveys_2018.xlsx', sheet_name = 'CY_18', skiprows = 45,  nrows= 8, usecols = 'S')
        SITE_SILT = pd.read_excel(r'P:\JimBower\FRMP\CAO Habitat Data_'+str(LONG_YEAR)+'\CAO Physical Stream Surveys_'+str(LONG_YEAR)+'.xlsx', sheet_name = str(SITE)+'_'+str(SHORT_YEAR), skiprows = 45,  nrows= 1, usecols = 'S', header=None)
        SITE_SAND = pd.read_excel(r'P:\JimBower\FRMP\CAO Habitat Data_'+str(LONG_YEAR)+'\CAO Physical Stream Surveys_'+str(LONG_YEAR)+'.xlsx', sheet_name = str(SITE)+'_'+str(SHORT_YEAR), skiprows = 46,  nrows= 1, usecols = 'S', header=None)
        SITE_FINE_GRAVEL = pd.read_excel(r'P:\JimBower\FRMP\CAO Habitat Data_'+str(LONG_YEAR)+'\CAO Physical Stream Surveys_'+str(LONG_YEAR)+'.xlsx', sheet_name = str(SITE)+'_'+str(SHORT_YEAR), skiprows = 47,  nrows= 1, usecols = 'S', header=None)
        SITE_COURSE_GRAVEL = pd.read_excel(r'P:\JimBower\FRMP\CAO Habitat Data_'+str(LONG_YEAR)+'\CAO Physical Stream Surveys_'+str(LONG_YEAR)+'.xlsx', sheet_name = str(SITE)+'_'+str(SHORT_YEAR), skiprows = 48,  nrows= 1, usecols = 'S', header=None)
        SITE_COBBLE = pd.read_excel(r'P:\JimBower\FRMP\CAO Habitat Data_'+str(LONG_YEAR)+'\CAO Physical Stream Surveys_'+str(LONG_YEAR)+'.xlsx', sheet_name = str(SITE)+'_'+str(SHORT_YEAR), skiprows = 49,  nrows= 1, usecols = 'S', header=None)
        SITE_SMALL_BOULDER = pd.read_excel(r'P:\JimBower\FRMP\CAO Habitat Data_'+str(LONG_YEAR)+'\CAO Physical Stream Surveys_'+str(LONG_YEAR)+'.xlsx', sheet_name = str(SITE)+'_'+str(SHORT_YEAR), skiprows = 50,  nrows= 1, usecols = 'S', header=None)
        SITE_LARGE_BOULDER = pd.read_excel(r'P:\JimBower\FRMP\CAO Habitat Data_'+str(LONG_YEAR)+'\CAO Physical Stream Surveys_'+str(LONG_YEAR)+'.xlsx', sheet_name = str(SITE)+'_'+str(SHORT_YEAR), skiprows = 51,  nrows= 1, usecols = 'S', header=None)
        SITE_HARDPAN = pd.read_excel(r'P:\JimBower\FRMP\CAO Habitat Data_'+str(LONG_YEAR)+'\CAO Physical Stream Surveys_'+str(LONG_YEAR)+'.xlsx', sheet_name = str(SITE)+'_'+str(SHORT_YEAR), skiprows = 52,  nrows= 1, usecols = 'S', header=None)
    
        SITE_SILT = SITE_SILT.to_numpy().reshape(-1)[0]
        SITE_SAND = SITE_SAND.to_numpy().reshape(-1)[0]
        SITE_FINE_GRAVEL = SITE_FINE_GRAVEL.to_numpy().reshape(-1)[0]
        SITE_COURSE_GRAVEL = SITE_COURSE_GRAVEL.to_numpy().reshape(-1)[0]
        SITE_COBBLE = SITE_COBBLE.to_numpy().reshape(-1)[0]
        SITE_SMALL_BOULDER = SITE_SMALL_BOULDER.to_numpy().reshape(-1)[0]
        SITE_LARGE_BOULDER = SITE_LARGE_BOULDER.to_numpy().reshape(-1)[0]
        SITE_HARDPAN = SITE_HARDPAN.to_numpy().reshape(-1)[0]
    

        d = {'SILT': [SITE_SILT], 'SAND': [SITE_SAND], 'FINE_GRAVEL': [SITE_FINE_GRAVEL], 'COURSE_GRAVEL': [SITE_COURSE_GRAVEL], 'COBBLE': [SITE_COBBLE], 'SMALL_BOULDER': [SITE_SMALL_BOULDER], 'LARGE_BOULDER': [SITE_LARGE_BOULDER], 'HARDPAN': [SITE_HARDPAN]}
        SITE_TOTAL_SUBSTRATE = pd.DataFrame(data=d)
   #     print(SITE_HARDPAN.iloc[0])
        #print("WATER YEAR SUBSTRATE")
        #print(SITE_TOTAL_SUBSTRATE)
        return SITE_TOTAL_SUBSTRATE    
    
    
    from access_database import query_access
    # get list of watersheds
    # should probably move this to beginning of program
    watersheds = query_access("Watershed", "Survey_events")
    watersheds = watersheds.drop_duplicates()

    for Watershed in watersheds:
        substrate = query_access("Survey_FK, Substrate_lcenter, Substrate_center, Substrate_rcenter, Substrate_right", "Transects")
        print(substrate)
    
    SITE = 'CY'
    CY_SUBSTRATE = SUBSTRATE()
    print("          CY")
    SITE = 'NS'
    NS_SUBSTRATE = SUBSTRATE()
    print("          NS")
    SITE = 'SS'
    SS_SUBSTRATE = SUBSTRATE()
    print("          SS")
    SITE = 'FR'
    FR_SUBSTRATE = SUBSTRATE()
    print("          FR")
    SITE = 'JD'
    JD_SUBSTRATE = SUBSTRATE()
    print("          JD")
    SITE = 'TH'
    TH_SUBSTRATE = SUBSTRATE()
    print("          TH")
    SITE = 'TR'
    TR_SUBSTRATE = SUBSTRATE()
    print("          TR")
    SITE = 'WR'
    WR_SUBSTRATE = SUBSTRATE()
    print("          WR")
    SITE = 'WS'
    WS_SUBSTRATE = SUBSTRATE()
    print("          WS")
    
    
    SITE_ALL = CY_SUBSTRATE
    SITE_ALL = SITE_ALL.append(NS_SUBSTRATE, ignore_index=True)
    SITE_ALL = SITE_ALL.append(SS_SUBSTRATE, ignore_index=True)
    SITE_ALL = SITE_ALL.append(FR_SUBSTRATE, ignore_index=True)
    SITE_ALL = SITE_ALL.append(JD_SUBSTRATE, ignore_index=True)
    SITE_ALL = SITE_ALL.append(TH_SUBSTRATE, ignore_index=True)
    SITE_ALL = SITE_ALL.append(TR_SUBSTRATE, ignore_index=True)
    SITE_ALL = SITE_ALL.append(WR_SUBSTRATE, ignore_index=True)
    SITE_ALL = SITE_ALL.append(WS_SUBSTRATE, ignore_index=True)
    SITE_NAMES = ['CYC','NSC','SSC','FRC','JDC','THC','TRC','WRC','WSC']
    SITE_ALL['Site'] = SITE_NAMES
    return(SITE_ALL)

def HYDROLOGY_GDATA():
    
    def HYDRO_QUERY():
        High_Pulse_Count = pd.read_sql_query('select High_Pulse_Count from tblStreamMetrics WHERE G_ID = '+str(G_ID)+' AND Water_Year = '+str(LONG_YEAR),conn)
        Pulse_Range = pd.read_sql_query('select Pulse_Range from tblStreamMetrics WHERE G_ID = '+str(G_ID)+' AND Water_Year = '+str(LONG_YEAR),conn)
        TQ_Mean = pd.read_sql_query('select TQ_Mean from tblStreamMetrics WHERE G_ID = '+str(G_ID)+' AND Water_Year = '+str(LONG_YEAR),conn)
        R_B_Index = pd.read_sql_query('select R_B_Index from tblStreamMetrics WHERE G_ID = '+str(G_ID)+' AND Water_Year = '+str(LONG_YEAR),conn)

        High_Pulse_Count = High_Pulse_Count.to_numpy().reshape(-1)[0]
        Pulse_Range = Pulse_Range.to_numpy().reshape(-1)[0]
        TQ_Mean = TQ_Mean.to_numpy().reshape(-1)[0]
        R_B_Index = R_B_Index.to_numpy().reshape(-1)[0]
        
        d = {"High_Pulse_Count": [High_Pulse_Count], "Pulse_Range": [Pulse_Range], "TQ_Mean": [TQ_Mean], "R_B_Index": [R_B_Index]}
        SITE_HYDRO = pd.DataFrame(data=d)

        return SITE_HYDRO
    
    # Cherry Trib 05b
    G_ID = 859
    CY_HYDRO = HYDRO_QUERY()
    # North Seidel RPWS_SEIMN - 1692
    G_ID = 1692
    NS_HYDRO = HYDRO_QUERY()
    # South Seidel RPWS_SEIMS - 1693
    G_ID = 1693
    SS_HYDRO = HYDRO_QUERY()
    # Fisher 65B = 703
    G_ID = 703
    FR_HYDRO = HYDRO_QUERY()
    # Judd 28a = 319
    G_ID = 319
    JD_HYDRO = HYDRO_QUERY()
    # Tahlequah 65A = 704
    G_ID = 704
    TH_HYDRO = HYDRO_QUERY()
     # Taylor 31i  103
    G_ID = 103
    TR_HYDRO = HYDRO_QUERY()
    # Webster 31q - 865
    G_ID = 865
    WR_HYDRO = HYDRO_QUERY()   
    # Weiss 53e = 863
    G_ID = 863
    WS_HYDRO = HYDRO_QUERY()

    
    SITE_ALL = CY_HYDRO
    SITE_ALL = SITE_ALL.append(NS_HYDRO, ignore_index=True)
    SITE_ALL = SITE_ALL.append(SS_HYDRO, ignore_index=True)
    SITE_ALL = SITE_ALL.append(FR_HYDRO, ignore_index=True)
    SITE_ALL = SITE_ALL.append(JD_HYDRO, ignore_index=True)
    SITE_ALL = SITE_ALL.append(TH_HYDRO, ignore_index=True)
    SITE_ALL = SITE_ALL.append(TR_HYDRO, ignore_index=True)
    SITE_ALL = SITE_ALL.append(WR_HYDRO, ignore_index=True)
    SITE_ALL = SITE_ALL.append(WS_HYDRO, ignore_index=True)
    SITE_NAMES = ['CYC','NSC','SSC','FRC','JDC','THC','TRC','WRC','WSC']
    SITE_ALL['Site'] = SITE_NAMES
    return(SITE_ALL)

#### ADD SUBSTRATE #####
SHORT_YEAR = '18'
LONG_YEAR = '2018'

YEAR_SUBSTRATE = SUBSTRATE_SITES()
DATA_2018_WHOLEREACH = DATA_2018_WHOLEREACH.merge(YEAR_SUBSTRATE, how='inner', on="Site")

SHORT_YEAR = '19'
LONG_YEAR = '2019'

YEAR_SUBSTRATE = SUBSTRATE_SITES()
DATA_2019_WHOLEREACH = DATA_2019_WHOLEREACH.merge(YEAR_SUBSTRATE, how='inner', on="Site")

SHORT_YEAR = '20'
LONG_YEAR = '2020'


YEAR_SUBSTRATE = SUBSTRATE_SITES()
DATA_2020_WHOLEREACH = DATA_2020_WHOLEREACH.merge(YEAR_SUBSTRATE, how='inner', on="Site")


### ADD HYDROLOGY DATA #####
LONG_YEAR = '2018'
YEAR_HYDRO = HYDROLOGY_GDATA()
DATA_2018_WHOLEREACH = DATA_2018_WHOLEREACH.merge(YEAR_HYDRO, how='inner', on="Site")



LONG_YEAR = '2019'
YEAR_HYDRO = HYDROLOGY_GDATA()
DATA_2019_WHOLEREACH = DATA_2019_WHOLEREACH.merge(YEAR_HYDRO, how='inner', on="Site")

LONG_YEAR = '2020'
YEAR_HYDRO = HYDROLOGY_GDATA()
DATA_2020_WHOLEREACH = DATA_2020_WHOLEREACH.merge(YEAR_HYDRO, how='inner', on="Site")



# RESET COLUMN NAMES
column_names = DATA_2018_WHOLEREACH.columns



#print(column_names)

# CREATE DATAFRAMES FOR EACH SITE
def PARSE_YEAR():
    SITE_DF = pd.DataFrame(data = [DATA_2009_WHOLEREACH.iloc[i].tolist(),DATA_2010_WHOLEREACH.iloc[i].tolist(),DATA_2011_WHOLEREACH.iloc[i].tolist(),DATA_2012_WHOLEREACH.iloc[i].tolist(),DATA_2018_WHOLEREACH.iloc[i].tolist(), DATA_2019_WHOLEREACH.iloc[i].tolist(), DATA_2020_WHOLEREACH.iloc[i].tolist()], columns = column_names, index = [2009, 2010, 2011, 2012, 2018, 2019, 2020])
    
    fig = make_subplots(rows=1, cols=1,subplot_titles=(''), horizontal_spacing = 0.1, vertical_spacing = 0.07)

    fig.append_trace(go.Scatter(
            x=SITE_DF.index,
            y=SITE_DF['Thal Len (m)'],
            name='Thal Len (m)'), row=1, col=1)
    fig.append_trace(go.Scatter(
            x=SITE_DF.index,
            y=SITE_DF['Avg Thal Dep (cm)'],
            name='Avg Thal Dep (cm)'), row=1, col=1)
    fig.append_trace(go.Scatter(
            x=SITE_DF.index,
            y=SITE_DF['SE Thal Dep (cm)'],
            name='SE Thal Dep (cm)'), row=1, col=1)
    fig.append_trace(go.Scatter(
            x=SITE_DF.index,
            y=SITE_DF['CV Thal Dep (%)'],
            name='CV Thal Dep (%)'), row=1, col=1)
    fig.append_trace(go.Scatter(
            x=SITE_DF.index,
            y=SITE_DF['Act Wid (cm)'],
            name='Act Wid (cm)'), row=1, col=1)
    fig.append_trace(go.Scatter(
            x=SITE_DF.index,
            y=SITE_DF['Wet Wid (cm)'],
            name='Wet Wid (cm)'), row=1, col=1)
    fig.append_trace(go.Scatter(
            x=SITE_DF.index,
            y=SITE_DF['Cnt of Pool'],
            name='Cnt of Pool'), row=1, col=1)
    fig.append_trace(go.Scatter(
            x=SITE_DF.index,
            y=SITE_DF['Sum of Pool Len (m)'],
            name='Sum of Pool Len (m)'), row=1, col=1)
    fig.append_trace(go.Scatter(
            x=SITE_DF.index,
            y=SITE_DF['Sum of Res Dep (cm)'],
            name='Sum of Res Dep (cm)'), row=1, col=1)
    fig.append_trace(go.Scatter(
            x=SITE_DF.index,
            y=SITE_DF['Avg of Res Dep (cm)'],
            name='Avg of Res Dep (cm)'), row=1, col=1)
    fig.append_trace(go.Scatter(
            x=SITE_DF.index,
            y=SITE_DF['LW count'],
            name='LW count'), row=1, col=1)
    
    fig.append_trace(go.Scatter(
            x=SITE_DF.index,
            y=SITE_DF['SILT'],
            name='Silt'), row=1, col=1)
    fig.append_trace(go.Scatter(
            x=SITE_DF.index,
            y=SITE_DF['SAND'],
            name='Sand'), row=1, col=1)
    fig.append_trace(go.Scatter(
            x=SITE_DF.index,
            y=SITE_DF['FINE_GRAVEL'],
            name='Fine Gravel'), row=1, col=1)
    fig.append_trace(go.Scatter(
            x=SITE_DF.index,
            y=SITE_DF['COURSE_GRAVEL'],
            name='Course Gravel'), row=1, col=1)
    fig.append_trace(go.Scatter(
            x=SITE_DF.index,
            y=SITE_DF['COBBLE'],
            name='Cobble'), row=1, col=1)
    fig.append_trace(go.Scatter(
            x=SITE_DF.index,
            y=SITE_DF['SMALL_BOULDER'],
            name='Small Boulder'), row=1, col=1)
    fig.append_trace(go.Scatter(
            x=SITE_DF.index,
            y=SITE_DF['LARGE_BOULDER'],
            name='Large Boulder'), row=1, col=1)
    fig.append_trace(go.Scatter(
            x=SITE_DF.index,
            y=SITE_DF['HARDPAN'],
            name='Hardpan'), row=1, col=1)
     
    fig.update_layout(height=500, width=500, title_text=SITE_GRAPH)
    fig.update_xaxes(showline=True, linewidth=.5, linecolor='black', mirror=True)
    fig.update_yaxes(showline=True, linewidth=.5, linecolor='black', mirror=True)

        
    fig.update_layout(plot_bgcolor='rgba(0,0,0,0)')
    fig.update_layout(font=dict(size=10))
    fig.update_xaxes(showgrid=True)
    fig.update_yaxes(showgrid=True)
    fig.update_xaxes(showticklabels=False)
    fig.update_yaxes(showticklabels=False)
    fig.update_layout(
            margin=dict(l=10, r=10, t=50, b=20)
            )
   
    
    # CALCULATE STATISITCS FOR EACH SITES DATAFRAME
    # I think this ignores zeros and nans in the dataframe
    SITE_DF.loc['mean'] = SITE_DF.iloc[0:2].mean()
    SITE_DF.loc['stdev'] = SITE_DF.iloc[0:2].std()
    
     # ROUND
    SITE_DF = round(SITE_DF,2)
    return SITE_DF
 
# RETURNS A DATAFRAME FOR EACH SITE"
i = 0
SITE_GRAPH = 'CHERRY'
PARSE_YEAR()
CHERRY = PARSE_YEAR()

#print(CHERRY)

i = 1
SITE_GRAPH = 'NORTH_SEIDEL'
PARSE_YEAR()
NORTH_SEIDEL = PARSE_YEAR()
#print(NORTH_SEIDEL)

i = 2
SITE_GRAPH = 'SOUTH_SEIDEL'
PARSE_YEAR()
SOUTH_SEIDEL = PARSE_YEAR()
#print(SOUTH_SEIDEL)

i = 3
SITE_GRAPH = 'FISHER'
PARSE_YEAR()
FISHER = PARSE_YEAR()
#print(FISHER)

i = 4
SITE_GRAPH = 'JUDD'
PARSE_YEAR()
JUDD = PARSE_YEAR()
#print(JUDD)

i = 5
SITE_GRAPH = 'TAHLAQUAH'
PARSE_YEAR()
TAHLAQUAH = PARSE_YEAR()
#print(TAHLAQUAH)

i = 6
SITE_GRAPH = 'TAYLOR'
PARSE_YEAR()
TAYLOR = PARSE_YEAR()
#print(TAYLOR)

i = 7
SITE_GRAPH = 'WEBSTER'
PARSE_YEAR()
WEBSTER = PARSE_YEAR()
#print(WEBSTER)

i = 8
SITE_GRAPH ='WEISS'
PARSE_YEAR()
WEISS = PARSE_YEAR()
#print(WEISS)

"""ADD EXCEL EXPORT HERE SEE XLSWRITER"""

# CREATE TREATMENT AND CONTROL DFS WITH AVERAGES FOR STUDY PEROID
TREATMENT_SITES = pd.DataFrame(data = [CHERRY.loc['mean'].tolist(), FISHER.loc['mean'].tolist(), JUDD.loc['mean'].tolist(), TAHLAQUAH.loc['mean'].tolist(), TAYLOR.loc['mean'].tolist(), WEISS.loc['mean'].tolist()], columns = column_names, index = ['CHERRY','FISHER','JUDD','TAHLAQUAH','TAYLOR','WEISS'])
#TREATMENT_SITES.dropna(inplace=True)
#print(TREATMENT_SITES)
CONTROL_SITES = pd.DataFrame(data = [NORTH_SEIDEL.loc['mean'].tolist(), SOUTH_SEIDEL.loc['mean'].tolist(), WEBSTER.loc['mean'].tolist()], columns = column_names, index = ['NORTH_SEIDEL','SOUTH_SEIDEL','WEBSTER'])
#CONTROL_SITES.dropna(inplace=True)
#print(CONTROL_SITES)
##CONTROL_SITES = pd.DataFrame(data = [DATA_2018_WHOLEREACH.iloc[i].tolist(), DATA_2019_WHOLEREACH.iloc[i].tolist(), DATA_2020_WHOLEREACH.iloc[i].tolist()], columns = column_names, index = [2018, 2019, 2020])


#print(TREATMENT_SITES['Act Wid (cm)'].tolist())

def T_TEST_FUNCTION():
    # this just looks at the mean see above pdf.dataframe = dataframe[mean]
    cat1 = TREATMENT_SITES[column].tolist()
    cat2 = CONTROL_SITES[column].tolist()
    ##### if p is les then .01 highly significant if p is les then .05 significant if p is less then .1 marginally significant
    ttest_ind(cat2, cat1)
    print(column)
    print(ttest_ind(cat1, cat2, equal_var=False))
    
def MAKE_FIGURE():
    fig = make_subplots(rows=1, cols=2,subplot_titles=(''), horizontal_spacing = 0.02, vertical_spacing = 0.07)
    Type = column
    ROW = 1
    COL = 1
    # YOU WILL NEED TO CHANGE THIS AS MORE YEARS ARE ADDED
    YEARS = 7
    # line
    COLOR='darksalmon'
    fig.append_trace(go.Scatter(
            x=WEBSTER.index,
            y=WEBSTER[Type].iloc[0:YEARS],
            name='WEBSTER', line=dict(color=COLOR)), row=ROW, col=COL)
    # Box
    #
    fig.append_trace(go.Box(
            y=WEBSTER[Type].iloc[0:YEARS],
            name='WEBSTER',marker_color = COLOR), row=ROW, col=COL+1)
    
    COLOR='indianred'
    fig.append_trace(go.Scatter(
            x=NORTH_SEIDEL.index,
            y=NORTH_SEIDEL[Type].iloc[0:YEARS],
            name='NORTH_SEIDEL', line=dict(color=COLOR)), row=ROW, col=COL)
    fig.append_trace(go.Box(
            y=NORTH_SEIDEL[Type].iloc[0:YEARS],
            name='NORTH_SEIDEL', marker_color = COLOR), row=ROW, col=COL+1)
    
    COLOR='pink'
    fig.append_trace(go.Scatter(
            x=SOUTH_SEIDEL.index,
            y=SOUTH_SEIDEL[Type].iloc[0:YEARS],
            name='SOUTH_SEIDEL', line=dict(color=COLOR)), row=ROW, col=COL)
    fig.append_trace(go.Box(
            y=SOUTH_SEIDEL[Type].iloc[0:YEARS],
            name='SOUTH_SEIDEL', marker_color = COLOR), row=ROW, col=COL+1)
    
    COLOR='palegreen'
    fig.append_trace(go.Scatter(
            x=FISHER.index,
            y=FISHER[Type].iloc[0:YEARS],
            name='FISHER', line=dict(color=COLOR)), row=ROW, col=COL)
    fig.append_trace(go.Box(
            y=FISHER[Type].iloc[0:YEARS],
            name='FISHER', marker_color = COLOR), row=ROW, col=COL+1)
    
    COLOR = 'mediumseagreen'
    fig.append_trace(go.Scatter(
            x=TAHLAQUAH.index,
            y=TAHLAQUAH[Type].iloc[0:YEARS],
            name='TAHLAQUAH', line=dict(color=COLOR)), row=ROW, col=COL)
    fig.append_trace(go.Box(
            y=TAHLAQUAH[Type].iloc[0:YEARS],
            name='TAHLAQUAH', marker_color = COLOR), row=ROW, col=COL+1)
    
    COLOR = 'mediumaquamarine'
    fig.append_trace(go.Scatter(
            x=JUDD.index,
            y=JUDD[Type].iloc[0:YEARS],
            name='JUDD', line=dict(color=COLOR)), row=ROW, col=COL)
    fig.append_trace(go.Box(
            y=JUDD[Type].iloc[0:YEARS],
            name='JUDD', marker_color = COLOR), row=ROW, col=COL+1)
    
    COLOR = 'mediumslateblue'
    fig.append_trace(go.Scatter(
            x=CHERRY.index,
            y=CHERRY[Type].iloc[0:YEARS],
            name='CHERRY', line=dict(color=COLOR)), row=ROW, col=COL)
    fig.append_trace(go.Box(
            y=CHERRY[Type].iloc[0:YEARS],
            name='CHERRY', marker_color = COLOR), row=ROW, col=COL+1)
    
    COLOR = 'mediumorchid'
    fig.append_trace(go.Scatter(
            x=WEISS.index,
            y=WEISS[Type].iloc[0:YEARS],
            name='WEISS', line=dict(color=COLOR)), row=ROW, col=COL)
    fig.append_trace(go.Box(
            y=WEISS[Type].iloc[0:YEARS],
            name='WEISS', marker_color = COLOR), row=ROW, col=COL+1)
    
    COLOR = 'plum'
    fig.append_trace(go.Scatter(
            x=TAYLOR.index,
            y=TAYLOR[Type].iloc[0:YEARS],
            name='TAYLOR', line=dict(color=COLOR)), row=ROW, col=COL)
    fig.append_trace(go.Box(
            y=TAYLOR[Type].iloc[0:YEARS],
            name='TAYLOR', marker_color = COLOR), row=ROW, col=COL+1)
    
    # SHOW LEGOND
    fig.update_layout(height=500, width=500, title_text=str(column), showlegend=False)
    fig.update_xaxes(showline=True, linewidth=.5, linecolor='black', mirror=True)
    fig.update_yaxes(showline=True, linewidth=.5, linecolor='black', mirror=True)
    
    
    fig.update_layout(plot_bgcolor='rgba(0,0,0,0)')
    fig.update_xaxes(showgrid=True, tickformat= 'd')
    fig.update_yaxes(showgrid=True)
    fig.update_xaxes(showticklabels=True)
    fig.update_yaxes(showticklabels=True)
    fig.update_layout(
            margin=dict(l=1, r=1, t=50, b=20)
            )
    #fig.show() 
    
    return fig



def MAKE_TABLE():
    #df.iloc{row, column]
    
    SITE = str(site)
    print(SITE)
    fig = go.Figure(data=go.Table(header=dict(values=[DATA_2020_WHOLEREACH.columns[0],DATA_2020_WHOLEREACH.columns[1],DATA_2020_WHOLEREACH.columns[2],DATA_2020_WHOLEREACH.columns[3],DATA_2020_WHOLEREACH.columns[4],DATA_2020_WHOLEREACH.columns[5],DATA_2020_WHOLEREACH.columns[6],DATA_2020_WHOLEREACH.columns[7],DATA_2020_WHOLEREACH.columns[8],DATA_2020_WHOLEREACH.columns[9],DATA_2020_WHOLEREACH.columns[10],DATA_2020_WHOLEREACH.columns[11],DATA_2020_WHOLEREACH.columns[12],DATA_2020_WHOLEREACH.columns[13],DATA_2020_WHOLEREACH.columns[14],DATA_2020_WHOLEREACH.columns[15],DATA_2020_WHOLEREACH.columns[16],DATA_2020_WHOLEREACH.columns[17],DATA_2020_WHOLEREACH.columns[18],DATA_2020_WHOLEREACH.columns[19]]),
                              cells=dict(values=CHERRY.iloc[[0,1,2],0])))
    return fig

    


# Run T_Test on each column in dataframe
column = 'Avg Daily Q (cfs)'
T_TEST_FUNCTION()
MAKE_FIGURE().write_image("A", format='pdf')
column = 'Thal Len (m)'
T_TEST_FUNCTION()
MAKE_FIGURE().write_image("B", format='pdf')
column = 'Avg Thal Dep (cm)'
T_TEST_FUNCTION()
MAKE_FIGURE().write_image("C", format='pdf')
column = 'SE Thal Dep (cm)'
T_TEST_FUNCTION()
MAKE_FIGURE().write_image("D", format='pdf')
column = 'CV Thal Dep (%)'
T_TEST_FUNCTION()
MAKE_FIGURE().write_image("E", format='pdf')
column = 'Act Wid (cm)'
T_TEST_FUNCTION()
MAKE_FIGURE().write_image("F", format='pdf')
column = 'SE Act Wid'
T_TEST_FUNCTION()
MAKE_FIGURE().write_image("G", format='pdf')
column = 'Wet Wid (cm)'
T_TEST_FUNCTION()
MAKE_FIGURE().write_image("H", format='pdf')
column = 'SE Wet Wid'
T_TEST_FUNCTION()
MAKE_FIGURE().write_image("I", format='pdf')
column = 'Cnt of Pool'
T_TEST_FUNCTION()
MAKE_FIGURE().write_image("J", format='pdf')
column = 'Sum of Pool Len (m)'
T_TEST_FUNCTION()
MAKE_FIGURE().write_image("K", format='pdf')
column = 'Sum of Res Dep (cm)'
T_TEST_FUNCTION()
MAKE_FIGURE().write_image("L", format='pdf')
column = 'Avg of Res Dep (cm)'
T_TEST_FUNCTION()
MAKE_FIGURE().write_image("M", format='pdf')
column = 'Channel widths per pool'
T_TEST_FUNCTION()
MAKE_FIGURE().write_image("N", format='pdf')
#column = 'LW count'
#T_TEST_FUNCTION()
#MAKE_FIGURE()
#column = 'LW/100m'
#T_TEST_FUNCTION()
#MAKE_FIGURE()
column = 'Channel widths per LW'
T_TEST_FUNCTION()
MAKE_FIGURE().write_image("Q", format='pdf')

column = 'SILT'
T_TEST_FUNCTION()
MAKE_FIGURE().write_image("R", format='pdf')
column = 'SAND'
T_TEST_FUNCTION()
MAKE_FIGURE().write_image("S", format='pdf')
column = 'FINE_GRAVEL'
T_TEST_FUNCTION()
MAKE_FIGURE().write_image("T", format='pdf')
column = 'COURSE_GRAVEL'
T_TEST_FUNCTION()
MAKE_FIGURE().write_image("U", format='pdf')
column = 'COBBLE'
T_TEST_FUNCTION()
MAKE_FIGURE().write_image("V", format='pdf')
column = 'SMALL_BOULDER'
T_TEST_FUNCTION()
MAKE_FIGURE().write_image("W", format='pdf')
column = 'LARGE_BOULDER'
T_TEST_FUNCTION()
MAKE_FIGURE().write_image("X", format='pdf')
column = 'HARDPAN'
T_TEST_FUNCTION()
MAKE_FIGURE().write_image("Y", format='pdf')


column = 'High_Pulse_Count'
T_TEST_FUNCTION()
MAKE_FIGURE().write_image("HA", format='pdf')
column = 'Pulse_Range'
T_TEST_FUNCTION()
MAKE_FIGURE().write_image("HB", format='pdf')
column = 'TQ_Mean'
T_TEST_FUNCTION()
MAKE_FIGURE().write_image("HC", format='pdf')
column = 'R_B_Index'
T_TEST_FUNCTION()
MAKE_FIGURE().write_image("HD", format='pdf')

#The ABC was just for practice but then I had exactly 24 variables so I went with it
pdfs = ['A', 'B','C','D','E','F','G','H','I','J','K','L','M','N','Q','R','S','T','U','V','W','X','Y','HA', 'HB', 'HC','HD']



merger = PdfFileMerger()

for pdf in pdfs:
    merger.append(pdf)

merger.write("W:\STS\hydro\GAUGE\Temp\Ian's Temp\GRAPH")

merger.close()
