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
'''
table = "Survey_events"
columns = "*"
where_column = "Reach"
where_value = "Lower"
access_keys = pd.read_sql_query(f'select {columns} from {table} WHERE {where_column} = {where_value};',conn)
print(access_keys)
'''

# df = query_access("Survey_ID, Watershed, Reach, Survey_date", "Survey_events")
# df["Survey_date"] = pd.to_datetime(df["Survey_date"]).dt.year
# print(df)

def access_keys(year, watershed, reach):
    '''accepts a year value, or all for all years'''
    '''accepts a watershed value or all for all watersheds'''
    '''accepts a reach value or all for all reaches'''
    '''returns a list of Survey_ID for Survey_BK index column'''

    columns = "Survey_ID, Watershed, Reach, THalweg_length, Survey_date"
    table = "Survey_events"
    # run basic query
    access_keys = query_access(columns, table)
    # convert to datetime
    access_keys["Survey_date"] = pd.to_datetime(access_keys["Survey_date"]).dt.year

    # filter by year
    if year != "all":
        access_keys = access_keys.loc[access_keys['Survey_date'] == year]
    # filter by watershed
    if watershed != "all":
        access_keys = access_keys.loc[access_keys['Watershed'] == watershed]
    # filter by reach
    if reach != "all":
        access_keys = access_keys.loc[access_keys['Reach'] == reach]
    surveys = access_keys.copy()
    access_keys = list(access_keys["Survey_ID"])
    return access_keys, surveys

def waterhseds_list(year, watershed):
    columns = "Survey_ID, Watershed, Reach, Survey_date"
    table = "Survey_events"
    filtered_watersheds = query_access(columns, table)
    # rename Survey_ID column
    filtered_watersheds = filtered_watersheds.rename(columns={"Survey_ID": "Survey_FK"})
    # change survey date
    filtered_watersheds["Survey_date"] = pd.to_datetime(filtered_watersheds["Survey_date"]).dt.year
    # filter Surveys
    if year != "all":
        filtered_watersheds = filtered_watersheds.loc[filtered_watersheds['Survey_date'] == year]
    if watershed != "all":
        filtered_watersheds = filtered_watersheds.loc[filtered_watersheds['Watershed'] == watershed]
    return filtered_watersheds

def access_pools(year, watershed, reach):

    # add watersheds to filtered access keys
    # get filtered watersheds
   
    filtered_watersheds = waterhseds_list(year, watershed)

    columns = "Survey_FK, Downstream_location, Upstream_location, Tail_depth, Max_depth"
    table = "Pools"
    reach = "all"

    # get access keys (Survey_FK)
    keys, surveys = access_keys(year, watershed, reach)

    #### pools ###
    # get all pools
    pools = query_access(columns, table)
    # filter pools by keys
    pools = pools[pools['Survey_FK'].isin(keys)]

    pools = pools.merge(filtered_watersheds, on = "Survey_FK", how = "left")
    pools = pools.sort_values(by='Survey_FK', ascending=True)

    # Cnt of Pools
    count_of_pools = pools.groupby(by="Watershed")["Survey_FK"].count().to_list()
 
    # Sum of Pool Len (m)
    pools['Pool Len (m)'] = pools["Upstream_location"] - pools["Downstream_location"]
    pool_length = pools.groupby(by="Watershed")["Pool Len (m)"].sum().to_list()
    # Sum of Res Dep (cm)
    pools['Res Dep (cm)'] = pools["Max_depth"] - pools["Tail_depth"]
    sum_pool_depth = pools.groupby(by="Watershed")['Res Dep (cm)'].sum().to_list()
    # Avg of Res Dep (cm)
    average_pool_depth = pools.groupby(by="Watershed")['Res Dep (cm)'].mean().to_list()
   
    # watershed names
    watershed_names = pools.drop_duplicates(subset=['Watershed'])
    watershed_names = watershed_names["Watershed"].to_list()

    # make pools df, it may eventually be easier to return lists instead of df
    pools_survey = pd.DataFrame(data={
        'Watershed': watershed_names, 
        'Cnt of Pools': count_of_pools,
        'Sum of Pool Len (m)': pool_length,
        'Sum of Res Dep (cm)': sum_pool_depth,
        'Avg of Res Dep (cm)': average_pool_depth,
        
        })
    pools_survey['Channel widths per pool'] = ""
    # Survey_FK
    # Downstream_location
    # Upstream_location
    # Tail_depth
    # Max_depth
    return pools_survey

def access_thalwegs(year, watershed, reach):
    # add watersheds to filtered access keys
    # get filtered watersheds
   
    filtered_watersheds = waterhseds_list(year, watershed)

    # get access keys (Survey_FK)
    keys, surveys = access_keys(year, watershed, reach)


    #### thalwegs ###
    # get all thalwegs
    # columns = "Survey_FK, Downstream_location, Upstream_location, Tail_depth, Max_depth"
    columns = "*"
    table = "Thalweg_depths"
    thalwegs = query_access(columns, table)

    # filter pools by keys
    thalwegs = thalwegs[thalwegs['Survey_FK'].isin(keys)]

    # merge watershed info with thalwegs
    thalwegs = thalwegs.merge(filtered_watersheds, on = "Survey_FK", how = "left")
    thalwegs = thalwegs.sort_values(by='Survey_FK', ascending=True)

    # Avg Thal Dep (cm)
    average_thalweg_depth = thalwegs.groupby(by="Watershed")['Depth'].mean().to_list()

    # SE Thal Dep (cm)
    # STDEV(B6:B250)/SQRT(COUNT(B6:B250)-1)
    stdev_thalweg_depth = thalwegs.groupby(by="Watershed")['Depth'].std().to_list()
    count_thalweg_depth = thalwegs.groupby(by="Watershed")['Depth'].count().to_list()
    
    # watershed names
    watershed_names = thalwegs.drop_duplicates(subset=['Watershed'])
    watershed_names = watershed_names["Watershed"].to_list()
    # SE Thal Dep (cm)
    se_thalweg_depth = pd.DataFrame(data={
        'Watershed': watershed_names, 
        'stdev_thalweg_depth': stdev_thalweg_depth,
        'count_thalweg_depth': count_thalweg_depth,
        })
    se_thalweg_depth['se_thalweg_depth'] = se_thalweg_depth['stdev_thalweg_depth']/(se_thalweg_depth['count_thalweg_depth']**(1/2))
    se_thalweg_depth = se_thalweg_depth['se_thalweg_depth'].tolist()
    
    # CV Thal Dep (%)
    #cv_depth = se_thalweg_depth['stdev_thalweg_depth']/(average_thalweg_depth)
    cv_thalweg_depth = pd.DataFrame(data={
        'Watershed': watershed_names, 
        'stdev_thalweg_depth': stdev_thalweg_depth,
        'average_thalweg_depth': average_thalweg_depth,
        })
    cv_thalweg_depth['cv_thalweg_depth'] = cv_thalweg_depth['stdev_thalweg_depth']/cv_thalweg_depth['average_thalweg_depth']
    cv_thalweg_depth = cv_thalweg_depth["cv_thalweg_depth"].to_list()
  
    thalwegs = pd.DataFrame(data={
        'Watershed': watershed_names, 
        'average_thalweg_depth': average_thalweg_depth,
        'se_thalweg_depth': se_thalweg_depth,
        'cv_thalweg_depth': cv_thalweg_depth,
        })
    
    return thalwegs

def access_transects(year, watershed, reach):
    # add watersheds to filtered access keys
    # get filtered watersheds
   
    filtered_watersheds = waterhseds_list(year, watershed)

    # get access keys (Survey_FK)
    keys, surveys = access_keys(year, watershed, reach)


    #### thalwegs ###
    # get all thalwegs
    # columns = "Survey_FK, Downstream_location, Upstream_location, Tail_depth, Max_depth"
    columns = "*"
    table = "Transects"
    transects = query_access(columns, table)

    
    
    # filter transects by keys
    transects = transects[transects['Survey_FK'].isin(keys)]
    
    # merge watershed info with thalwegs
    transects = transects.merge(filtered_watersheds, on = "Survey_FK", how = "left")
    transects = transects.sort_values(by='Survey_FK', ascending=True)

    # filter by main channel...this may change
    transects = transects.loc[transects['Channel_type'] == "Main channel"]

    # Act Wid (cm)
    active_width = transects.groupby(by="Watershed")['Active_width'].mean().to_list()
    # wetted width
    wetted_width = transects.groupby(by="Watershed")['Wetted_width'].mean().to_list()

    # SE Wet Wid
    # STDEV
    stdev_wetted_width = transects.groupby(by="Watershed")['Wetted_width'].std().to_list()
    count_wetted_width = transects.groupby(by="Watershed")['Wetted_width'].count().to_list()

    stdev_active_width = transects.groupby(by="Watershed")['Active_width'].std().to_list()
    count_active_width = transects.groupby(by="Watershed")['Active_width'].count().to_list()

    # watershed names
    watershed_names = transects.drop_duplicates(subset=['Watershed'])
    watershed_names = watershed_names["Watershed"].to_list()

    # active width
    se_active_width = pd.DataFrame(data={
        'Watershed': watershed_names, 
        'stdev_active_width': stdev_active_width,
        'count_active_width': count_active_width,
        })
    se_active_width['se_active_width'] = se_active_width['stdev_active_width']/(se_active_width['count_active_width']**(1/2))
    se_active_width = se_active_width['se_active_width'].tolist()

    # wetted width
    se_wetted_width = pd.DataFrame(data={
        'Watershed': watershed_names, 
        'stdev_wetted_width': stdev_wetted_width,
        'count_wetted_width': count_wetted_width,
        })
    se_wetted_width['se_wetted_width'] = se_wetted_width['stdev_wetted_width']/(se_wetted_width['count_wetted_width']**(1/2))
    se_wetted_width = se_wetted_width['se_wetted_width'].tolist()

    transects = pd.DataFrame(data={
        'Watershed': watershed_names, 
        'Act Wid (cm)': active_width,
        'SE Act Wid': se_active_width,
        'Wet Wid (cm)': wetted_width,
        'SE Wet Wid': se_wetted_width,
        })

    return transects

def access_wood(year, watershed, reach):
    # add watersheds to filtered access keys
    # get filtered watersheds
   
    filtered_watersheds = waterhseds_list(year, watershed)

    # get access keys (Survey_FK)
    keys, surveys = access_keys(year, watershed, reach)


    #### large wood ###
    # get all wood
    # columns = "Survey_FK, Downstream_location, Upstream_location, Tail_depth, Max_depth"
    columns = "*"
    table = "Wood_data"
    wood = query_access(columns, table)
    
    # filter wood by keys
    wood = wood[wood['Survey_FK'].isin(keys)]
    
    # merge watershed info with thalwegs
    wood = wood.merge(filtered_watersheds, on = "Survey_FK", how = "left")
    wood = wood.sort_values(by='Survey_FK', ascending=True)

    # filter by main channel...this may change
    # wood = wood.loc[wood['Channel_type'] == "Main channel"]

    # lare wood count - takes the sum of large wood counts per wood class
    count_wood = wood.groupby(by="Watershed")['Count'].sum().to_list()
    # watershed names
    watershed_names = wood.drop_duplicates(subset=['Watershed'])
    watershed_names = watershed_names["Watershed"].to_list()

    wood = pd.DataFrame(data={
        'Watershed': watershed_names, 
        'LW count': count_wood,
        })
    wood['LW/100m'] = wood['LW count']/100

    return wood

def access_substrate(year, watershed, reach):

     # add watersheds to filtered access keys
    # get filtered watersheds
   
    filtered_watersheds = waterhseds_list(year, watershed)

    # get access keys (Survey_FK)
    keys, surveys = access_keys(year, watershed, reach)


    #### substrate ###
    # get all substrate
    # columns = "Survey_FK, Downstream_location, Upstream_location, Tail_depth, Max_depth"
    columns = "*"
    table = "Transects"
    substrate = query_access(columns, table)
    
    # filter substrate by keys
    substrate = substrate[substrate['Survey_FK'].isin(keys)]
    
    # merge watershed info with thalwegs
    substrate = substrate.merge(filtered_watersheds, on = "Survey_FK", how = "left")
    substrate = substrate.sort_values(by='Survey_FK', ascending=True)

    substrate_melt = pd.melt(substrate, id_vars=['Watershed'], value_vars=['Substrate_left', 'Substrate_lcenter', 'Substrate_center', 'Substrate_rcenter','Substrate_right'])
    print("melt")
    print(substrate_melt)
    
    df = pd.pivot_table(substrate_melt, values='value', index=['Watershed', 'value'], aggfunc='count')
    df.reset_index(inplace=True)
    fines = df.loc[df['value'] == 'Fines'].rename(columns={"variable": "Fines"})
    #fines = fines["Fines"].to_list()
    sand = df.loc[df['value'] == 'Sand'].rename(columns={"variable": "Sand"})
    gravel_fine = df.loc[df['value'] == 'Gravel fine'].rename(columns={"variable": "Gravel fine"})

    gravel_coarse = df.loc[df['value'] == 'Gravel coarse'].rename(columns={"variable": "Gravel coarse"})
    cobble = df.loc[df['value'] == 'Cobble'].rename(columns={"variable": "Cobble"})
    boulder_small = df.loc[df['value'] == 'Boulder small'].rename(columns={"variable": "Boulder small"})
    boulder_large= df.loc[df['value'] == 'Boulder large'].rename(columns={"variable": "Boulder large"})
    hard_pan = df.loc[df['value'] == 'Hard pan'].rename(columns={"variable": "Hard pan"})
    watershed_names = substrate.drop_duplicates(subset=['Watershed'])
    watershed_names = watershed_names["Watershed"].to_list()
    substrate_count = pd.DataFrame(data={
        'Watershed': watershed_names,
         })

    substrate_count = substrate_count.merge(fines,how='left', on="Watershed")


    substrate_count = substrate_count.drop(columns=['value'])
    substrate_count = substrate_count.merge(sand, how='left', on="Watershed")
    substrate_count = substrate_count.drop(columns=['value'])
    
    substrate_count = substrate_count.merge(gravel_fine, how='left', on="Watershed")
    substrate_count = substrate_count.drop(columns=['value'])
    
    substrate_count = substrate_count.merge(gravel_coarse, how='left', on="Watershed")
    substrate_count = substrate_count.drop(columns=['value'])
    
    substrate_count = substrate_count.merge(cobble, how='left', on="Watershed")
    substrate_count = substrate_count.drop(columns=['value'])
    
    substrate_count = substrate_count.merge(boulder_small, how='left', on="Watershed")
    substrate_count = substrate_count.drop(columns=['value'])

    substrate_count = substrate_count.merge(boulder_large, how='left', on="Watershed")
    substrate_count = substrate_count.drop(columns=['value'])
   
    substrate_count = substrate_count.merge(hard_pan, how='left', on="Watershed")
    substrate_count = substrate_count.drop(columns=['value'])

    substrate_count = substrate_count.fillna(0)
    substrate_count[["Fines", "Sand", "Gravel fine", "Gravel coarse", "Cobble", "Boulder small", "Boulder large", "Hard pan"]] = substrate_count[["Fines", "Sand", "Gravel fine", "Gravel coarse", "Cobble", "Boulder small", "Boulder large", "Hard pan"]].astype("int")


    return substrate_count

def all_data(year):

#year = 2018
    watershed = "all"
    reach = "all"
    pools = access_pools(year, watershed, reach)
    thalwegs = access_thalwegs(year, watershed, reach)
    transects = access_transects(year, watershed, reach)
    wood = access_wood(year, watershed, reach)
    substrate = access_substrate(year, watershed, reach)
    
    df = transects.merge(thalwegs, on = "Watershed")
    df = df.merge(pools, on = "Watershed")
    df = df.merge(wood, on = "Watershed")
    df = df.merge(transects, on = "Watershed")
    df = df.merge(substrate, on = "Watershed")

    print(df)
    
    # rename columns
    # df = df.rename(columns = {'Watershed':'SITE'}, inplace = True)
    df.rename(columns={"Watershed": "SITE"}, inplace=True)

    df['SITE'] = df['SITE'].replace({'South Seidel Creek':'SSC'})
    df['SITE'] = df['SITE'].replace({'North Seidel Creek':'NSC'})
    df['SITE'] = df['SITE'].replace({'Cherry Tributary':'CYC'})
    df['SITE'] = df['SITE'].replace({'Fisher Creek':'FRC'})
    df['SITE'] = df['SITE'].replace({'Judd Creek':'JDC'})
    df['SITE'] = df['SITE'].replace({'Tahlequah Creek':'THC'})
    df['SITE'] = df['SITE'].replace({'Weiss Creek':'WSC'})
    df['SITE'] = df['SITE'].replace({'Taylor Creek':'TYC'})
    df['SITE'] = df['SITE'].replace({'Webster Creek':'WRC'})

    return df

all_data(2018)