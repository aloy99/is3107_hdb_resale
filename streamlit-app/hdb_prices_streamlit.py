import streamlit as st
import pandas as pd
from utils_functions import find_postal, find_nearest, dist_from_location, map
import joblib

st.set_page_config(page_title="HDB Resale Prices", page_icon="🏙️", layout="wide")


st.title('IS3107: Visualisation and Prediction of HDB Resale Prices')

with st.sidebar.form('Input HDB Features'):
    st.title("Input HDB Details")
    flat_address = st.text_input("Address or Postal Code", '426 Ang Mo Kio Ave 3')
    town = st.selectbox('Town', list(['ANG MO KIO', 'BEDOK', 'BISHAN', 'BUKIT BATOK', 'BUKIT MERAH',
                                            'BUKIT TIMAH', 'CENTRAL AREA', 'CHOA CHU KANG', 'CLEMENTI',
                                            'GEYLANG', 'HOUGANG', 'JURONG EAST', 'JURONG WEST',
                                            'KALLANG/WHAMPOA', 'MARINE PARADE', 'QUEENSTOWN', 'SENGKANG',
                                            'SERANGOON', 'TAMPINES', 'TOA PAYOH', 'WOODLANDS', 'YISHUN',
                                            'LIM CHU KANG', 'SEMBAWANG', 'BUKIT PANJANG', 'PASIR RIS','PUNGGOL']),
                                index=0)
    flat_model = st.selectbox('Model', list(['Standard', 'Improved', 'Premium Apartment', 'Model A',
                                                            'New Generation', 'Maisonette', 'Apartment', 'Simplified',
                                                            'Model A2', 'DBSS', 'Terrace', 'Adjoined flat', 'Multi Generation',
                                                            '2-room', 'Executive Maisonette', 'Type S1S2']), index=0)
    flat_type = st.selectbox('Type', list(['2 ROOM', '3 ROOM', '4 ROOM', '5 ROOM', 'EXECUTIVE']),
                                    index=3)
    storey = st.selectbox('Level', list(['01 TO 03','04 TO 06','07 TO 09','10 TO 12','13 TO 15',
                                                '16 TO 18','19 TO 21','22 TO 24','25 TO 27','28 TO 30',
                                                '31 TO 33','34 TO 36','37 TO 39','40 TO 42','43 TO 45',
                                                '46 TO 48','49 TO 51']), index=1)
    lease_commence_date = st.selectbox('Lease Commencement Date', list(reversed(range(1966, 2050))), index=0)
    floor_area = st.slider("Floor Area (sqm)", 34,280,135) # floor area

    submitted1 = st.form_submit_button(label = 'See Results &nbsp; 🔎', type='primary', use_container_width=True)

@st.cache_resource() 
def load_model():
    model_save_path = "models/rf_compressed.pkl"
    rfr = joblib.load(model_save_path)
    return rfr

rfr = load_model()

## Get flat coordinates
coord = find_postal(flat_address)
try:
    flat_coord = pd.DataFrame({'address':[coord.get('results')[0].get('ADDRESS')],
                            'LATITUDE':[coord.get('results')[0].get('LATITUDE')], 
                            'LONGITUDE':[coord.get('results')[0].get('LONGITUDE')]})
except IndexError:
    st.error('Please enter a valid address!')
    pass
    
## Load amenities coordinates
@st.cache_data()
def load_data(filepath):
    return pd.read_csv(filepath)

supermarket_coord = load_data('data/supermarket_coordinates.csv')
school_coord = load_data('data/school_coordinates.csv')
hawker_coord = load_data('data/hawker_coordinates.csv')
shop_coord = load_data('data/shoppingmall_coordinates.csv')
park_coord = load_data('data/parks_coordinates.csv')
mrt_coord = load_data('data/MRT_coordinates.csv')[['STN_NAME','Latitude','Longitude']]

## Get nearest and number of amenities in 2km radius
# Supermarkets
nearest_supermarket,supermarkets_2km = find_nearest(flat_coord, supermarket_coord)
flat_supermarket = pd.DataFrame.from_dict(nearest_supermarket).T
flat_supermarket = flat_supermarket.rename(columns={0: 'flat', 1: 'supermarket', 2: 'supermarket_dist',
                                                    3: 'num_supermarket_2km'}).reset_index().drop(['index'], axis=1)
supermarkets_2km['type'] = ['Supermarket']*len(supermarkets_2km)

# Primary Schools
nearest_school,schools_2km = find_nearest(flat_coord, school_coord)
flat_school = pd.DataFrame.from_dict(nearest_school).T
flat_school = flat_school.rename(columns={0: 'flat', 1: 'school', 2: 'school_dist',
                                          3: 'num_school_2km'}).reset_index().drop('index', axis=1)
schools_2km['type'] = ['School']*len(schools_2km)

# Hawker Centers
nearest_hawker,hawkers_2km = find_nearest(flat_coord, hawker_coord)
flat_hawker = pd.DataFrame.from_dict(nearest_hawker).T
flat_hawker = flat_hawker.rename(columns={0: 'flat', 1: 'hawker', 2: 'hawker_dist',
                                          3: 'num_hawker_2km'}).reset_index().drop('index', axis=1)
hawkers_2km['type'] = ['Hawker']*len(hawkers_2km)

# Shopping Malls
nearest_mall,malls_2km = find_nearest(flat_coord, shop_coord)
flat_mall = pd.DataFrame.from_dict(nearest_mall).T
flat_mall = flat_mall.rename(columns={0: 'flat', 1: 'mall', 2: 'mall_dist',
                                      3: 'num_mall_2km'}).reset_index().drop('index', axis=1)
malls_2km['type'] = ['Mall']*len(malls_2km)

# Parks
nearest_park,parks_2km = find_nearest(flat_coord, park_coord)
flat_park = pd.DataFrame.from_dict(nearest_park).T
flat_park = flat_park.rename(columns={0: 'flat', 1: 'park', 2: 'park_dist',
                                      3: 'num_park_2km'}).reset_index().drop(['index','park'], axis=1)
parks_2km['type'] = ['Park']*len(parks_2km)
parks_2km['name'] = ['Park']*len(parks_2km)

# MRT
nearest_mrt,mrt_2km = find_nearest(flat_coord, mrt_coord)
flat_mrt = pd.DataFrame.from_dict(nearest_mrt).T
flat_mrt = flat_mrt.rename(columns={0: 'flat', 1: 'mrt', 2: 'mrt_dist',
                                    3: 'num_mrt_2km'}).reset_index().drop('index', axis=1)
mrt_2km['type'] = ['MRT']*len(mrt_2km)

amenities = pd.concat([supermarkets_2km, schools_2km, hawkers_2km, malls_2km, parks_2km, mrt_2km])
amenities = amenities.rename(columns={'lat':'LATITUDE', 'lon':'LONGITUDE'})

# Distance from Dhoby Ghaut
dist_dhoby = dist_from_location(flat_coord, (1.299308, 103.845285))
flat_coord['dist_dhoby'] = [list(dist_dhoby.values())[0][1]]

## Concat all dataframes
flat_coord = pd.concat([flat_coord, flat_supermarket.drop(['flat'], axis=1), 
                        flat_school.drop(['flat'], axis=1),
                        flat_hawker.drop(['flat'], axis=1),
                        flat_mall.drop(['flat'], axis=1),
                        flat_park.drop(['flat'], axis=1),
                        flat_mrt.drop(['flat'], axis=1)],
                       axis=1)

# Flat Type
replace_values = {'2 ROOM': 0, '3 ROOM': 1, '4 ROOM': 2, '5 ROOM': 3, 'EXECUTIVE': 4}
flat_coord['flat_type'] = replace_values.get(flat_type)

# Storey
flat_coord['storey_range'] = list(['01 TO 03','04 TO 06','07 TO 09','10 TO 12','13 TO 15',
                                              '16 TO 18','19 TO 21','22 TO 24','25 TO 27','28 TO 30',
                                              '31 TO 33','34 TO 36','37 TO 39','40 TO 42','43 TO 45',
                                              '46 TO 48','49 TO 51']).index(storey)

# Floor Area
flat_coord['floor_area_sqm'] = floor_area

# Lease commence date
flat_coord['lease_commence_date'] = lease_commence_date

# Region
d_region = {'ANG MO KIO':'North East', 'BEDOK':'East', 'BISHAN':'Central', 'BUKIT BATOK':'West', 'BUKIT MERAH':'Central',
       'BUKIT PANJANG':'West', 'BUKIT TIMAH':'Central', 'CENTRAL AREA':'Central', 'CHOA CHU KANG':'West',
       'CLEMENTI':'West', 'GEYLANG':'Central', 'HOUGANG':'North East', 'JURONG EAST':'West', 'JURONG WEST':'West',
       'KALLANG/WHAMPOA':'Central', 'MARINE PARADE':'Central', 'PASIR RIS':'East', 'PUNGGOL':'North East',
       'QUEENSTOWN':'Central', 'SEMBAWANG':'North', 'SENGKANG':'North East', 'SERANGOON':'North East', 'TAMPINES':'East',
       'TOA PAYOH':'Central', 'WOODLANDS':'North', 'YISHUN':'North'}
region_dummy = {'region_East':[0], 'region_North':[0], 'region_North East':[0], 'region_West':[0]}
region = d_region.get(town)
if region == 'East': region_dummy['region_East'][0] += 1
elif region == 'North': region_dummy['region_North'][0] += 1
elif region == 'North East': region_dummy['region_North East'][0] += 1
elif region == 'West': region_dummy['region_West'][0] += 1
#region_dummy
flat_coord = pd.concat([flat_coord, pd.DataFrame.from_dict(region_dummy)], axis=1)

# Flat Model
replace_values = {'Model A':'model_Model A', 'Simplified':'model_Model A', 'Model A2':'model_Model A', 
                  'Standard':'Standard', 'Improved':'Standard', '2-room':'Standard',
                  'New Generation':'model_New Generation',
                  'Apartment':'model_Apartment', 'Premium Apartment':'model_Apartment',
                  'Maisonette':'model_Maisonette', 'Executive Maisonette':'model_Maisonette', 
                  'Special':'model_Special', 'Terrace':'model_Special', 'Adjoined flat':'model_Special', 
                    'Type S1S2':'model_Special', 'DBSS':'model_Special'}
d = {'model_Apartment':[0], 'model_Maisonette':[0], 'model_Model A':[0], 'model_New Generation':[0], 'model_Special':[0]}
if replace_values.get(flat_model) != 'Standard': d[replace_values.get(flat_model)][0] += 1

df = pd.DataFrame.from_dict(d)
flat_coord = pd.concat([flat_coord, pd.DataFrame.from_dict(d)], axis=1)
flat_coord['selected_flat'] = [1] # for height of building

flat1 = flat_coord[['flat_type', 'storey_range', 'floor_area_sqm', 'lease_commence_date',
       'school_dist', 'num_school_2km', 'hawker_dist', 'num_hawker_2km',
       'park_dist', 'num_park_2km', 'mall_dist', 'num_mall_2km', 'mrt_dist',
       'num_mrt_2km', 'supermarket_dist', 'num_supermarket_2km', 'dist_dhoby',
       'region_East', 'region_North', 'region_North East', 'region_West',
       'model_Apartment', 'model_Maisonette', 'model_Model A',
       'model_New Generation', 'model_Special']]

flats = pd.read_csv('data/flat_coordinates.csv')[['LATITUDE','LONGITUDE','address']]
flats['selected_flat'] = [0.000001]*len(flats)
flats = flats.append(flat_coord[['LATITUDE', 'LONGITUDE', 'selected_flat', 'address']], ignore_index=True)
flats[['LATITUDE', 'LONGITUDE', 'selected_flat']] = flats[['LATITUDE', 'LONGITUDE', 'selected_flat']].astype(float)
flats['type'] = ['HDB']*len(flats)
flats = flats.rename(columns={'address':'name'})
all_buildings = pd.concat([amenities,flats])

# How to use
          
st.write("Input the features of your HDB flat of interest in the left panel and click \"See Results\" to obtain the projected resale price.")

def legend_checkbox_item(hex_color, text):
    c1, c2 = st.columns([2, 1])
    with c1:
        is_checked = st.checkbox(text, True)
    with c2: # colored circle
        st.markdown(f"""<div style="width: 20px; height: 20px; border-radius: 50%; background-color: {hex_color};"></div>""", True)
    return is_checked

st.divider()

# Prediction
predict1 = rfr.predict(flat1)[0]
st.title('Predicted HDB Resale Price: **SGD$%s**' % ("{:,}".format(int(predict1))))
flat1.to_csv('tmp_csv.csv',index=False)

# Amenities
st.title('Amenities Nearby (2km radius)')

with st.container():
    st.subheader(f"{(flat_coord.iloc[0]['num_mrt_2km'])} MRT Stations")
    st.write('Nearest MRT: **%s** (%0.2fkm)' % (flat_coord.iloc[0]['mrt'], flat_coord.iloc[0]['mrt_dist']))
with st.container():
    st.subheader(f"{(flat_coord.iloc[0]['num_mall_2km'])} Shopping Malls")
    st.write('Nearest Mall: **%s** (%0.2fkm)' % (flat_coord.iloc[0]['mall'], flat_coord.iloc[0]['mall_dist']))
with st.container():
    st.subheader(f"{(flat_coord.iloc[0]['num_school_2km'])} Primary Schools")
    st.write('Nearest School: **%s** (%0.2fkm)' % (flat_coord.iloc[0]['school'], flat_coord.iloc[0]['school_dist']))
with st.container():
    st.subheader(f"{(flat_coord.iloc[0]['num_park_2km'])} Parks")
    st.write('Nearest Park: %0.2fkm' % (flat_coord.iloc[0]['park_dist']))
with st.container():
    st.subheader(f"{(flat_coord.iloc[0]['num_hawker_2km'])} Hawker Centers")
    st.write('Nearest Hawker Center: **%s** (%0.2fkm)' % (flat_coord.iloc[0]['hawker'], flat_coord.iloc[0]['hawker_dist']))
with st.container():
    st.subheader(f"{(flat_coord.iloc[0]['num_supermarket_2km'])} Supermarkets")
    st.write('Nearest Supermarket: **%s** (%0.2fkm)' % (flat_coord.iloc[0]['supermarket'], flat_coord.iloc[0]['supermarket_dist']))

st.divider()

# Map Display 

row1_1, row1_2, row1_3, row1_4 = st.columns(4)
with row1_1:
    show_mrt = st.checkbox("MRT Station &nbsp; :red_circle:", True)
with row1_2:
    show_malls = st.checkbox("Shopping Mall &nbsp; :large_orange_circle:", True)
with row1_3:  
    show_schools = st.checkbox("Primary School &nbsp; :large_blue_circle:", True)

row2_1, row2_2, row2_3, row2_4 = st.columns(4)
with row2_1:
    show_parks = st.checkbox("Park &nbsp; :large_green_circle:", True)
with row2_2:
    show_hawkers = st.checkbox("Hawker Center &nbsp; :large_purple_circle:", True)
with row2_3:
    show_supermarkets = st.checkbox("Supermarket &nbsp; :large_brown_circle:", True)
with row2_4:
    hide_hdb = st.checkbox('Hide HDBs &nbsp; :large_yellow_circle:',False)  
    
amenities_toggle = [show_mrt,show_malls,show_schools,show_parks,show_hawkers,show_supermarkets,hide_hdb]
map(all_buildings, float(flat_coord.iloc[0]['LATITUDE']), float(flat_coord.iloc[0]['LONGITUDE']),
    13.5, amenities_toggle)