import os
import pandas as pd

from geopy.geocoders import Nominatim, GoogleV3
from sqlalchemy import create_engine


def get_coords(address, google_api_key):
    ''' given an address, geocode it either using OpenStreetMaps or Google API
        return (0, 0) if the address cannot be geocoded
    '''

    try: # first try OpenStreetMaps (free)
        locator = Nominatim(user_agent="http")
        location = locator.geocode(address)
    except: # try Google API
        try:
            googlelocator = GoogleV3(api_key=google_api_key)
            location = googlelocator.geocode(address) 
        except:
            return (0, 0)
    return (location.latitude, location.longitude) if location is not None else (0, 0)


def query_addresses(where_clause="where county_code='2' and program_key='27' and state='PA'"):
    ''' query for baseline addresses, can customize "where" clause however you'd like'''
    engine = create_engine("postgresql:///acdhs-housing")

    query = """
    select c.client_hash, address_line_1, address_line_2, city, state, zip from raw.clients c
    left join raw.snp_involvement_feed sif using(client_hash)
    left join raw.client_feed cf using(client_hash)
    {}
    group by c.client_hash, address_line_1, address_line_2 , city, state, zip, cf.race, c.gender_cd;
    """.format(where_clause)

    return pd.read_sql(query, engine)

def process_address(row):
    ''' given a row from ACDHS data, turn separate address cols into one address string
    '''
    line1 = (row.address_line_1 if row.address_line_1 is not None else '').lower()
    zip_ = int(float(row.zip)) if row.zip is not None else ''

    # OpenStreetMaps cannot geocode if these keywords are in the address, remove them
    for keyword in ('apt', 'unit', 'fl', 'ste', '#'):
        if keyword in line1:
            line1 = line1.split(keyword)[0]
    address = ', '.join([str(s) for s in (line1, row.city, row.state, zip_) if s is not None])
    
    return address.lower()


def geocoder_pipeline(api_key_filename, geoloc_addresses_filename):
    ''' top-to-bottom pipeline for getting data from db, geocoding, and saving it'''

    # get Google API key
    if os.path.exists(api_key_filename):
        with open(api_key_filename) as f:
            keys = [k.strip() for k in f.readlines()]
    else:
        keys = ['']

    # get address data from DB, prep for geocoding
    address_data = query_addresses()
    address_data['address'] = address_data.apply(process_address, axis=1)

    # check if any addresses have already been geocoded
    # TODO: connect to db instead of local file
    if os.path.exists(geoloc_addresses_filename):
        geoloc_addresses = pd.read_csv(geoloc_addresses_filename)
    else:
        geoloc_addresses = pd.DataFrame()
        
    # geocode and write to dataframe
    for address in address_data.address.unique():
        # if address already geocoded, don't redo the work
        if address in geoloc_addresses.address:
            row = geoloc_addresses[geoloc_addresses.address == address].iloc[0]
            lat = row.lat
            lon = row.lon
        else:
            lat, lon = get_coords(address, keys[0])

        address_data.loc[address_data.address == address, 'lat'] = lat
        address_data.loc[address_data.address == address, 'lon'] = lon

    # write to csv file
    # TODO: change this to writing back to DB
    crosswalk_cols = ['client_hash', 'address', 'lat', 'lon']
    client_address_crosswalk = pd.concat([geoloc_addresses, address_data[crosswalk_cols]])
    client_address_crosswalk.to_csv('../client_address_crosswalk.csv', index=False)

    
if __name__ == '__main__':
    api_key_filename = '../api_keys.txt'
    geoloc_addresses_filename = '../client_address_crosswalk.csv'
    geocoder_pipeline(api_key_filename, geoloc_addresses_filename)