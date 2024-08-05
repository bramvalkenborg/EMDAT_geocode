import pandas as pd
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
import ast
import numpy as np
from tqdm import tqdm

# Input the EMDAT file (default: csv) and the path to save the file (default: csv) 
EMDAT_file = ''
save_path = ''


def apply_admin_geolocation_EMDAT(row):
    # Load the geolocator
    geolocator = Nominatim(user_agent='Disaster_locator')
    geolocator = RateLimiter(geolocator.geocode, min_delay_seconds=1)

    # Load the admin level
    admin_levels = row['Admin Units']

    # list of locations
    loc_list = list()
    if not (isinstance(admin_levels, float) and np.isnan(admin_levels)):
        for i_loc in range(len(admin_levels)):
            admin_level = admin_levels[i_loc]
            # Change the levels to an input for the geolocation
            # admin_levels = admin_levels[0]
            if 'adm2_name' in admin_level:
                admin_level['county'] = admin_level.pop('adm2_name')
                admin_level.pop('adm2_code')
            if 'adm1_name' in admin_level:
                admin_level['state'] = admin_level.pop('adm1_name')
                admin_level.pop('adm1_code')
            admin_level['country'] = row['Country']

            # Something to store the location
            location = {'lat': '', 'lon': ''}

            geocoded_location = geolocator(admin_level, exactly_one=True, language='en', addressdetails=True)

            # If there is no location detected, try without the admin levels
            if geocoded_location is None:
                # Extract values from the dictionary
                admin_level_list = list(admin_level.values())
                # Convert values to strings and join them
                admin_level_string = ', '.join(map(str, admin_level_list))
                geocoded_location = geolocator(admin_level_string)

            # Latitude and longitude
            try:
                location['lat'] = geocoded_location.latitude
                location['lon'] = geocoded_location.longitude
            except:
                location['lat'] = np.nan
                location['lon'] = np.nan
            loc_list.append(location)
    else:
        loc_list = {'lat': np.nan, 'lon': np.nan}

    return loc_list


def geolocate(df):
    """
    Retrieves the level of detail of a certain location by using zero-shot classification
    :param levels: (list) the different levels of a location
    :return:
    """
    tqdm.pandas(desc='Geolocate')

    locations = df.progress_apply(apply_admin_geolocation_EMDAT, axis=1)
    locations = locations.apply(pd.Series)
    locations.rename(columns={col: 'Location_' + str(col) for col in locations.columns}, inplace=True)
    return locations


# Import the data of EMDAT
EMDAT = pd.read_csv(EMDAT_file)

# Admin Units column is read as a string. Convert is into list of dictionaries
EMDAT['Admin Units'] = EMDAT['Admin Units'].apply(lambda x: ast.literal_eval(x) if pd.notna(x) else np.nan)

locations = geolocate(EMDAT)
EMDAT_locations = pd.merge(EMDAT, locations, left_index=True, right_index=True)

# Save the file
EMDAT_locations.to_csv(save_path)
