import pandas as pd
from erddapy import ERDDAP
import re

# Let's define a tool that look through a server for datasets containing the listed standard variables
# Output is a table that list
# + server
# + dataset ID
# + distinct location (lat/long)
# + standard variables and their corresponding variable name

# Those following information could be added in the future to help filtering the datasets for each distinct location
# + Earliest time attached to location
# + Latest time attached to location
# + Shallowest depth
# + Deepest depth


def get_standard_variables_and_metadata(server_link, standard_variable_list):

    # Get access to the server and find datasets associated with standard_name variable listed
    e = ERDDAP(server=server_link,
               protocol='tabledap',
               response='csv')

    # Define Filter for which datasets to look into
    kw = {'standard_name': ','.join(standard_variable_list),
          'min_lon': -180.0, 'max_lon': 180.0,
          'min_lat': -90.0, 'max_lat': 90.0,
          'min_time': '', 'max_time': '',
          'cdm_data_type': ''}

    # Get available datasets from that server
    search_url = e.get_search_url(response='csv', **kw)
    datasets = pd.read_csv(search_url)

    # Print results
    print(e.server)
    print(str(len(datasets))+" datasets contains "+', '.join(standard_variable_list))

    # Loop through different data sets and create a metadata dataFrame
    df = pd.DataFrame(columns=['Dataset ID'])

    for index, row in datasets.iterrows():
        # Get Info from dataset (mostly min/max lat/long)
        print(row['Dataset ID'])
        info_url = e.get_info_url(dataset_id=row['Dataset ID'], response='csv')
        info = pd.read_csv(info_url)
        attribute_table = info.set_index(['Row Type','Variable Name','Attribute Name']).transpose()['attribute']

        # Try to get the distinct lat/long and time and depth range for that dataset, if it fails rely on the
        # ERDDAP metadata
        try:
            # If dataset is spread out geographically find distinct locations (may not work well for trajectory data)
            latlong_url = e.get_download_url(dataset_id=row['Dataset ID'],
                                             protocol='tabledap',
                                             variables=['latitude', 'longitude', 'time'])

            # Get add to the url commands to get distinct values and ordered with min and max time for each lat/long
            distinctMinMaxTime_url = latlong_url+'&distinct()&orderByMinMax(%22latitude%2Clongitude%2Ctime%22)'

            # Get lat/long and min/max depth for this dataset
            data = pd.read_csv(distinctMinMaxTime_url, header=[0, 1])

            # Group data by latitude/longitude and get min max values
            data_reduced = data.groupby(by=[('latitude', 'degrees_north'),
                                            ('longitude', 'degrees_east')]).agg(['min', 'max'])

            if info[(info['Variable Name'] == 'depth')].size > 0:
                latlongdepth_url = e.get_download_url(dataset_id=row['Dataset ID'],
                                                      protocol='tabledap',
                                                      variables=['latitude', 'longitude', 'depth'])

                # Get add to the url commands to get distinct values and ordered with min and max depth for
                # each lat/long
                distinctMinMaxDepth_url = latlongdepth_url + \
                                          '&distinct()&orderByMinMax(%22latitude%2Clongitude%2Cdepth%22)'

                # Get lat/long and min/max depth for this dataset
                data_depth = pd.read_csv(distinctMinMaxDepth_url, header=[0, 1])

                # Group depth data by lat/long and get min max values
                data_depth_reduced = data_depth.groupby(by=[('latitude', 'degrees_north'),
                                                            ('longitude', 'degrees_east')]
                                                        ).agg(['min', 'max'])

                # Merge depth values with time
                data_reduced = data_reduced.merge(data_depth_reduced, left_index=True, right_index=True)

            # Merge multi index column names
            data_reduced.columns = data_reduced.columns.map(' '.join).str.strip(' ')

        except Exception as exception_error:

            print('Failed to read: ' + str(exception_error))
            # If there's only one location, it could get the range from metadata

            # Find lat/long range of this dataset, if it's point we don't need to look into it
            min_latitude = float(attribute_table['NC_GLOBAL', 'geospatial_lat_min'].Value)
            max_latitude = float(attribute_table['NC_GLOBAL', 'geospatial_lat_max'].Value)
            min_longitude = float(attribute_table['NC_GLOBAL', 'geospatial_lon_min'].Value)
            max_longitude = float(attribute_table['NC_GLOBAL', 'geospatial_lon_max'].Value)

            # If min/max lat/long are the same don't go in the dataset
            if (min_latitude == max_latitude) & (min_longitude == max_longitude):
                data_reduced = pd.DataFrame(columns=['Dataset ID'])
                data_reduced['latitude degrees_north'] = min_latitude
                data_reduced['longitude degrees_east'] = min_longitude

                if attribute_table.filter('depth').size > 0 and 'actual_range' in attribute_table['depth'] and ('m' == attribute_table['depth','units']):

                        depth_range = attribute_table['depth', 'actual_range']['Value']
                        data_reduced['depth m min']
                        data_reduced['depth m max']


                data_reduced = data_reduced.set_index(['latitude degrees_north', 'longitude degrees_east'])

                print('Retrieved metadata')
            else:
                # Won't handle data with multiple location that it can't retrieve the data
                continue

        # Add Standard Name Variable Name to table info['Attribute Name'] == 'geospatial_lat_min'
        for var in standard_variable_list:
            data_reduced[var] = ','.join(e.get_var_by_attr(dataset_id=row['Dataset ID'],  standard_name=var))

        # Add cdm_data_type to table
        data_reduced['cdm_data_type'] = ','.join(info[info['Attribute Name'] == 'cdm_data_type']['Value'].values)

        # Add Dataset id to the table
        data_reduced['Dataset ID'] = row['Dataset ID']

        # Merge that dataset ID with previously downloaded data
        df = df.append(data_reduced)

    # Add server to dataframe
    df['server'] = e.server

    # Save resulting dataframe to a CSV, file name is based on the server address
    file_name = re.sub('https*://', '', e.server)
    file_name = re.sub("[\./]", '_', file_name)
    file_name = 'Server_List_'+file_name+'.csv'

    print('Save result to '+file_name)
    df.to_csv(file_name)

    return df

# ERDDAP Links from CIOOS Pacific
cioos_pacific_servers = ['https://catalogue.hakai.org/erddap',
                        'http://dap.onc.uvic.ca/erddap',
                        'https://data.cioospacific.ca/erddap']

standard_variable_list_to_look_for = ['sea_water_practical_salinity', 'sea_water_temperature']

# Get metadata for each server and datasets
for server in cioos_pacific_servers:
    get_standard_variables_and_metadata(server, standard_variable_list_to_look_for)