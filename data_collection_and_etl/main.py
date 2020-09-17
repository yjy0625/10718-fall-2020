import os
import yaml
import click
from attrdict import AttrDict
import pandas as pd

from acs_utils import get_census_api, get_counties_in_state, get_vars_for_counties
from acs_utils import get_var_descriptions_and_types, get_column_data_types
from db_utils import get_connection, insert_data_from_csv


@click.command()
@click.option('--config_file', default='config.yaml',
              help='Path to config file.')
def main(config_file):
    # get config
    with open(config_file) as f:
        config = AttrDict(yaml.load(f, Loader=yaml.FullLoader))

    # get census api
    census_api = get_census_api()

    # create directory for local data storage
    if not os.path.exists(config.data_dir):
        os.makedirs(config.data_dir)

    # get variable descriptions and process data types
    var_descriptions, var_types = get_var_descriptions_and_types(config.var_list)
    column_types = get_column_data_types(var_types)

    # get block groups by counties
    counties = get_counties_in_state(census_api, config.state)

    # get block group level query result in dataframe
    df = get_vars_for_counties(census_api, counties, config.data_dir,
                               var_names=config.var_list,
                               use_saved=config.use_saved_data)
    df = df.rename(columns=var_descriptions)

    # process dataframe to produce the right data types
    for i, (k, v) in enumerate(column_types.items()):
        if v == 'int':
            df[k] = df[k].astype(int)

    # save dataframe to a temporary csv file
    data_row_count = len(df)
    df.to_csv(config.csv_filename, index=True, sep='|')

    # create db connection and insert data from csv
    db_connection = get_connection(config.db['host'],
                                   config.db['user'],
                                   config.db['database'])
    inserted_row_count = insert_data_from_csv(db_connection,
                                              config.schema_name,
                                              config.table_name,
                                              column_types,
                                              csv_filename=config.csv_filename)
    if not inserted_row_count:
        print('Insert data failed.')

    assert inserted_row_count == data_row_count

    # remove temporary csv file
    os.remove(config.csv_filename)


if __name__ == '__main__':
    main()
