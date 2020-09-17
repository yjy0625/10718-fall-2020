import os
import re
import requests
import pandas as pd
from tqdm import tqdm

from us import states
from census import Census


def get_census_api():
    with open('.census_api_key', 'r') as f:
        api_key = f.read().replace('\n', '')
    return Census(api_key)


def get_counties_in_state(c, state):
    return c.acs5.state_county('NAME', states.lookup(state).fips, Census.ALL)


def get_vars_for_counties(c, counties, data_dir, prefix='default',
                          var_names=None, group_name=None, use_saved=True):
    saved_filename = os.path.join(data_dir, f'{prefix}.pkl')
    if use_saved and os.path.exists(saved_filename):
        df = pd.read_pickle(saved_filename)
    else:
        block_groups = []
        for county in tqdm(counties, desc=f'Getting counties by block group'):
            if group_name is not None:
                var_list = [f'group({group_name})']
            elif var_names is not None:
                var_list = list(var_names)
            var_list = ['NAME'] + var_list

            block_group = c.acs5.state_county_blockgroup(var_list,
                                                         county['state'],
                                                         county['county'],
                                                         Census.ALL)
            block_groups.extend(block_group)

        df = pd.DataFrame.from_records(block_groups)

        ## light processing to the dataframe {{{

        # add geometry id column
        df = df.rename(columns={ 'NAME': 'geography_description', 'block group': 'block_group' })
        df['geography_id'] = df['state'] + df['county'] + df['tract'] + df['block_group']
        df = df.set_index('geography_id')
        df = df[['geography_description', 'state', 'county', 'tract', 'block_group'] + var_list[1:]]
        num = df._get_numeric_data()
        num[num < 0] = -1

        ## }}}

        df.to_pickle(saved_filename)
    return df


def get_var_descriptions_and_types(var_list):
    var_description_url = 'https://api.census.gov/data/2018/acs/acs5/variables.json'
    var_descriptions = requests.get(var_description_url).json()['variables']

    def process_description(item):
        desc = item['label'].lower()
        desc = desc.replace('estimate!!', '')
        desc = desc.replace('!!', '_')
        desc = desc.replace(' ', '_')
        desc = re.sub('[ -]', '_', desc)
        desc = re.sub('[^0-9a-zA-Z_]+', '', desc)
        desc = re.sub('[_]+', '_', desc)
        desc = desc.replace('total', item['concept'].lower().replace(' ', '_'))
        return desc

    def process_types(t):
        if t == 'int':
            return 'int'
        elif t == 'float':
            return 'float4'
        else:
            raise ValueError(f'Cannot handle type {t}.')

    selected_descriptions = { k: process_description(var_descriptions[k]) for k in var_list }
    selected_data_types = { selected_descriptions[k]: process_types(var_descriptions[k]['predicateType']) for k in var_list }

    assert len(set(selected_descriptions.values())) == len(selected_descriptions)

    return selected_descriptions, selected_data_types


def get_column_data_types(selected_data_types):
    data_types = {}
    data_types.update({
        'geography_id': 'varchar(12) primary key',
        'geography_description': 'varchar(256)',
        'state': 'varchar(2)',
        'county': 'varchar(3)',
        'tract': 'varchar(6)',
        'block_group': 'varchar(1)'
    })

    data_types.update(selected_data_types)

    return data_types
