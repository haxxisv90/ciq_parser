# Documentation:
# The purpose of this script is to parse raw CIQ data for dark and lit sites. It will
# generate a data model for input into an Ansible playbook. Ansible will use these network inputs
# to create the necessary network on Infoblox. Dedicated to YA.


import pandas as pd
from os import path
from json import dumps
from time import time


# Parser Setup
ciq_name = 'ciq_network_engineering.xlsx'
ciq_path = path.abspath(ciq_name)


# Call this function separate output on console (Not needed for scrip functionality)
def page_break(title, width):
    print('\n' + '{}|{}|{}'.format('-'*int(width), title.upper(), '-'*int(width)))


# Call this function to print full row of dataframe (Not needed for script functionality)
def df_to_console(df, lines_to_print):
    desired_width = 320
    with pd.option_context('display.max_columns', None, 'display.width', desired_width):
        print(df.head(int(lines_to_print)))


def ciq_to_dataframe(ciq):

    sheets_to_read = ['LIT SITE IP ADDRESSING', 'DARK SITE IP ADDRESSING']

    # Read necessary CIQ sheets into Pandas dataframes
    with pd.ExcelFile(ciq) as raw_site_data:
        lit_site_df = pd.read_excel(raw_site_data, sheet_name=sheets_to_read[0], index_col=0)
        drk_site_df = pd.read_excel(raw_site_data, sheet_name=sheets_to_read[1], index_col=0)
    list_of_dataframes = [lit_site_df, drk_site_df]

    # Fills merged cells in site_id column with site names for every entry in dataframe
    # Drops columns that don't have a headers (unnamed)
    # Drops rows where all values are NaN
    # Iterate through a list of headers and replace NaN with 'na'
    for site_info in list_of_dataframes:
        site_info.index = pd.Series(site_info.index).fillna(method='ffill')
        site_info.drop(site_info.columns[site_info.columns.str.contains('unnamed', case=False)], axis=1, inplace=True)
        site_info.dropna(axis=0, how='all', inplace=True)
        site_info_columns = [col for col in site_info.columns]

        for col in site_info_columns:
            site_info[col] = site_info[col].fillna('na')

        # Convert 1's and 0's to 'true' or 'false' in dhcp column
        site_info['dhcp'] = site_info['dhcp'].astype(str)
        site_info['dhcp'] = site_info['dhcp'].replace(['1.0', '0.0'], ['true', 'false'])

    # list_of_dataframes will always be: [lit,dark]
    return list_of_dataframes


def dataframe_to_datamodel(dataframes):

    # Creating list of all unique site IDs for both Dark and Lit cell-sites
    all_lit_site_ids = list(set(site_id for site_id in dataframes[0].index))
    all_drk_site_ids = list(set(site_id for site_id in dataframes[1].index))

    all_lit_site_datamodels = []
    all_drk_site_datamodels = []

    def network_data_cleanup(dataframe, site_id):

        list_of_matching_dataframes = []

        for index, row in dataframe.iterrows():
            if str(site_id) == index:
                list_of_matching_dataframes.append(dict(row))

        # Modify keys in network dictionary from CIQ format to AWX format
        for network in list_of_matching_dataframes:
            try:
                network['name'] = network.pop('vlan_name')
                network['ipv4_cidr'] = network.pop('IPv4')
                network['ipv4_cidr'] = network['ipv4_cidr'].split('/')[-1]
                network['ipv6_cidr'] = network.pop('IPv6')
                network['ipv6_cidr'] = network['ipv6_cidr'].split('/')[-1]

            except KeyError as error:
                print("CIQ doesn't have necessary columns")
                print("Tool Requires Following Columns in CIQ: ")
                print("site_id, vlan_id, vlan_name, IPv4, IPv6, vrf, dhcp, ipam_use ")
                print(error)

            # Hard-coding ipam_zone as internal for now
            network['ipam_zone'] = 'INTERNAL'

        # Adding 'id' field for every network in list of networks
        for i in range(len(list_of_matching_dataframes)):
            list_of_matching_dataframes[i]['id'] = i + 1

        # Assemble network data into necessary format
        site_data_structure = {
             'adhoc_ipam': list_of_matching_dataframes,
             'ipam_environment': 'INTEROP',
             'site_id': site_id
        }

        return site_data_structure

    # Loop though all site IDs and append to list all networks for lit sites
    for lit_site_id in all_lit_site_ids:
        lit_parsed_data = network_data_cleanup(dataframes[0], lit_site_id)
        all_lit_site_datamodels.append(lit_parsed_data)

    # Loop though all site IDs and append to list all networks for dar sites
    for drk_site_id in all_drk_site_ids:
        drk_parsed_data = network_data_cleanup(dataframes[1], drk_site_id)
        all_drk_site_datamodels.append(drk_parsed_data)

    return all_lit_site_datamodels, all_drk_site_datamodels


if __name__ == '__main__':

    start = time()

    all_site_info = ciq_to_dataframe(ciq_path)
    lit, drk = dataframe_to_datamodel(all_site_info)

    page_break('lit sites', 20)
    print(dumps(lit, indent=4))

    page_break('dark sites', 20)
    print(dumps(drk, indent=4))

    end = time()

    page_break('time', 20)
    print(end - start)
