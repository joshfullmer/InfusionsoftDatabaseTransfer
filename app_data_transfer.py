import json
import os
import pandas as pd
import re
import datetime as dt
import sys
# from pprint import pprint
#import numpy as np

from models import Database


#  !!!!!!REQUIRES APP RELOAD WHEN COMPLETE!!!!!!

# TODO:
# Transfer most recent EmailStatus
# Opportunity StageMove


def transfer_lead_sources(source, destination):
    # Get matching lead source categories
    s_lsc = source.get_table('LeadSourceCategory')
    d_lsc = destination.get_table('LeadSourceCategory')

    # Generate labels for Id matching
    s_id = f'Id_{source.appname}'
    d_id = f'Id_{destination.appname}'

    # Generate list of matches, matching by Name
    lsc_matches = pd.merge(
        s_lsc,
        d_lsc,
        on='Name',
        how='left',
        suffixes=(f'_{source.appname}', f'_{destination.appname}')
    ).filter(items=[s_id, d_id])

    # Create missing records and store matches in dataframe
    lsc_rel = create_missing_records(
        'LeadSourceCategory',
        destination,
        s_lsc,
        lsc_matches,
    )

    # Lead source categories aren't required.  Adding 0 as a match.
    lsc_rel[0] = 0

    # Transfer lead sources
    s_ls = source.get_table('LeadSource')
    d_ls = destination.get_table('LeadSource')

    # Getting list of matches
    ls_matches = pd.merge(
        s_ls,
        d_ls,
        on='Name',
        how='left',
        suffixes=(f'_{source.appname}', f'_{destination.appname}')
    ).filter(items=[s_id, d_id])

    # Replace old lead source category id with new
    s_ls['LeadSourceCategoryId'] = s_ls['LeadSourceCategoryId'].map(lsc_rel)

    # Create missing lead sources
    ls_rel = create_missing_records(
        'LeadSource',
        destination,
        s_ls,
        ls_matches
    )

    # Return the lead source relationship dictionary
    return ls_rel


def transfer_tags(source, destination, tag_ids=[]):
    # Get tags to transfer, then filter by provided tag IDs list.
    s_tags = source.get_table('ContactGroup')
    d_tags = destination.get_table('ContactGroup')

    if tag_ids:
        s_tags = s_tags[s_tags['Id'].isin(tag_ids)].copy()
        cats_to_transfer = list(s_tags['GroupCategoryId'].unique())

    # Get matching tag categories, then filter only those used by the tags
    # to transfer
    s_tc = source.get_table('ContactGroupCategory')
    d_tc = destination.get_table('ContactGroupCategory')

    if tag_ids:
        s_tc = s_tc[s_tc['Id'].isin(cats_to_transfer)].copy()

    # Generate labels for Id matching
    s_id = f'Id_{source.appname}'
    d_id = f'Id_{destination.appname}'

    # Generate list of matches, matching by CategoryName
    tc_matches = pd.merge(
        s_tc,
        d_tc,
        on='CategoryName',
        how='left',
        suffixes=(f'_{source.appname}', f'_{destination.appname}')
    ).filter(items=[s_id, d_id])

    # Create missing records and store matches in dataframe
    tc_rel = create_missing_records(
        'ContactGroupCategory',
        destination,
        s_tc,
        tc_matches,
    )

    # Tag categories aren't required.  Adding 0 as a match.
    tc_rel[0] = 0

    # Transfer tags

    # Getting list of matches
    tag_matches = pd.merge(
        s_tags,
        d_tags,
        on='GroupName',
        how='left',
        suffixes=(f'_{source.appname}', f'_{destination.appname}')
    ).filter(items=[s_id, d_id])

    # Replace old tag category id with new
    s_tags['GroupCategoryId'] = s_tags['GroupCategoryId'].map(tc_rel)

    # Create missing tags
    tag_rel = create_missing_records(
        'ContactGroup',
        destination,
        s_tags,
        tag_matches
    )

    # Return the tag relationship dictionary
    return tag_rel


def transfer_dropdown_values(source, destination):
    modified = False
    dropdowns = [
        'optiontypes',
        'optiontitles',
        'optionsuffixes',
        'optionphonetypes',
        'optionfaxtypes'
    ]
    for dropdown in dropdowns:
        s_values = source.get_app_setting(dropdown).split(',')
        d_values = destination.get_app_setting(dropdown).split(',')
        missing_values = [v for v in s_values if v not in d_values]
        if missing_values:
            modified = True
        raw_string = ','.join(d_values + missing_values)
        values_string = raw_string.replace("'", "''")
        destination.update_app_setting(dropdown, values_string)
    return modified


def get_user_relationship(source, destination):
    """Returns a dictionary containing the relationship between user records
    matching using GlobalUserId, then by Email.  If no match is provided,
    the user ID will be replaced with 0, which leaves things unassigned."""
    # Generate labels for Id matching
    s_id = f'Id_{source.appname}'
    d_id = f'Id_{destination.appname}'

    s_users = source.get_table('User')
    d_users = destination.get_table('User')

    s_user_ids = s_users['Id'].tolist()
    d_user_ids = d_users['Id'].tolist()

    s_contacts = source.get_table('Contact', columns=['Id', 'Email'])
    d_contacts = destination.get_table('Contact', columns=['Id', 'Email'])

    s_contacts.replace('', pd.np.nan, inplace=True)
    s_contacts = s_contacts.dropna(subset=['Email'])
    s_contacts = s_contacts[s_contacts['Id'].isin(s_user_ids)]

    d_contacts.replace('', pd.np.nan, inplace=True)
    d_contacts = d_contacts.dropna(subset=['Email'])
    d_contacts = d_contacts[d_contacts['Id'].isin(d_user_ids)]

    contact_matches = pd.merge(
        s_contacts,
        d_contacts,
        on='Email',
        suffixes=(f'_{source.appname}', f'_{destination.appname}')
    ).filter(items=[s_id, d_id])

    user_matches = pd.merge(
        s_users,
        d_users,
        how='left',
        on='GlobalUserId',
        suffixes=(f'_{source.appname}', f'_{destination.appname}')
    ).filter(items=[s_id, d_id])
    user_matches.replace(pd.np.nan, 0, inplace=True)
    user_matches[d_id] = user_matches[d_id].astype(int)

    user_rel = {}
    for row in user_matches.itertuples():
        user_rel[getattr(row, s_id)] = getattr(row, d_id)
    for row in contact_matches.itertuples():
        user_rel[getattr(row, s_id)] = getattr(row, d_id)

    # Add system user -1
    user_rel[-1] = -1
    user_rel[0] = 0

    return user_rel


def transfer_contacts(
        source,
        destination,
        t_tags,
        t_ls,
        t_comp,
        contacts_with_tag_id=None,
        tag_ids=[]):
    """
    Transfers all contacts and companies to destination app

    Other objects transferred include:
    - Tags
    - Lead Sources
    """

    contacts = source.get_table('Contact')

    # Filter contacts to import
    if t_comp:
        contacts = contacts.loc[contacts.IsUser == 0
                                & ~(contacts.Id != contacts.CompanyID)]
    else:
        # Filter contacts to remove user records
        contacts = contacts[contacts.IsUser == 0]

    if contacts_with_tag_id:
        tag_apps = source.get_table('ContactGroupAssign')
        f_tag_apps = tag_apps[tag_apps['GroupId'] == contacts_with_tag_id]
        contact_ids_to_keep = f_tag_apps['ContactId'].tolist()
        contacts = contacts[contacts['Id'].isin(contact_ids_to_keep)]

    # Create new IDs and ID relationship
    old_ids = contacts['Id'].tolist()

    # Get auto increment and generate list of ids based on that
    offset = 500
    increment_start = destination.get_auto_increment('Contact') + offset
    increment_end = (2 * len(contacts)) + increment_start
    new_ids = [i for i in range(increment_start, increment_end, 2)]

    # Generate contact relationship from generated id list
    contact_rel = {0: 0}
    for i, id_num in enumerate(old_ids):
        contact_rel[id_num] = new_ids[i]

    # Get user relationship dictionary
    user_rel = get_user_relationship(source, destination)

    # Transfer Lead Sources
    if t_ls:
        # Get lead source relationship dictionary
        ls_rel = transfer_lead_sources(source, destination)

    # Transfer Tags
    if t_tags:
        # Get tag relationship dictionary
        tag_rel = transfer_tags(source, destination, tag_ids)

        # Convert Groups field using tag relationship dictionary
        new_groups = []
        for groups in contacts['Groups'].tolist():
            if groups and (not isinstance(groups, int)):
                group_ids = [str(tag_rel[int(x)])
                             for x in groups.split(',')
                             if x.isdigit() and
                             int(x) in tag_rel.keys()]

                new_groups.append(','.join(group_ids))
            else:
                new_groups.append(groups)
        contacts['Groups'] = new_groups

    # Field reassignments from generated relationship dictionaries
    contacts['Id'] = contacts['Id'].map(contact_rel)
    contacts['CompanyID'] = contacts['CompanyID'].map(contact_rel)
    contacts['CreatedBy'] = contacts['CreatedBy'].map(user_rel)
    contacts['LastUpdatedBy'] = contacts['LastUpdatedBy'].map(user_rel)
    contacts['OwnerID'] = contacts['OwnerID'].map(user_rel)
    contacts.drop(columns=['OffSetTimeZone'], axis = 1)
    if t_ls:
        contacts['LeadSourceId'] = contacts['LeadSourceId'].map(ls_rel)
    else:
        contacts['LeadSourceId'] = 0

    # Transfer contact records
    destination.insert_dataframe('Contact', contacts)

    # Transfer companies
    if t_comp:
        # Transfer primary contact for companies
        companies = source.get_table('Company')

        # Filter out items not in contact_rel
        companies = companies[companies['MainContactId'].isin(
                              list(contact_rel.keys()))]

        # Field reassignments
        companies['Id'] = companies['Id'].map(contact_rel)
        companies['MainContactId'] = companies['MainContactId'].map(
            contact_rel
        )

        # Transfer company records
        destination.insert_dataframe('Company', companies)

    # Apply Tag to Transferred Contacts
    apply_transfer_tag(source, destination, contact_rel)

    return contact_rel

def dataframe_difference(df1, df2, which=None):
    """Find rows which are different between two DataFrames."""
    comparison_df = df2.merge(df1,
                              indicator=True,
                              how='outer')
    if which is None:
        diff_df = comparison_df[comparison_df['_merge'] != 'both']
    else:
        diff_df = comparison_df[comparison_df['_merge'] == which]
    return diff_df

def transfer_custom_fields(source, destination, contact_rel):
    # Generate labels for Id matching
    s_id = f'Id_{source.appname}'
    d_id = f'Id_{destination.appname}'

    # Generate labels for Id matching
    s_id = f'Id_{source.appname}'
    d_id = f'Id_{destination.appname}'

    create_string = source.get_table_create('Custom_Contact')
    create_field_strings = create_string.split('\n')[2:-2]
    field_creation = {}
    for string in create_field_strings:
        fieldname = re.search(r'`(.*)`', string).group(1)
        string = string.replace(f'`{fieldname}`', '')
        string = string.strip().strip(',')
        field_creation[fieldname] = string

    s_custom_fields = source.get_table('DataFormField')
    s_contact_cfs = s_custom_fields[s_custom_fields['FormId'] == -1].copy()
    if not len(s_contact_cfs.index):
        return None
    d_custom_fields = destination.get_table('DataFormField')
    d_contact_cfs = d_custom_fields[d_custom_fields['FormId'] == -1].copy()

    # Get list of drilldown custom fieldnames
    drilldowns = s_custom_fields[(s_custom_fields['DataType'] == 23) &
                                 (s_custom_fields['FormId'] == -1)]
    drilldown_names = drilldowns['FieldName'].tolist()

    # Generate matches on Label and Type
    s_fieldname = f'FieldName_{source.appname}'
    items = [
        s_id,
        d_id,
        s_fieldname,
        f'NewDatabaseName'
    ]
    print(items)
    matches = pd.merge(
        s_contact_cfs,
        d_contact_cfs,
        how='left',
        on=['DisplayName', 'DataType'],
        suffixes=(f'_{source.appname}', f'_{destination.appname}')
    )

    # Handle database names
    d_db_names = destination.get_column_names('Custom_Contact')

    def fieldname_match_check(row):
        if pd.isnull(row[d_id]):
            return handle_db_names(row[s_fieldname], d_db_names)
        else:
            return row[f'FieldName_{source.appname}']

    matches['NewDatabaseName'] = matches.apply(
        lambda x: fieldname_match_check(x),
        axis=1
    )
    matches = matches.filter(items=items)

    # Filter to achieve list of missing custom fields
    missing = matches[matches[d_id].isnull()]
    missing_ids = missing[s_id].tolist()
    missing_rows = s_contact_cfs.loc[s_contact_cfs['Id'].isin(missing_ids)]
    missing_rows = missing_rows.copy()

    # Get auto increment and generate list of ids based on that
    offset = 50
    increment_start = destination.get_auto_increment('DataFormField') + offset
    increment_end = (2 * len(missing_rows)) + increment_start
    new_ids = [i for i in range(increment_start, increment_end, 2)]

    # Update missing categories to have newly generated ids
    new_ids_series = pd.Series(new_ids)
    missing_rows['Id'] = new_ids_series.values
    
    #####################################
    # Add custom field tabs and headers #
    #####################################

    s_tabs = source.get_table('DataFormTab')
    d_tabs = destination.get_table('DataFormTab')

    s_contact_tabs = s_tabs[s_tabs['FormId'] == -1].copy()
    d_contact_tabs = d_tabs[d_tabs['FormId'] == -1].copy()

    tab_matches = pd.merge(
        s_contact_tabs,
        d_contact_tabs,
        on='TabName',
        how='left',
        suffixes=(f'_{source.appname}', f'_{destination.appname}')
    ).filter(items=[s_id, d_id])

    tab_rel = create_missing_records(
        'DataFormTab',
        destination,
        s_contact_tabs,
        tab_matches,
    )
    print(tab_rel)
    sys.exit()

def transfer_custom_fieldsOLD(source, destination, contact_rel):
    """
    Creates custom fields in the destination app if they don't already exist
    by the same name and type

    Also adds the custom field data attached to contacts
    """
    # Generate labels for Id matching
    s_id = f'Id_{source.appname}'
    d_id = f'Id_{destination.appname}'

    create_string = source.get_table_create('Custom_Contact')
    create_field_strings = create_string.split('\n')[2:-2]
    field_creation = {}
    for string in create_field_strings:
        fieldname = re.search(r'`(.*)`', string).group(1)
        string = string.replace(f'`{fieldname}`', '')
        string = string.strip().strip(',')
        field_creation[fieldname] = string

    s_custom_fields = source.get_table('DataFormField')
    s_contact_cfs = s_custom_fields[s_custom_fields['FormId'] == -1].copy()
    if not len(s_contact_cfs.index):
        return None
    d_custom_fields = destination.get_table('DataFormField')
    d_contact_cfs = d_custom_fields[d_custom_fields['FormId'] == -1].copy()

    # Get list of drilldown custom fieldnames
    drilldowns = s_custom_fields[(s_custom_fields['DataType'] == 23) &
                                 (s_custom_fields['FormId'] == -1)]
    drilldown_names = drilldowns['FieldName'].tolist()

    # Generate matches on Label and Type
    s_fieldname = f'FieldName_{source.appname}'
    items = [
        s_id,
        d_id,
        s_fieldname,
        f'NewDatabaseName'
    ]
    matches = pd.merge(
        s_contact_cfs,
        d_contact_cfs,
        how='left',
        on=['DisplayName', 'DataType'],
        suffixes=(f'_{source.appname}', f'_{destination.appname}')
    )

    # Handle database names
    d_db_names = destination.get_column_names('Custom_Contact')

    def fieldname_match_check(row):
        if pd.isnull(row[d_id]):
            return handle_db_names(row[s_fieldname], d_db_names)
        else:
            return row[f'FieldName_{source.appname}']

    matches['NewDatabaseName'] = matches.apply(
        lambda x: fieldname_match_check(x),
        axis=1
    )
    matches = matches.filter(items=items)

    # Filter to achieve list of missing custom fields
    missing = matches[matches[d_id].isnull()]
    missing_ids = missing[s_id].tolist()
    missing_rows = s_contact_cfs.loc[s_contact_cfs['Id'].isin(missing_ids)]
    missing_rows = missing_rows.copy()

    # Get auto increment and generate list of ids based on that
    offset = 50
    increment_start = destination.get_auto_increment('DataFormField') + offset
    increment_end = (2 * len(missing_rows)) + increment_start
    new_ids = [i for i in range(increment_start, increment_end, 2)]

    # Update missing categories to have newly generated ids
    new_ids_series = pd.Series(new_ids)
    missing_rows['Id'] = new_ids_series.values

    #####################################
    # Add custom field tabs and headers #
    #####################################

    s_tabs = source.get_table('DataFormTab')
    d_tabs = destination.get_table('DataFormTab')

    s_contact_tabs = s_tabs[s_tabs['FormId'] == -1].copy()
    d_contact_tabs = d_tabs[d_tabs['FormId'] == -1].copy()

    tab_matches = pd.merge(
        s_contact_tabs,
        d_contact_tabs,
        on='TabName',
        how='left',
        suffixes=(f'_{source.appname}', f'_{destination.appname}')
    ).filter(items=[s_id, d_id])

    tab_rel = create_missing_records(
        'DataFormTab',
        destination,
        s_contact_tabs,
        tab_matches,
    )

    s_headers = source.get_table('DataFormGroup')
    d_headers = destination.get_table('DataFormGroup')

    s_headers['TabId'] = s_headers['TabId'].map(tab_rel)

    header_matches = pd.merge(
        s_headers,
        d_headers,
        on=['TabId', 'GroupName'],
        how='left',
        suffixes=(f'_{source.appname}', f'_{destination.appname}')
    ).filter(items=[s_id, d_id])

    header_rel = create_missing_records(
        'DataFormGroup',
        destination,
        s_headers,
        header_matches
    )

    fieldname_rel = {}
    for row in matches.itertuples():
        old_name = getattr(row, s_fieldname)
        new_name = getattr(row, 'NewDatabaseName')
        fieldname_rel[old_name] = new_name
        # TODO: fix fieldname_rel to work after fields are added
        #  to dataform field

    #####################
    # ADD Custom Fields #
    #####################

    # Map fields
    missing_rows['GroupId'] = missing_rows['GroupId'].map(header_rel)
    missing_rows['FieldName'] = missing_rows['FieldName'].map(fieldname_rel)
    
    # Insert dataframe for custom fields
    if not missing_rows.empty:
        destination.insert_dataframe('DataFormField', missing_rows)

    # Add custom fields to Custom_Contact
    missing_fieldnames = missing_rows['FieldName'].tolist()
    #print(missing_fieldnames)
    if missing_fieldnames:
        destination.alter_custom_field_table(
            field_creation,
            fieldname_rel,
            missing_fieldnames
        )

    # Update matching relationship with new Ids
    d_fieldname = f'FieldName_{destination.appname}'
    matches.loc[matches[d_id].isnull(), d_id] = new_ids_series.values
    matches[d_id] = matches[d_id].astype(int)
    matches.rename(
        {'NewDatabaseName': d_fieldname},
        axis=1,
        inplace=True,
    )

    # Create custom field relationship
    s_cfs = source.get_table('DataFormField')
    s_cfs = s_cfs[s_cfs['FormId'] == -1].copy()
    d_cfs = destination.get_table('DataFormField')
    d_cfs = d_cfs[d_cfs['FormId'] == -1].copy()
    cfs = pd.merge(
        s_cfs,
        d_cfs,
        how='left',
        on=['DisplayName', 'DataType'],
        suffixes=(f'_{source.appname}', f'_{destination.appname}')
    )
    cf_id_rel = {}
    cf_name_rel = {}
    for _, cf in cfs.iterrows():
        cf_id_rel[cf[s_id]] = cf[d_id]
        cf_name_rel[cf[f'FieldName_{source.appname}']] = cf[
            f'FieldName_{destination.appname}']

    #########################
    # ADD Custom Field Data #
    #########################
    s_fieldnames = s_contact_cfs['FieldName'].tolist()
    s_fieldnames.append('Id')
    #print(s_fieldnames) Field names in a list

    cf_data = source.get_table('Custom_Contact').filter(items=s_fieldnames)

    # Manage drilldowns
    if drilldown_names:
        for name in drilldown_names:
            cf_data[name] = cf_data[name].astype(object)
            cf_data[name] = cf_data[name].fillna('')
            cf_data[name] = cf_data[name].str.replace(r'\.0', '', regex=True)
        s_dds = source.get_table('DrilldownOption')

        # Get new Ids
        offset = 50
        increment_start = (destination.get_auto_increment('DrilldownOption') +
                           offset)
        increment_end = (2 * len(s_dds)) + increment_start
        new_ids = [i for i in range(increment_start, increment_end, 2)]
        ddo_rel = dict(zip(s_dds['Id'].tolist(), new_ids))
        ddo_rel[-1] = -1
        ddo_rel[0] = 0

        # Map new Ids
        s_dds['Id'] = s_dds['Id'].map(ddo_rel)
        s_dds['CategoryId'] = s_dds['CategoryId'].map(
            ddo_rel
        )
        s_dds['FieldId'] = s_dds['FieldId'].map(
            cf_id_rel
        )

        # Format data
        s_dds = s_dds[pd.notnull(
            s_dds['FieldId'])]
        s_dds['FieldId'] = s_dds['FieldId'].astype(str)
        s_dds['FieldId'] = s_dds['FieldId'].str.replace(
            r'\.0',
            '',
            regex=True
        )

        # Check if drilldowns need to be created
        d_dds = destination.get_table('DrilldownOption')
        d_dds['FieldId'] = d_dds['FieldId'].astype(str)
        dds = pd.merge(
            s_dds,
            d_dds,
            how='inner',
            on=['FieldId', 'Label'],
            suffixes=(f'_{source.appname}', f'_{destination.appname}')
        )
        e_dds = dds[s_id].tolist()
        dds_to_import = s_dds[~s_dds['Id'].isin(e_dds)]

        # Add rows
        if not dds_to_import.empty:
            destination.insert_dataframe('DrilldownOption', dds_to_import)
    #print(cf_data['TestRadio'])
    
    # Filter out contacts not in contact_rel
    cf_data = cf_data[cf_data['Id'].isin(list(contact_rel.keys()))]
    #print(cf_data['TestRadio'])
    cf_data['Id'] = cf_data['Id'].map(contact_rel)
    cf_data_to_import = cf_data[cf_data['Id'].notnull()].copy()
    #print(cf_data_to_import['TestRadio'])
    cf_data_to_import['Id'] = cf_data_to_import['Id'].astype(int)
    #print(cf_data_to_import['TestRadio'])
    cf_data_to_import.rename(cf_name_rel, axis=1, inplace=True)
    print(cf_data_to_import.columns)
    #sys.exit()
    #josh columns
    oldColumns = list(cf_data_to_import.columns)
    #for a in oldColumns:
    #    print(a)
    #sys.exit()
    #for col in cf_data_to_import.columns: 
    #    print(col)
    #sys.exit()
    cf_data_to_import = cf_data_to_import[list(cf_name_rel.values())]

    # Gets custom_contact combines and creates statement if any columns need to be added
    #Get source show custom_contact
    source_columns = source.show_table_columns('Custom_Contact')
    #Get source show custom_contact
    dest_columns = destination.show_table_columns('Custom_Contact')
    #source to dataframe
    source_columns = pd.DataFrame(source_columns, columns =['Field', 'Type', 'Null', 'Key', 'Default', 'Extra'])
    
    

    #I have Josh Columns I need to add 0's to my list of missing columns and compare with josh, if they match insert
    #print(source_columns)
    #print(oldColumns)
    
    #destination to dataframe
    dest_columns = pd.DataFrame(dest_columns, columns =['Field', 'Type', 'Null', 'Key', 'Default', 'Extra'])
    combine_fields = dataframe_difference(source_columns, dest_columns)
    select_color = combine_fields.loc[combine_fields['_merge'] == 'right_only']
    select_color1 = combine_fields.loc[combine_fields['_merge'] == 'left_only']
    flap = select_color['Field'].tolist()
    snap = [i + '0' for i in flap]
    print(snap)
    print(oldColumns)
    sys.exit()
    #clap = pd.DataFrame(flap)
    #print(clap)
    sys.exit()
    alter_statement1 = ''
    for x in combine_fields.index:
        if combine_fields['_merge'][x] == 'right_only':
            field1 = ''.join(combine_fields['Field'][x])
            field_type1 = field1
            alter_statement1 += field_type1
    print(alter_statement1)  
    sys.exit()  
    alter_statement = ''
    for x in combine_fields.index:
        if combine_fields['_merge'][x] == 'right_only':
            field = ''.join(combine_fields['Field'][x])
            typefield = ''.join(combine_fields['Type'][x])
            field_type = 'ADD ' + field + ' ' + typefield + ', '
            alter_statement += field_type
    alter_statement = re.sub(r'\, $',';',alter_statement)
    
    destination.alter_missing_cfs(alter_statement)

    # TODO: add messaging for when they need to purge custom fields
    if not cf_data_to_import.empty:
        #print(cf_data_to_import)
        destination.insert_dataframe(
            'Custom_Contact',
            cf_data_to_import,
            replace=True
        )

    return matches


def transfer_tag_applications(source, destination, contact_rel, tag_ids=[]):
    """
    Transfers the tags which are applied to contacts.
    """

    s_tag_apps = source.get_table('ContactGroupAssign')
    d_tag_apps = destination.get_table('ContactGroupAssign')

    if tag_ids:
        s_tag_apps = s_tag_apps[s_tag_apps['GroupId'].isin(tag_ids)]

    # Get list of existing apps into one object each
    existing_tag_apps = {}
    for row in d_tag_apps.itertuples():
        old_contact_id = getattr(row, 'ContactId')
        old_tag_id = getattr(row, 'GroupId')
        if pd.isnull(old_contact_id):
            continue
        existing_tag_apps[old_contact_id] = existing_tag_apps.get(
            old_contact_id,
            []
        )
        existing_tag_apps[old_contact_id].append(old_tag_id)

    # Get tag relationship for mapping
    tag_rel = transfer_tags(source, destination, tag_ids)

    # Filter out contacts not in contact_rel
    s_tag_apps = s_tag_apps[s_tag_apps['ContactId'].isin(
                            list(contact_rel.keys()))]

    # Map relationships to generate new values
    s_tag_apps = s_tag_apps.drop(columns='Id')
    s_tag_apps['ContactId'] = s_tag_apps['ContactId'].map(contact_rel)
    s_tag_apps['GroupId'] = s_tag_apps['GroupId'].map(tag_rel)

    # Check if the tag application already exists
    def tag_exists(row):
        row = dict(row)
        contact_id = row['ContactId']
        if pd.isnull(contact_id):
            return False
        contact_id = int(contact_id)
        group_id = row['GroupId']
        return group_id in existing_tag_apps[contact_id]

    s_tag_apps['Exists?'] = s_tag_apps.apply(
        lambda x: tag_exists(x),
        axis=1
    )

    # Filter out rows that already exist
    tag_apps_to_import = s_tag_apps[~s_tag_apps['Exists?']].copy()

    # Remove Exists? column
    tag_apps_to_import = tag_apps_to_import.drop(columns='Exists?')

    # Remove duplicates that may have appeared
    tag_apps_to_import.drop_duplicates(['ContactId', 'GroupId'], inplace=True)

    # Add tag applications
    if not tag_apps_to_import.empty:
        destination.insert_dataframe('ContactGroupAssign', tag_apps_to_import)


def apply_transfer_tag(source, destination, contact_rel):
    """
    Transfers the tags which are applied to contacts.
    """

    # Get auto increment and generate list of ids based on that
    offset = 10
    tran_tag_id = destination.get_auto_increment('ContactGroup') + offset
    tran_tag = f'Data Transferred - {source.appname} > {destination.appname}'
    now = dt.datetime.now()
    tran_date_created = now.strftime('%Y-%m-%d %H:%M:%S')
    tran_dict = {
        'Id': tran_tag_id,
        'GroupName': tran_tag,
        'GroupCategoryId': 0,
        'GroupDescription': '',
        'ImportId': 0
        }
    tran_df = pd.DataFrame.from_dict([tran_dict])
    destination.insert_dataframe('ContactGroup', tran_df)

    contact_ids = [v for _, v in contact_rel.items()]
    tag_apps = pd.DataFrame({'ContactId': contact_ids})
    tag_apps['GroupId'] = tran_tag_id
    tag_apps['DateCreated'] = tran_date_created

    # Add tag applications
    if not tag_apps.empty:
        destination.insert_dataframe('ContactGroupAssign', tag_apps)


def transfer_contact_actions(source, destination, contact_rel):
    """
    Transfers all contact actions
    """

    actions = source.get_table('ContactAction')

    user_rel = get_user_relationship(source, destination)

    # Filter out contacts not in contact_rel
    actions = actions[actions['Id'].isin(list(contact_rel.keys()))]

    # Relationship mapping
    actions['ContactId'] = actions['ContactId'].map(contact_rel)
    actions['UserID'] = actions['UserID'].map(user_rel)
    actions['OpportunityId'] = 0
    actions['TemplateId'] = 0
    actions['FunnelId'] = 0
    actions['JGraphId'] = 0

    # Remove actions that don't have a contact
    actions_to_import = actions[actions['ContactId'].notnull()].copy()
    actions_to_import['ContactId'] = actions_to_import['ContactId'].astype(int)

    # Get auto increment and generate list of ids based on that
    offset = 100
    increment_start = destination.get_auto_increment('ContactAction') + offset
    increment_end = (2 * len(actions_to_import)) + increment_start
    new_ids = [i for i in range(increment_start, increment_end, 2)]

    # Create action relationship
    action_rel = dict(zip(actions_to_import['Id'].tolist(), new_ids))

    # Update missing categories to have newly generated ids
    new_ids_series = pd.Series(new_ids)
    actions_to_import['Id'] = new_ids_series.values

    # Add actions to destination
    if not actions_to_import.empty:
        destination.insert_dataframe('ContactAction', actions_to_import)

    return action_rel


def transfer_products(source, destination):
    """
    Transfer all products and return relationship dictionary
    """
    # Generate labels for Id matching
    s_id = f'Id_{source.appname}'
    d_id = f'Id_{destination.appname}'

    #####################
    # Transfer Products #
    #####################

    # Get tables
    s_products = source.get_table('Product')
    d_products = destination.get_table('Product')

    # Generate list of matches by product name
    product_matches = pd.merge(
        s_products,
        d_products,
        on='ProductName',
        how='left',
        suffixes=(f'_{source.appname}', f'_{destination.appname}')
    ).filter(items=[s_id, d_id])

    prod_rel = create_missing_records(
        'Product',
        destination,
        s_products,
        product_matches,
    )

    ###############################
    # Transfer Subscription Plans #
    ###############################

    s_subplans = source.get_table('SubscriptionPlan')
    d_subplans = destination.get_table('SubscriptionPlan')

    s_subplans['ProductId'] = s_subplans['ProductId'].map(prod_rel)
    s_subplans = s_subplans.dropna(subset=['ProductId'])

    sp_headers = [
        'ProductId',
        'Cycle',
        'Frequency',
        'NumberOfCycles',
        'PlanPrice'
    ]
    for header in sp_headers:
        s_subplans[header] = s_subplans[header].fillna(0)
        d_subplans[header] = d_subplans[header].fillna(0)
        if header == 'PlanPrice':
            s_subplans[header] = pd.to_numeric(s_subplans[header])
            d_subplans[header] = pd.to_numeric(d_subplans[header])
        else:
            s_subplans[header] = s_subplans[header].astype('int64')
            d_subplans[header] = d_subplans[header].astype('int64')

    subplan_matches = pd.merge(
        s_subplans,
        d_subplans,
        on=sp_headers,
        how='left',
        suffixes=(f'_{source.appname}', f'_{destination.appname}')
    ).filter(items=[s_id, d_id])

    subplan_rel = create_missing_records(
        'SubscriptionPlan',
        destination,
        s_subplans,
        subplan_matches
    )

    ###############################
    # Transfer Product Categories #
    ###############################

    s_categories = source.get_table('ProductCategory')
    d_categories = destination.get_table('ProductCategory')

    cat_matches = pd.merge(
        s_categories,
        d_categories,
        on='CategoryDisplayName',
        how='left',
        suffixes=(f'_{source.appname}', f'_{destination.appname}')
    ).filter(items=[s_id, d_id])

    # Get list of missing lead source categories
    missing = cat_matches[cat_matches[d_id].isnull()]
    missing_ids = missing[s_id].tolist()
    missing_bools = s_categories['Id'].isin(missing_ids)
    missing_categories = s_categories[missing_bools].copy()

    # Get auto increment and generate list of ids based on that
    off = 50
    increment_start = destination.get_auto_increment('ProductCategory') + off
    increment_end = (2 * len(missing_categories)) + increment_start
    new_ids = [i for i in range(increment_start, increment_end, 2)]

    # Update missing categories to have newly generated ids
    new_ids_series = pd.Series(new_ids)
    missing_categories['Id'] = new_ids_series.values

    # Update matching relationship with new Ids
    cat_matches.loc[cat_matches[d_id].isnull(), d_id] = new_ids_series.values
    cat_matches[d_id] = cat_matches[d_id].astype(int)

    cat_rel = {}
    for row in cat_matches.itertuples():
        cat_rel[getattr(row, s_id)] = getattr(row, d_id)

    # Handle Parent ID from newly generated IDs
    missing_categories['ParentId'] = missing_categories['ParentId'].fillna(0)
    missing_categories['ParentId'] = missing_categories['ParentId'].astype(int)

    # Add product categories
    if not missing_categories.empty:
        destination.insert_dataframe('ProductCategory', missing_categories)

    return prod_rel, subplan_rel


def transfer_opportunities(
    source,
    destination,
    contact_rel,
    prod_rel,
    subplan_rel
):
    """
    TODO:
    - AffiliateID
    - PayPlanID
    - AssignedTo group
    """

    # Generate labels for Id matching
    s_id = f'Id_{source.appname}'
    d_id = f'Id_{destination.appname}'

    ##########
    # Stages #
    ##########

    s_stages = source.get_table('Stage')
    d_stages = destination.get_table('Stage')

    stage_matches = pd.merge(
        s_stages,
        d_stages,
        on='StageName',
        how='left',
        suffixes=(f'_{source.appname}', f'_{destination.appname}')
    ).filter(items=[s_id, d_id])

    s_stages['StageOrder'] = 0

    stage_rel = create_missing_records(
        'Stage',
        destination,
        s_stages,
        stage_matches,
        10
    )

    #################
    # Opportunities #
    #################

    opps = source.get_table('Opportunity')

    # Handle deleted stages

    stage_ids = set(opps['StageID'].tolist())
    stage_keys = stage_rel.keys()
    default_stage_id = int(destination.get_app_setting('defaultstage'))
    for stage_id in stage_ids:
        if stage_id not in stage_keys:
            stage_rel[stage_id] = default_stage_id

    # Set Won and Loss stages

    won_stage_string = source.get_app_setting('stagewin')
    if won_stage_string:
        won_stage_id = int(won_stage_string)
        new_won_stage_id = stage_rel[won_stage_id]
        destination.update_app_setting('stagewin', new_won_stage_id)

    loss_stage_string = source.get_app_setting('stageloss')
    if loss_stage_string:
        loss_stage_id = int(loss_stage_string)
        new_loss_stage_id = stage_rel[loss_stage_id]
        destination.update_app_setting('stageloss', new_loss_stage_id)

    # Filter out contacts not in contact_rel
    opps = opps[opps['ContactID'].isin(list(contact_rel.keys()))]

    # Field mapping

    user_rel = get_user_relationship(source, destination)

    opps['ContactID'] = opps['ContactID'].map(contact_rel)
    opps['StageID'] = opps['StageID'].map(stage_rel)
    opps['UserID'] = opps['UserID'].map(user_rel)
    opps['CreatedBy'] = opps['CreatedBy'].map(user_rel)
    opps['LastUpdatedBy'] = opps['LastUpdatedBy'].map(user_rel)
    opps['AffiliateId'] = 0
    opps['PayPlanId'] = 0

    # Generate matches

    opp_matches = opps.filter(items=['Id']).copy()
    opp_matches.rename({'Id': s_id}, axis=1, inplace=True)
    opp_matches[d_id] = pd.np.nan

    # Create new opportunities
    opp_rel = create_missing_records(
        'Opportunity',
        destination,
        opps,
        opp_matches,
        50
    )

    #####################
    # Product Interests #
    #####################

    # Get Product Interests Table
    s_allpi = source.get_table('ProductInterest')
    s_oppi = s_allpi.loc[s_allpi['ObjType'] == 'Opportunity'].copy()

    ####################################
    # GENERATE NEW PRODUCTINTEREST IDS #
    ####################################

    # Get auto increment and generate list of ids based on that
    offset = 50
    increment_start = (destination.get_auto_increment('ProductInterest') +
                       offset)
    increment_end = (2 * len(s_oppi)) + increment_start
    new_oppi_ids = [i for i in range(increment_start, increment_end, 2)]

    # Create prodinterest relationship
    oppi_rel = dict(zip(s_oppi['Id'].tolist(), new_oppi_ids))
    oppi_rel[0] = 0

    # Update missing prodinterest to have newly generated ids
    new_oppi_ids_series = pd.Series(new_oppi_ids)
    s_oppi['Id'] = new_oppi_ids_series.values

    # Field mapping

    s_oppi['ProductId'] = s_oppi['ProductId'].map(prod_rel)
    s_oppi['ObjectId'] = s_oppi['ObjectId'].map(opp_rel)
    s_oppi['SubscriptionPlanId'] = s_oppi['SubscriptionPlanId'].map(
        subplan_rel)
    s_oppi['LegacyProductId'] = None

    return opp_rel


def transfer_credit_cards(old_appname, database, contact_rel):
    return database.move_credit_cards(old_appname, contact_rel)


def transfer_subscriptions(
    source,
    destination,
    contact_rel,
    cc_rel,
    prod_rel,
    subplan_rel
):

    subs = source.get_table('JobRecurring')

    # Filter out contacts not in contact_rel
    subs = subs[subs['ContactId'].isin(list(contact_rel.keys()))]

    # ID mapping
    subs['ContactId'] = subs['ContactId'].map(contact_rel)
    subs['CC1'] = subs['CC1'].map(cc_rel)
    subs['CC2'] = 0
    subs['MerchantAccountId'] = 0
    subs['AffiliateId'] = 0
    subs['LeadAffiliateId'] = 0
    subs['SubscriptionPlanId'] = subs['SubscriptionPlanId'].map(
        subplan_rel)
    subs['ProductId'] = subs['ProductId'].map(prod_rel)
    subs['ShippingOptionId'] = 0
    subs['PaymentGatewayId'] = 0
    subs['PaymentSubType'] = 'USE_DEFAULT'
    # TODO Set Merchant Account Id from User Input

    # Remove subscriptions that don't have a contact
    subs_to_import = subs[subs['ContactId'].notnull()].copy()
    subs_to_import['ContactId'] = subs_to_import['ContactId'].astype(int)

    # Get auto increment and generate list of ids based on that
    offset = 50
    increment_start = destination.get_auto_increment('JobRecurring') + offset
    increment_end = (2 * len(subs_to_import)) + increment_start
    new_ids = [i for i in range(increment_start, increment_end, 2)]

    # Create subscription relationship
    sub_rel = dict(zip(subs_to_import['Id'].tolist(), new_ids))

    # Update missing categories to have newly generated ids
    new_ids_series = pd.Series(new_ids)
    subs_to_import['Id'] = new_ids_series.values

    # Add actions to destination
    if not subs_to_import.empty:
        destination.insert_dataframe('JobRecurring', subs_to_import)

    return sub_rel


def transfer_jobtojobrecurring(
    source,
    destination,
    job_rel,
    sub_rel
):

    s_jtjr = source.get_table('JobToJobRecurring')

    ######################################
    # GENERATE NEW JOBTOJOBRECURRING IDS #
    ######################################

    # Get auto increment and generate list of ids based on that
    offset = 50
    increment_start = (destination.get_auto_increment('JobToJobRecurring') +
                       offset)
    increment_end = (2 * len(s_jtjr)) + increment_start
    new_jtjr_ids = [i for i in range(increment_start, increment_end, 2)]

    # Create jobtojobrecurring relationship
    jtjr_rel = dict(zip(s_jtjr['Id'].tolist(), new_jtjr_ids))

    # Update missing jobtojobrecurrings to have newly generated ids
    new_jtjr_ids_series = pd.Series(new_jtjr_ids)
    s_jtjr['Id'] = new_jtjr_ids_series.values

    #####################################
    # JOBTOJOBRECURRING TABLE TRANSFORM #
    #####################################

    s_jtjr['JobId'] = s_jtjr['JobId'].map(job_rel)
    s_jtjr['JobRecurringId'] = s_jtjr['JobRecurringId'].map(sub_rel)

    s_jtjr = s_jtjr[s_jtjr['JobId'].notnull()
                    & s_jtjr['JobRecurringId'].notnull()]

    # Add jobtojobrecurring to destination
    if not s_jtjr.empty:
        destination.insert_dataframe('JobToJobRecurring', s_jtjr)

    return jtjr_rel


def transfer_orders(
    source,
    destination,
    contact_rel,
    prod_rel,
    cc_rel,
    subplan_rel
):
    """
    Things to transfer before Jobs:
    - Address
    """

    user_rel = get_user_relationship(source, destination)

    ###################
    # RETRIEVE TABLES #
    ###################

    s_addresses = source.get_table('Address')
    s_jobs = source.get_table('Job')
    s_invoices = source.get_table('Invoice')
    s_payplans = source.get_table('PayPlan')
    s_invoiceitems = source.get_table('InvoiceItem')
    s_orderitems = source.get_table('OrderItem')
    s_invoicepayments = source.get_table('InvoicePayment')
    s_payments = source.get_table('Payment')
    s_payplanitems = source.get_table('PayPlanItem')

    ############################
    # GENERATE NEW ADDRESS IDS #
    ############################

    # Get auto increment and generate list of ids based on that
    offset = 50
    increment_start = destination.get_auto_increment('Address') + offset
    increment_end = (2 * len(s_addresses)) + increment_start
    new_address_ids = [i for i in range(increment_start, increment_end, 2)]

    # Create address relationship
    add_rel = dict(zip(s_addresses['Id'].tolist(), new_address_ids))

    # Update missing categories to have newly generated ids
    new_add_ids_series = pd.Series(new_address_ids)
    s_addresses['Id'] = new_add_ids_series.values

    ########################
    # GENERATE NEW JOB IDS #
    ########################

    # Get auto increment and generate list of ids based on that
    offset = 50
    increment_start = destination.get_auto_increment('Job') + offset
    increment_end = (2 * len(s_jobs)) + increment_start
    new_job_ids = [i for i in range(increment_start, increment_end, 2)]

    # Create jobs relationship
    job_rel = dict(zip(s_jobs['Id'].tolist(), new_job_ids))
    job_rel[0] = 0

    # Update missing jobs to have newly generated ids
    new_job_ids_series = pd.Series(new_job_ids)
    s_jobs['Id'] = new_job_ids_series.values

    ############################
    # GENERATE NEW INVOICE IDS #
    ############################

    # Get auto increment and generate list of ids based on that
    offset = 50
    increment_start = destination.get_auto_increment('Invoice') + offset
    increment_end = (2 * len(s_invoices)) + increment_start
    new_invoice_ids = [i for i in range(increment_start, increment_end, 2)]

    # Create invoices relationship
    invoice_rel = dict(zip(s_invoices['Id'].tolist(), new_invoice_ids))
    invoice_rel[0] = 0

    # Update missing invoices to have newly generated ids
    new_invoice_ids_series = pd.Series(new_invoice_ids)
    s_invoices['Id'] = new_invoice_ids_series.values

    ############################
    # GENERATE NEW PAYPLAN IDS #
    ############################

    # Get auto increment and generate list of ids based on that
    offset = 50
    increment_start = destination.get_auto_increment('PayPlan') + offset
    increment_end = (2 * len(s_payplans)) + increment_start
    new_payplan_ids = [i for i in range(increment_start, increment_end, 2)]

    # Create payplans relationship
    payplan_rel = dict(zip(s_payplans['Id'].tolist(), new_payplan_ids))
    payplan_rel[0] = 0

    # Update missing payplans to have newly generated ids
    new_payplan_ids_series = pd.Series(new_payplan_ids)
    s_payplans['Id'] = new_payplan_ids_series.values

    ################################
    # GENERATE NEW INVOICEITEM IDS #
    ################################

    # Get auto increment and generate list of ids based on that
    offset = 50
    increment_start = destination.get_auto_increment('InvoiceItem') + offset
    increment_end = (2 * len(s_invoiceitems)) + increment_start
    new_invoiceitem_ids = [i for i in range(increment_start, increment_end, 2)]

    # Create invoiceitems relationship
    invoiceitem_rel = dict(
        zip(s_invoiceitems['Id'].tolist(), new_invoiceitem_ids)
    )
    invoiceitem_rel[0] = 0

    # Update missing invoiceitems to have newly generated ids
    new_invoiceitem_ids_series = pd.Series(new_invoiceitem_ids)
    s_invoiceitems['Id'] = new_invoiceitem_ids_series.values

    ##############################
    # GENERATE NEW ORDERITEM IDS #
    ##############################

    # Get auto increment and generate list of ids based on that
    offset = 50
    increment_start = destination.get_auto_increment('OrderItem') + offset
    increment_end = (2 * len(s_orderitems)) + increment_start
    new_orderitem_ids = [i for i in range(increment_start, increment_end, 2)]

    # Create orderitems relationship
    orderitem_rel = dict(
        zip(s_orderitems['Id'].tolist(), new_orderitem_ids)
    )
    orderitem_rel[0] = 0

    # Update missing orderitems to have newly generated ids
    new_orderitem_ids_series = pd.Series(new_orderitem_ids)
    s_orderitems['Id'] = new_orderitem_ids_series.values

    ###################################
    # GENERATE NEW INVOICEPAYMENT IDS #
    ###################################

    # Get auto increment and generate list of ids based on that
    offset = 50
    increment_start = destination.get_auto_increment('InvoicePayment') + offset
    increment_end = (2 * len(s_invoicepayments)) + increment_start
    new_ip_ids = [i for i in range(increment_start, increment_end, 2)]

    # Create invoicepayments relationship
    invoicepayment_rel = dict(
        zip(s_invoicepayments['Id'].tolist(), new_ip_ids)
    )
    invoicepayment_rel[0] = 0

    # Update missing invoicepayments to have newly generated ids
    new_invoicepayment_ids_series = pd.Series(new_ip_ids)
    s_invoicepayments['Id'] = new_invoicepayment_ids_series.values

    ############################
    # GENERATE NEW PAYMENT IDS #
    ############################

    # Get auto increment and generate list of ids based on that
    offset = 50
    increment_start = destination.get_auto_increment('Payment') + offset
    increment_end = (2 * len(s_payments)) + increment_start
    new_payment_ids = [i for i in range(increment_start, increment_end, 2)]

    # Create payments relationship
    payment_rel = dict(
        zip(s_payments['Id'].tolist(), new_payment_ids)
    )
    payment_rel[0] = 0

    # Update missing payments to have newly generated ids
    new_payment_ids_series = pd.Series(new_payment_ids)
    s_payments['Id'] = new_payment_ids_series.values

    ################################
    # GENERATE NEW PAYPLANITEM IDS #
    ################################

    # Get auto increment and generate list of ids based on that
    offset = 50
    increment_start = destination.get_auto_increment('PayPlanItem') + offset
    increment_end = (2 * len(s_payplanitems)) + increment_start
    new_payplanitem_ids = [i for i in range(increment_start, increment_end, 2)]

    # Create payplanitem relationship
    payplanitem_rel = dict(
        zip(s_payplanitems['Id'].tolist(), new_payplanitem_ids)
    )
    payplanitem_rel[0] = 0

    # Update missing payplanitem to have newly generated ids
    new_payplanitem_ids_series = pd.Series(new_payplanitem_ids)
    s_payplanitems['Id'] = new_payplanitem_ids_series.values

    #######################
    # JOB TABLE TRANSFORM #
    #######################

    # Filter out contacts not in contact_rel
    s_jobs = s_jobs[s_jobs['ContactId'].isin(list(contact_rel.keys()))]

    s_jobs['ContactId'] = s_jobs['ContactId'].map(contact_rel)
    s_jobs['CreatedBy'] = s_jobs['CreatedBy'].map(user_rel)
    s_jobs['LastUpdatedBy'] = s_jobs['LastUpdatedBy'].map(user_rel)
    s_jobs['ShippingAddressId'] = s_jobs['ShippingAddressId'].map(add_rel)
    s_jobs['InvoiceId'] = s_jobs['InvoiceId'].map(invoice_rel)
    s_jobs['AffiliateId'] = 0
    s_jobs['LeadAffiliateId'] = 0
    s_jobs['SalesId'] = 0
    s_jobs['TechId'] = 0
    s_jobs['OppId'] = 0
    s_jobs['ProductId'] = 0
    s_jobs['JumpLogId'] = 0
    s_jobs['MarketingEmailId'] = 0
    s_jobs['JobRecurringId'] = None
    s_jobs['LegacyJobRecurringInstanceId'] = None
    s_jobs['FileBoxId'] = None

    ###########################
    # INVOICE TABLE TRANSFORM #
    ###########################

    # Filter out contacts not in contact_rel
    s_invoices = s_invoices[s_invoices['ContactId'].isin(
                            list(contact_rel.keys()))]
    s_invoices = s_invoices[s_invoices['JobId'].isin(s_jobs['Id'].tolist())]

    # Convert ProductSold field using product relationship dictionary
    new_products = []
    for products in s_invoices['ProductSold'].tolist():
        if products:
            product_ids = []
            for x in products.split(','):
                if int(x) in prod_rel.keys():
                    product_ids.append(str(prod_rel[int(x)]))
            new_products.append(','.join(product_ids))
        else:
            new_products.append(products)
    s_invoices['ProductSold'] = new_products

    s_invoices['ContactId'] = s_invoices['ContactId'].map(contact_rel)
    s_invoices['UserCreate'] = s_invoices['UserCreate'].map(user_rel)
    s_invoices['JobId'] = s_invoices['JobId'].map(job_rel)
    s_invoices['PayPlanId'] = s_invoices['PayPlanId'].map(payplan_rel)
    s_invoices['AffiliateId'] = 0
    s_invoices['LeadAffiliateId'] = 0
    s_invoices['MarketingEmailId'] = 0

    ###########################
    # PAYPLAN TABLE TRANSFORM #
    ###########################

    # Filter out invoices not created above
    s_payplans = s_payplans[
        s_payplans['InvoiceId'].isin(s_invoices['Id'].tolist())
    ]

    s_payplans['InvoiceId'] = s_payplans['InvoiceId'].map(invoice_rel)
    if cc_rel:
        s_payplans['CC1'] = s_payplans['CC1'].map(cc_rel)
    else:
        s_payplans['CC1'] = 0
    s_payplans['CC2'] = 0
    s_payplans['MerchantAccountId'] = 0
    s_payplans['PaymentGatewayId'] = 0
    s_payplans['PaymentSubType'] = 'USE_DEFAULT'
    s_payplans['PayPalRefTxnId'] = None

    ###############################
    # INVOICEITEM TABLE TRANSFORM #
    ###############################

    # Filter out items skipped above
    s_invoiceitems = s_invoiceitems[
        s_invoiceitems['ContactId'].isin(list(contact_rel.keys()))
    ]
    s_invoiceitems = s_invoiceitems[
        s_invoiceitems['InvoiceId'].isin(s_invoices['Id'].tolist())
    ]
    s_invoiceitems = s_invoiceitems[
        s_invoiceitems['JobId'].isin(s_jobs['Id'].tolist())
    ]

    s_invoiceitems['InvoiceId'] = s_invoiceitems['InvoiceId'].map(invoice_rel)
    s_invoiceitems['JobId'] = s_invoiceitems['JobId'].map(job_rel)
    s_invoiceitems['UserCreate'] = s_invoiceitems['UserCreate'].map(user_rel)
    s_invoiceitems['ContactId'] = s_invoiceitems['ContactId'].map(contact_rel)
    s_invoiceitems['ChargeIds'] = None
    s_invoiceitems['InvoiceGroup'] = None
    s_invoiceitems['OrderItemId'] = s_invoiceitems['OrderItemId'].map(
        orderitem_rel)

    #############################
    # ORDERITEM TABLE TRANSFORM #
    #############################

    # Filter out items not created above
    s_orderitems = s_orderitems[
        s_orderitems['OrderId'].isin(s_jobs['Id'].tolist())
    ]

    s_orderitems['OrderId'] = s_orderitems['OrderId'].map(job_rel)
    s_orderitems['ProductId'] = s_orderitems['ProductId'].map(prod_rel)
    s_orderitems['DiscountedOrderItemId'] = 0
    s_orderitems['InvoiceItemId'] = s_orderitems['InvoiceItemId'].map(
        invoiceitem_rel)
    s_orderitems['SubscriptionPlanId'] = s_orderitems.get(
        'SubscriptionPlanId').map(subplan_rel)
    s_orderitems['SourceOrderItemId'] = s_orderitems['SourceOrderItemId'].map(
        orderitem_rel)

    ###########################
    # PAYMENT TABLE TRANSFORM #
    ###########################

    # Filter out items skipped above
    s_payments = s_payments[
        s_payments['ContactId'].isin(list(contact_rel.keys()))
    ]
    s_payments = s_payments[
        s_payments['InvoiceId'].isin(s_invoices['Id'].tolist())
    ]

    s_payments['UserId'] = s_payments['UserId'].map(user_rel)
    s_payments['ContactId'] = s_payments['ContactId'].map(contact_rel)
    s_payments['InvoiceId'] = s_payments['InvoiceId'].map(invoice_rel)
    s_payments['ChargeId'] = 0
    s_payments['TransactionId'] = None
    s_payments['CollectionMethod'] = 'MANUAL'
    s_payments['PaymentSubType'] = 'MANUAL'
    s_payments['PaymentGatewayId'] = 0
    s_payments['RefundId'] = s_payments['RefundId'].map(payment_rel)

    ##################################
    # INVOICEPAYMENT TABLE TRANSFORM #
    ##################################

    # Filter out items skipped above
    s_invoicepayments = s_invoicepayments[
        s_invoicepayments['InvoiceId'].isin(s_invoices['Id'].tolist())
    ]
    s_invoicepayments = s_invoicepayments[
        s_invoicepayments['PaymentId'].isin(s_payments['Id'].tolist())
    ]

    s_invoicepayments['InvoiceId'] = s_invoicepayments['InvoiceId'].map(
        invoice_rel)
    s_invoicepayments['PaymentId'] = s_invoicepayments['PaymentId'].map(
        payment_rel)
    s_invoicepayments['RefundInvoicePaymentId'] = s_invoicepayments.get(
        'RefundInvoicePaymentId').map(invoicepayment_rel)

    ###############################
    # PAYPLANITEM TABLE TRANSFORM #
    ###############################

    # Filter out items skipped above
    s_payplanitems = s_payplanitems[
        s_payplanitems['PayPlanId'].isin(s_payplans['Id'].tolist())
    ]

    s_payplanitems['PayPlanId'] = s_payplanitems['PayPlanId'].map(payplan_rel)

    #################
    # INSERT TABLES #
    #################

    # Add addresses to destination
    if not s_addresses.empty:
        destination.insert_dataframe('Address', s_addresses)
    # Add Jobs to destination
    if not s_jobs.empty:
        destination.insert_dataframe('Job', s_jobs)
    # Add invoices to destination
    if not s_invoices.empty:
        destination.insert_dataframe('Invoice', s_invoices)
    # Add payplans to destination
    if not s_payplans.empty:
        destination.insert_dataframe('PayPlan', s_payplans)
    # Add invoiceitems to destination
    if not s_invoiceitems.empty:
        destination.insert_dataframe('InvoiceItem', s_invoiceitems)
    # Add orderitems to destination
    if not s_orderitems.empty:
        destination.insert_dataframe('OrderItem', s_orderitems)
    # Add invoicepayments to destination
    if not s_invoicepayments.empty:
        destination.insert_dataframe('InvoicePayment', s_invoicepayments)
    # Add payments to destination
    if not s_payments.empty:
        destination.insert_dataframe('Payment', s_payments)
    # Add invoicepayments to destination
    if not s_payplanitems.empty:
        destination.insert_dataframe('PayPlanItem', s_payplanitems)

    return job_rel


def disable_receipt_settings(database):
    """
    These settings are disabled so emails aren't mistakenly sent to people
    who have purchase things a long time ago.
    """
    settings = [
        'runPurchaseActionsOnManualOrders',
        'emailinvoiceflag',
        'invoicepayments'
    ]
    for setting in settings:
        value = database.get_app_setting(setting)
        if value == '1':
            database.update_app_setting(setting, 0)


def create_missing_records(tablename, database, data, matches, offset=0):
    s_id, d_id = list(matches)

    # Get list of missing items
    missing = matches[matches[d_id].isnull()]
    missing_ids = missing[s_id].tolist()
    missing_rows = data.loc[data['Id'].isin(missing_ids)].copy()

    # Get auto increment and generate list of ids based on that
    increment_start = database.get_auto_increment(tablename) + offset
    increment_end = (2 * len(missing_rows)) + increment_start
    new_ids = [i for i in range(increment_start, increment_end, 2)]

    # Update missing categories to have newly generated ids
    new_ids_series = pd.Series(new_ids)
    missing_rows['Id'] = new_ids_series.values

    # If there are missing items, insert into table
    if not missing_rows.empty:
        database.insert_dataframe(tablename, missing_rows)

    # Update matching relationship with new Ids
    matches.loc[matches[d_id].isnull(), d_id] = new_ids_series.values
    matches[d_id] = matches[d_id].astype(int)

    relationship = {}
    for row in matches.itertuples():
        relationship[getattr(row, s_id)] = getattr(row, d_id)

    return relationship


def handle_db_names(fieldname, existing_db_names):
    fieldname = re.sub(r'[^A-Za-z0-9]', '', fieldname)
    while fieldname in existing_db_names:
        matches = re.match(r'([A-Za-z]*)(\d*)$', fieldname)
        if matches.group(2):
            fieldname = matches.group(1) + str(int(matches.group(2)) + 1)
        else:
            fieldname = matches.group(1) + '0'
    return fieldname


if __name__ == '__main__':
    source = Database('qj154', 27011)
    destination = Database('ij520', 27001)

    # Returns boolean if dropdowns have been modified or not
    # This is so we can notify the user that they need to reload frontend
    dropdowns_modified = transfer_dropdown_values(source, destination)

    # Disable receipt triggers before transfering payments
    disable_receipt_settings(destination)

    # Make relationship directory if not exists
    os.makedirs('/relationships', exist_ok=True)

    # CONTACTS

    if os.path.isfile('/relationships/contact_rel.json'):
        with open('/relationships/contact_rel.json') as file:
            contact_rel = json.load(file)
        contact_rel = {int(k): int(v) for k, v in contact_rel.items()}
    else:
        contact_rel = transfer_contacts(source, destination, True, True, True)
        with open('/relationships/contact_rel.json', 'w') as file:
            json.dump(contact_rel, file)

    transfer_custom_fields(source, destination, contact_rel)

    transfer_tag_applications(source, destination, contact_rel)

    # CONTACT ACTIONS

    if os.path.isfile('/relationships/action_rel.json'):
        with open('/relationships/action_rel.json') as file:
            action_rel = json.load(file)
        action_rel = {int(k): int(v) for k, v in action_rel.items()}
    else:
        action_rel = transfer_contact_actions(source, destination, contact_rel)
        with open('/relationships/action_rel.json', 'w') as file:
            json.dump(action_rel, file)

    # PRODUCTS AND SUBSCRIPTION PLANS

    prod_rel, subplan_rel = transfer_products(source, destination)

    # OPPORTUNITIES

    if os.path.isfile('/relationships/opp_rel.json'):
        with open('/relationships/opp_rel.json') as file:
            opp_rel = json.load(file)
        opp_rel = {int(k): int(v) for k, v in opp_rel.items()}
    else:
        opp_rel = transfer_opportunities(
            source,
            destination,
            contact_rel,
            prod_rel,
            subplan_rel
        )
        with open('/relationships/opp_rel.json', 'w') as file:
            json.dump(opp_rel, file)

    # CREDIT CARDS

    if os.path.isfile('/relationships/cc_rel.json'):
        with open('/relationships/cc_rel.json') as file:
            cc_rel = json.load(file)
        cc_rel = {int(k): int(v) for k, v in cc_rel.items()}
    else:
        cc_rel = transfer_credit_cards(
            source.appname,
            destination,
            contact_rel
        )
        with open('/relationships/cc_rel.json', 'w') as file:
            json.dump(cc_rel, file)
    cc_rel[0] = 0

    # SUBSCRIPTIONS

    if os.path.isfile('/relationships/sub_rel.json'):
        with open('/relationships/sub_rel.json') as file:
            sub_rel = json.load(file)
        sub_rel = {int(k): int(v) for k, v in sub_rel.items()}
    else:
        sub_rel = transfer_subscriptions(
            source,
            destination,
            contact_rel,
            cc_rel,
            prod_rel,
            subplan_rel
        )
        with open('/relationships/sub_rel.json', 'w') as file:
            json.dump(sub_rel, file)

    # ORDERS
    if os.path.isfile('/relationships/job_rel.json'):
        with open('/relationships/job_rel.json') as file:
            job_rel = json.load(file)
        job_rel = {int(k): int(v) for k, v in job_rel.items()}
    else:
        job_rel = transfer_orders(
            source,
            destination,
            contact_rel,
            prod_rel,
            cc_rel,
            subplan_rel
        )
        with open('/relationships/job_rel.json', 'w') as file:
            json.dump(job_rel, file)

    # jobtojobrecurring
    if os.path.isfile('/relationships/jtjr_rel.json'):
        with open('/relationships/jtjr_rel.json') as file:
            jtjr_rel = json.load(file)
        jtjr_rel = {int(k): int(v) for k, v in jtjr_rel.items()}
    else:
        jtjr_rel = transfer_jobtojobrecurring(
            source,
            destination,
            job_rel,
            sub_rel
        )
        with open('/relationships/jtjr_rel.json', 'w') as file:
            json.dump(jtjr_rel, file)

    source.close()
    destination.close()

    if dropdowns_modified:
        print('IMPORTANT: Reload Frontend')
