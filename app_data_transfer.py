import json
import os
import pandas as pd
import re

from models import Database


#  !!!!!!REQUIRES APP RELOAD WHEN COMPLETE!!!!!!

source = Database('qj154', 27011)
destination = Database('ij520', 27001)

# TODO:
# Transfer most recent EmailStatus
# Opportunity StageMove
# Opportunity Product Interest

# TODO later:
# Transfer order history


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


def transfer_tags(source, destination):
    # Get matching tag categories
    s_tc = source.get_table('ContactGroupCategory')
    d_tc = destination.get_table('ContactGroupCategory')

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
    s_tags = source.get_table('ContactGroup')
    d_tags = destination.get_table('ContactGroup')

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
        values_string = ','.join(d_values + missing_values)
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


def transfer_contacts(source, destination):
    """
    Transfers all contacts and companies to destination app

    Other objects transferred include:
    - Tags
    - Lead Sources
    """

    contacts = source.get_table('Contact')

    # Filter contacts to remove user records
    contacts = contacts[contacts.IsUser == 0]

    # Create new IDs and ID relationship
    old_ids = contacts['Id'].tolist()

    # Get auto increment and generate list of ids based on that
    offset = 500
    increment_start = destination.get_auto_increment('Contact') + offset
    increment_end = (2 * len(contacts)) + increment_start
    new_ids = [i for i in range(increment_start, increment_end, 2)]

    # Generate contact relationship from generated id list
    contact_rel = {}
    for i, id_num in enumerate(old_ids):
        contact_rel[id_num] = new_ids[i]

    # Get user relationship dictionary
    user_rel = get_user_relationship(source, destination)

    # Get lead source relationship dictionary
    ls_rel = transfer_lead_sources(source, destination)

    # Get tag relationship dictionary
    tag_rel = transfer_tags(source, destination)

    # Convert Groups field using tag relationship dictionary
    new_groups = []
    for groups in contacts['Groups'].tolist():
        if groups:
            group_ids = [str(tag_rel[int(x)]) for x in groups.split(',')]
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
    contacts['LeadSourceId'] = contacts['LeadSourceId'].map(ls_rel)

    # Transfer contact records
    destination.insert_dataframe('Contact', contacts)

    # Transfer primary contact for companies
    companies = source.get_table('Company')

    # Field reassignments
    companies['Id'] = companies['Id'].map(contact_rel)
    companies['MainContactId'] = companies['MainContactId'].map(contact_rel)

    # Transfer company records
    destination.insert_dataframe('Company', companies)

    return contact_rel


def transfer_custom_fields(source, destination, contact_rel):
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
    d_custom_fields = destination.get_table('DataFormField')
    d_contact_cfs = d_custom_fields[d_custom_fields['FormId'] == -1].copy()

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
            return row[f'FieldName_{destination.appname}']

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

    #####################
    # ADD Custom Fields #
    #####################

    # Map header IDs
    missing_rows['GroupId'] = missing_rows['GroupId'].map(header_rel)

    # Delete old database name
    missing_fieldnames = missing_rows['FieldName'].tolist()
    missing_rows.drop('FieldName', axis=1, inplace=True)

    # Rename new database name column
    missing_rows.rename({'NewDatabaseName': 'FieldName'}, axis=1, inplace=True)

    # Insert dataframe for custom fields
    if not missing_rows.empty:
        destination.insert_dataframe('DataFormField', missing_rows)

    # Add custom fields to Custom_Contact
    fieldname_rel = {}
    for row in matches.itertuples():
        old_name = getattr(row, s_fieldname)
        new_name = getattr(row, 'NewDatabaseName')
        fieldname_rel[old_name] = new_name
    if missing_fieldnames:
        destination.alter_custom_field_table(
            field_creation,
            fieldname_rel,
            missing_fieldnames
        )

    # Update matching relationship with new Ids
    matches.loc[matches[d_id].isnull(), d_id] = new_ids_series.values
    matches[d_id] = matches[d_id].astype(int)
    matches.rename(
        {'NewDatabaseName': f'FieldName_{destination.appname}'},
        axis=1,
        inplace=True,
    )
    cf_rel = {}
    for row in matches.itertuples():
        old_id = getattr(row, s_id)
        new_id = getattr(row, d_id)
        cf_rel[old_id] = new_id

    #########################
    # ADD Custom Field Data #
    #########################
    cf_data = source.get_table('Custom_Contact')
    cf_data.rename(fieldname_rel, axis=1, inplace=True)
    d_db_names = destination.get_column_names('Custom_Contact')
    cf_data['Id'] = cf_data['Id'].map(contact_rel)
    cf_data_to_import = cf_data[cf_data['Id'].notnull()].copy()
    cf_data_to_import['Id'] = cf_data_to_import['Id'].astype(int)
    cf_data_to_import = cf_data_to_import[d_db_names]

    # TODO: add messaging for when they need to purge custom fields
    if not cf_data_to_import.empty:
        destination.insert_dataframe('Custom_Contact', cf_data_to_import)

    return matches


def transfer_tag_applications(source, destination, contact_rel):
    """
    Transfers the tags which are applied to contacts.
    """

    s_tag_apps = source.get_table('ContactGroupAssign')
    d_tag_apps = destination.get_table('ContactGroupAssign')

    # Get list of existing apps into one object each
    existing_tag_apps = []
    for row in d_tag_apps.itertuples():
        old_contact_id = getattr(row, 'ContactId')
        old_tag_id = getattr(row, 'GroupId')
        if pd.isnull(old_contact_id):
            continue
        existing_tag_apps.append(f'{int(old_contact_id)},{old_tag_id}')

    # Get tag relationship for mapping
    tag_rel = transfer_tags(source, destination)

    # Map relationships to generate new values
    s_tag_apps = s_tag_apps.drop(columns='Id')
    s_tag_apps['ContactId'] = s_tag_apps['ContactId'].map(contact_rel)
    s_tag_apps['GroupId'] = s_tag_apps['GroupId'].map(tag_rel)

    # Check if the tag application already exists
    def tag_exists(row):
        contact_id = row['ContactId']
        if pd.isnull(contact_id):
            return False
        contact_id = int(contact_id)
        group_id = row['GroupId']
        return f'{contact_id},{group_id}' in existing_tag_apps

    s_tag_apps['Exists?'] = s_tag_apps.apply(
        lambda x: tag_exists(x),
        axis=1
    )

    # Filter out rows that already exist
    tag_apps_to_import = s_tag_apps[~s_tag_apps['Exists?']].copy()

    # Remove Exists? column
    tag_apps_to_import = tag_apps_to_import.drop(columns='Exists?')

    # Add tag applications
    if not tag_apps_to_import.empty:
        destination.insert_dataframe('ContactGroupAssign', tag_apps_to_import)


def transfer_contact_actions(source, destination, contact_rel):
    """
    Transfers all contact actions
    """

    actions = source.get_table('ContactAction')

    user_rel = get_user_relationship(source, destination)

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

    subplan_matches = pd.merge(
        s_subplans,
        d_subplans,
        on=['ProductId', 'Cycle', 'Frequency', 'NumberOfCycles', 'PlanPrice'],
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


def transfer_opportunities(source, destination, contact_rel):
    """
    TODO:
    - Product Interests
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

    won_stage_id = int(source.get_app_setting('stagewin'))
    loss_stage_id = int(source.get_app_setting('stageloss'))

    new_won_stage_id = stage_rel[won_stage_id]
    new_loss_stage_id = stage_rel[loss_stage_id]

    destination.update_app_setting('stagewin', new_won_stage_id)
    destination.update_app_setting('stageloss', new_loss_stage_id)

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


def transfer_orders(source, destination, contact_rel, prod_rel):
    """
    Things to transfer before Jobs:
    - Address
    """

    user_rel = get_user_relationship(source, destination)

    #--------------------#
    # ADDRESSES          #
    #--------------------#

    s_addresses = source.get_table('Address')

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

    ####################
    # CREATE ADDRESSES #
    ####################

    # Add addresses to destination
    if not s_addresses.empty:
        destination.insert_dataframe('Address', s_addresses)

    #--------------------#
    # JOBS               #
    #--------------------#

    s_jobs = source.get_table('Job')
    s_invoices = source.get_table('Invoice')

    ########################
    # GENERATE NEW JOB IDS #
    ########################

    # Get auto increment and generate list of ids based on that
    offset = 50
    increment_start = destination.get_auto_increment('Job') + offset
    increment_end = (2 * len(s_jobs)) + increment_start
    new_job_ids = [i for i in range(increment_start, increment_end, 2)]

    # Create address relationship
    job_rel = dict(zip(s_jobs['Id'].tolist(), new_job_ids))
    job_rel[0] = 0

    # Update missing categories to have newly generated ids
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

    # Create address relationship
    invoice_rel = dict(zip(s_invoices['Id'].tolist(), new_invoice_ids))
    invoice_rel[0] = 0

    # Update missing categories to have newly generated ids
    new_invoice_ids_series = pd.Series(new_invoice_ids)
    s_invoices['Id'] = new_invoice_ids_series.values

    #######################
    # JOB TABLE TRANSFORM #
    #######################

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
    s_jobs['PromoCode'] = 0
    s_jobs['JumpLogId'] = 0
    s_jobs['MarketingEmailId'] = 0
    s_jobs['JobRecurringId'] = 0
    s_jobs['LegacyJobRecurringInstanceId'] = 0
    s_jobs['FileBoxId'] = 0

    ###############
    # CREATE JOBS #
    ###############

    # Add addresses to destination
    if not s_jobs.empty:
        destination.insert_dataframe('Job', s_jobs)


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


# Returns boolean if dropdowns have been modified or not
# This is so we can notify the user that they need to reload frontend
dropdowns_modified = transfer_dropdown_values(source, destination)

# Disable receipt triggers before transfering payments
disable_receipt_settings(destination)

# Make relationship directory if not exists
os.makedirs('/relationships', exist_ok=True)

# CONTACTS

# TODO ask user if contacts have been created or not
if os.path.isfile('contact_rel.json'):
    with open('contact_rel.json') as file:
        contact_rel = json.load(file)
    contact_rel = {int(k): int(v) for k, v in contact_rel.items()}
else:
    contact_rel = transfer_contacts(source, destination)
    with open('contact_rel.json', 'w') as file:
        json.dump(contact_rel, file)
    transfer_custom_fields(source, destination, contact_rel)

transfer_tag_applications(source, destination, contact_rel)

# CONTACT ACTIONS

if os.path.isfile('action_rel.json'):
    with open('action_rel.json') as file:
        action_rel = json.load(file)
    action_rel = {int(k): int(v) for k, v in action_rel.items()}
else:
    action_rel = transfer_contact_actions(source, destination, contact_rel)
    with open('action_rel.json', 'w') as file:
        json.dump(action_rel, file)

# PRODUCTS AND SUBSCRIPTION PLANS

prod_rel, subplan_rel = transfer_products(source, destination)

# OPPORTUNITIES

if os.path.isfile('opp_rel.json'):
    with open('opp_rel.json') as file:
        opp_rel = json.load(file)
    opp_rel = {int(k): int(v) for k, v in opp_rel.items()}
else:
    opp_rel = transfer_opportunities(source, destination, contact_rel)
    with open('opp_rel.json', 'w') as file:
        json.dump(opp_rel, file)

# CREDIT CARDS

if os.path.isfile('cc_rel.json'):
    with open('cc_rel.json') as file:
        cc_rel = json.load(file)
    cc_rel = {int(k): int(v) for k, v in cc_rel.items()}
else:
    cc_rel = transfer_credit_cards(source.appname, destination, contact_rel)
    with open('cc_rel.json', 'w') as file:
        json.dump(cc_rel, file)
cc_rel[0] = 0

# SUBSCRIPTIONS

if os.path.isfile('sub_rel.json'):
    with open('sub_rel.json') as file:
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
    with open('sub_rel.json', 'w') as file:
        json.dump(sub_rel, file)

# set merchant account to USE_DEFAULT, Id 0

source.close()
destination.close()

# os.system('cls') if os.name == 'nt' else os.system('clear')

if dropdowns_modified:
    print('IMPORTANT: Reload Frontend')
