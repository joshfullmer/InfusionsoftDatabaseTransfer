from collections import defaultdict
import json
import os

import app_data_transfer as adt
from models import Database


def clear():
    os.system('cls' if os.name == 'nt' else 'clear')


def main():
    clear()
    print("""
============ Keap Database Transfer Tool ============

This tool is for exporting, transforming, and importind data
between two applications.

Please provide information for each application:
    """)
    source_app = ''
    while len(source_app) <= 0:
        source_app = input('Source Application Name:\n')
    source_port = 0
    while len(str(source_port)) != 5:
        try:
            source_port = int(input('Source Application Port:\n'))
        except ValueError:
            print('Port number must be 5-digits.\n')
    destination_app = ''
    while len(destination_app) <= 0:
        destination_app = input('Destination Application Name:\n')
    destination_port = 0
    while len(str(destination_port)) != 5:
        try:
            destination_port = input('Destination Application Port:\n')
        except ValueError:
            print('Port number must be 5-digits.\n')
    clear()
    print("""
Thanks!  Please indicate which records should be transferred through
the following series of questions.

Yes is the default selection.  Press anything besides 'n' to say yes.
    """)
    config = defaultdict(bool)
    config['CONTACTS'] = input('Contacts? (Yn) ').lower() != 'n'
    if not config['CONTACTS']:
        config['CONTACTS'] = input("""
The following things rely on the Contact table:
Custom Fields, Contact Actions, Opportunities, Credit Cards, Subscriptions,
and Orders
Skip Contacts? (Yn)"""
                                   ).lower() == 'n'
    config['TAGS'] = input('Tags? (Yn) ').lower() != 'n'
    config['LEAD_SOURCES'] = input('Lead Sources? (Yn) ').lower() != 'n'
    config['COMPANIES'] = input('Companies? (Yn) ').lower() != 'n'
    config['PRODUCTS'] = input('Products? (Yn) ').lower() != 'n'
    if config['CONTACTS']:
        config['CONTACT_CFS'] = input(
            'Contact Custom Fields? (Yn) ').lower() != 'n'
        config['CONTACT_ACTIONS'] = input(
            'Contact Actions? (Yn) ').lower() != 'n'
        config['OPPORTUNITIES'] = input('Opportunities? (Yn) ').lower() != 'n'
        config['CREDIT_CARDS'] = input('Credit Cards? (Yn) ').lower() != 'n'
    if config['CONTACTS'] and config['PRODUCTS'] and config['CREDIT_CARDS']:
        config['SUBSCRIPTIONS'] = input('Subscriptions? (Yn) ').lower() != 'n'
        config['ORDERS'] = input('Orders? (Yn) ').lower() != 'n'

    #######################
    # BEGIN DATA TRANSFER #
    #######################

    clear()
    print(f'Beginning transfer from {source_app} to {destination_app}')

    # Instantiate databases
    source = Database(source_app, source_port)
    destination = Database(destination_app, destination_port)

    # Returns boolean if dropdowns have been modified or not
    # This is so we can notify the user that they need to reload frontend
    dropdowns_modified = adt.transfer_dropdown_values(source, destination)

    # Disable receipt triggers before transfering payments
    adt.disable_receipt_settings(destination)

    # Make relationship directory if not exists
    package_dir = os.path.dirname(os.path.abspath(__file__))
    rel_dir = package_dir + '/relationships'
    os.makedirs(rel_dir, exist_ok=True)

    # -------- #
    # CONTACTS #
    # -------- #

    if config['CONTACTS']:
        if os.path.isfile(rel_dir + '/contact_rel.json'):
            with open(rel_dir + '/contact_rel.json') as file:
                contact_rel = json.load(file)
            contact_rel = {int(k): int(v) for k, v in contact_rel.items()}
        else:
            contact_rel = adt.transfer_contacts(
                source,
                destination,
                config['TAGS'],
                config['LEAD_SOURCES'],
                config['COMPANIES'],
            )
            with open(rel_dir + '/contact_rel.json', 'w') as file:
                json.dump(contact_rel, file)
        print('Contacts Transferred.')
        if config['TAGS']:
            print('Tags Transferred.')
        if config['LEAD_SOURCES']:
            print('Lead Sources Transferred.')
        if config['COMPANIES']:
            print('Companies Transferred.')

    # ---------------- #
    # TAG APPLICATIONS #
    # ---------------- #

    if config['TAGS']:
        adt.transfer_tag_applications(source, destination, contact_rel)
        print('Tag Applications Transferred.')

    # -------- #
    # PRODUCTS #
    # -------- #

    if config['PRODUCTS']:
        prod_rel, subplan_rel = adt.transfer_products(source, destination)
        print('Products Transferred.')

    # ------------- #
    # CUSTOM FIELDS #
    # ------------- #

    if config['CONTACT_CFS']:
        adt.transfer_custom_fields(source, destination, contact_rel)
        print('Custom Fields Transferred.')

    # --------------- #
    # CONTACT ACTIONS #
    # --------------- #

    if config['CONTACT_ACTIONS']:
        if os.path.isfile(rel_dir + '/action_rel.json'):
            with open(rel_dir + '/action_rel.json') as file:
                action_rel = json.load(file)
            action_rel = {int(k): int(v) for k, v in action_rel.items()}
        else:
            action_rel = adt.transfer_contact_actions(
                source, destination, contact_rel)
            with open(rel_dir + '/action_rel.json', 'w') as file:
                json.dump(action_rel, file)
        print('Contact Actions Transferred.')

    # ------------- #
    # OPPORTUNITIES #
    # ------------- #

    if config['OPPORTUNITIES']:
        if os.path.isfile(rel_dir + '/opp_rel.json'):
            with open(rel_dir + '/opp_rel.json') as file:
                opp_rel = json.load(file)
            opp_rel = {int(k): int(v) for k, v in opp_rel.items()}
        else:
            opp_rel = adt.transfer_opportunities(
                source,
                destination,
                contact_rel,
                prod_rel,
                subplan_rel
            )
            with open(rel_dir + '/opp_rel.json', 'w') as file:
                json.dump(opp_rel, file)
        print('Opportunities Transferred.')

    # ------------ #
    # CREDIT CARDS #
    # ------------ #

    if config['CREDIT_CARDS']:
        if os.path.isfile(rel_dir + '/cc_rel.json'):
            with open(rel_dir + '/cc_rel.json') as file:
                cc_rel = json.load(file)
            cc_rel = {int(k): int(v) for k, v in cc_rel.items()}
        else:
            cc_rel = adt.transfer_credit_cards(
                source.appname,
                destination,
                contact_rel
            )
            with open(rel_dir + '/cc_rel.json', 'w') as file:
                json.dump(cc_rel, file)
        cc_rel[0] = 0
        print('Credit Cards Transferred.')

    # ------------- #
    # SUBSCRIPTIONS #
    # ------------- #

    if config['SUBSCRIPTIONS']:
        if os.path.isfile(rel_dir + '/sub_rel.json'):
            with open(rel_dir + '/sub_rel.json') as file:
                sub_rel = json.load(file)
            sub_rel = {int(k): int(v) for k, v in sub_rel.items()}
        else:
            sub_rel = adt.transfer_subscriptions(
                source,
                destination,
                contact_rel,
                cc_rel,
                prod_rel,
                subplan_rel
            )
            with open(rel_dir + '/sub_rel.json', 'w') as file:
                json.dump(sub_rel, file)
        print('Subscriptions Transferred.')

    # ------ #
    # ORDERS #
    # ------ #

    if config['ORDERS']:
        if os.path.isfile(rel_dir + '/job_rel.json'):
            with open(rel_dir + '/job_rel.json') as file:
                job_rel = json.load(file)
            job_rel = {int(k): int(v) for k, v in job_rel.items()}
        else:
            job_rel = adt.transfer_orders(
                source,
                destination,
                contact_rel,
                prod_rel,
                cc_rel,
                subplan_rel
            )
            with open(rel_dir + '/job_rel.json', 'w') as file:
                json.dump(job_rel, file)
        print('Orders Transferred.')

    # ----------------- #
    # JOBTOJOBRECURRING #
    # ----------------- #

    if config['SUBSCRIPTIONS'] and config['ORDERS']:
        if os.path.isfile(rel_dir + '/jtjr_rel.json'):
            with open(rel_dir + '/jtjr_rel.json') as file:
                jtjr_rel = json.load(file)
            jtjr_rel = {int(k): int(v) for k, v in jtjr_rel.items()}
        else:
            jtjr_rel = adt.transfer_jobtojobrecurring(
                source,
                destination,
                job_rel,
                sub_rel
            )
            with open(rel_dir + '/jtjr_rel.json', 'w') as file:
                json.dump(jtjr_rel, file)

    ##########################
    # AFTER TRANSFER CLEANUP #
    ##########################

    # Close Database Connections
    source.close()
    destination.close()

    # Prompt for reload to properly display dropdown.
    if dropdowns_modified:
        print('==========IMPORTANT: Reload Frontend==========')


if __name__ == '__main__':
    main()
