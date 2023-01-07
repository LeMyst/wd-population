import csv
import logging
from datetime import datetime

from wikibaseintegrator import WikibaseIntegrator, wbi_fastrun, wbi_helpers, wbi_login
from wikibaseintegrator.datatypes import ExternalID, Item, Quantity, Time
from wikibaseintegrator.entities import ItemEntity
from wikibaseintegrator.wbi_config import config as wbi_config
from wikibaseintegrator.wbi_enums import ActionIfExists, WikibaseRank
from wikibaseintegrator.wbi_exceptions import MWApiError

# Import local config for user and password
import config

wbi_config['USER_AGENT'] = 'Update French Population'

# login object
login_instance = wbi_login.Login(user=config.user, password=config.password)

wbi = WikibaseIntegrator(login=login_instance, is_bot=True)

logging.basicConfig(level=logging.DEBUG)

base_filter = [
    Item(prop_nr='P31', value='Q36784'),  # instance of region of France
    Item(prop_nr='P17', value='Q142'),  # country France
    ExternalID(prop_nr='P2585')  # INSEE region code
]

print('Creating fastrun container')
frc = wbi_fastrun.get_fastrun_container(base_filter=base_filter)

skip_to_insee = 0

print('Start parsing CSV')
with open('donnees_regions.csv', newline='', encoding='utf-8') as csvfile:
    spamreader = csv.reader(csvfile, delimiter=';')
    for row in spamreader:
        if row[0].isnumeric():
            code_insee = row[0]
            if int(code_insee.replace('A', '0').replace('B', '0')) > skip_to_insee:
                population = int(row[5])  # PMUN

                qualifiers = [
                    Time(prop_nr='P585', time='+2020-01-01T00:00:00Z'),
                    Item(prop_nr='P459', value='Q39825')
                ]

                references = [
                    [
                        Item(value='Q115923391', prop_nr='P248')
                    ]
                ]

                # Search the Wikidata Element for this commune
                id_items = frc.get_items(claims=[ExternalID(prop_nr='P2585', value=str(code_insee))])

                if not id_items:
                    continue

                id_item = None
                final_items = id_items.copy()

                if not id_item and len(final_items) == 1:  # if only one item remains, we take it
                    id_item = final_items.pop()

                if id_item:
                    wb_item = ItemEntity(api=wbi).get(id_item)

                    # Check if a write is needed or not
                    write_needed = True
                    for claim in wb_item.claims.get('P1082'):
                        if claim.rank == WikibaseRank.PREFERRED:
                            for qualifier in claim.qualifiers.get('P585'):
                                if qualifier == qualifiers[0].mainsnak and claim.mainsnak.datavalue['value']['amount'] == wbi_helpers.format_amount(population):
                                    write_needed = False
                                    break
                            else:
                                continue
                            break

                    # Set the preferred rank of others claims to normal
                    for claim in wb_item.claims.get('P1082'):
                        claim.rank = WikibaseRank.NORMAL

                    # Create the claim to add with references, qualifiers and preferred rank
                    wb_item.claims.add(claims=Quantity(amount=population, prop_nr='P1082', references=references, qualifiers=qualifiers, rank=WikibaseRank.PREFERRED),
                                       action_if_exists=ActionIfExists.APPEND_OR_REPLACE)

                    if write_needed:
                        print(f'Write to Wikidata for {row[1]} ({row[0]})')
                        try:
                            print('write')
                            wb_item.write(summary='Update population for 2020')
                            exit(0)
                        except MWApiError as e:
                            print(e)
                    else:
                        print(f'Skipping {row[1]} ({row[0]})')
