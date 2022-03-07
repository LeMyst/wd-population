import csv
from datetime import datetime

from wikibaseintegrator import (WikibaseIntegrator, wbi_fastrun, wbi_helpers, wbi_login)
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

wbi = WikibaseIntegrator(login=login_instance)

# logging.basicConfig(level=logging.DEBUG)

base_filter = [
    Item(prop_nr='P31', value='Q484170'),  # instance of commune of France
    Item(prop_nr='P17', value='Q142'),  # country France
    ExternalID(prop_nr='P374')  # INSEE municipality code
]

print('Creating fastrun container')
frc = wbi_fastrun.get_fastrun_container(base_filter=base_filter)

skip_to_insee = 24167

print('Start parsing CSV')
with open('donnees_communes.csv', newline='', encoding='utf-8') as csvfile:
    spamreader = csv.reader(csvfile, delimiter=';')
    for row in spamreader:
        if row[0].isnumeric():
            code_insee = int(row[2][0:2] + row[5])
            if code_insee > skip_to_insee:
                population = int(row[7])

                qualifiers = [
                    Time(prop_nr='P585', time='+2019-01-01T00:00:00Z'),
                    Item(prop_nr='P459', value='Q39825')
                ]

                references = [
                    [
                        Item(value='Q110382235', prop_nr='P248')
                    ]
                ]

                # Search the Wikidata Element for this commune
                id_items = frc.get_items(claims=[ExternalID(prop_nr='P374', value=str(code_insee))])

                id_item = None
                if not id_items:
                    continue
                elif not len(id_items) == 1:
                    for item in id_items:
                        test_item = ItemEntity(api=wbi).get(item)
                        claims = test_item.claims.get('P31')
                        for claim in claims:
                            if claim.mainsnak.datavalue['value']['id'] == 'Q484170':
                                if 'P580' in claim.qualifiers_order and 'P582' not in claim.qualifiers_order:
                                    d = datetime.strptime(claim.qualifiers.get('P580')[0].datavalue['value']['time'], '+%Y-%m-%dT00:00:00Z')
                                    census = datetime.strptime('+2019-01-01T00:00:00Z', '+%Y-%m-%dT00:00:00Z')
                                    if d.time() >= census.time():
                                        id_item = item
                                        break
                        else:
                            continue
                        break

                else:
                    id_item = id_items.pop()

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
                                       action_if_exists=ActionIfExists.APPEND)

                    if write_needed:
                        print(f'Write to Wikidata for {row[6]} ({row[2]})')
                        try:
                            wb_item.write(summary='Update population for 2019')
                        except MWApiError as e:
                            print(e)
                    else:
                        print(f'Skipping {row[6]} ({row[2]})')
