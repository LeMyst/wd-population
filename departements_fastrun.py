import csv
import logging
import time
from datetime import datetime

from wikibaseintegrator import wbi_fastrun
from wikibaseintegrator.datatypes import ExternalID, Item, Quantity, Time
from wikibaseintegrator.wbi_enums import ActionIfExists, WikibaseRank
from wikibaseintegrator.wbi_exceptions import MWApiError

from common import Bot, point_in_time, qualifiers, references, stated_in, year

bot = Bot('Update French departments population on Wikidata')
wbi = bot.wbi

base_filter = [
    Item(prop_nr='P31', value='Q6465'),  # instance of department of France
    Item(prop_nr='P17', value='Q142'),  # country France
    ExternalID(prop_nr='P2586')  # INSEE department code
]

print('Creating fastrun container')
frc = wbi_fastrun.get_fastrun_container(base_filter=base_filter, use_qualifiers=True, use_references=True, use_rank=True, cache=True)

skip_to_insee = 0

print('Start parsing CSV')
with open(f'annees/{year}/donnees_departements.csv', newline='', encoding='utf-8') as csvfile:
    spamreader = csv.reader(csvfile, delimiter=';')
    start_time = time.time()
    for row in spamreader:
        id_item = None
        if row[0].isnumeric():
            code_insee = row[2]  # DEP
            if int(code_insee.replace('A', '0').replace('B', '0')) > skip_to_insee:
                population = int(row[7])  # PMUN

                claims = [
                    ExternalID(prop_nr='P2586', value=str(code_insee)),
                    Quantity(amount=population, prop_nr='P1082', references=references, qualifiers=qualifiers, rank=WikibaseRank.PREFERRED)
                ]

                entities = frc.get_entities(claims=claims, cache=True, query_limit=1000000)
                if not entities:
                    logging.info(f'No item found for {row[3]} ({row[2]}) {code_insee}')
                    continue

                write_required = frc.write_required(claims=claims, entity_filter=entities, property_filter='P1082', cache=True, query_limit=1000000)

                if write_required:
                    id_item = None
                    final_items = entities.copy()

                    if len(entities) > 1:
                        for entity in entities:
                            test_item = wbi.item.get(entity, props=['claims'])
                            claims = test_item.claims.get('P31')  # instance of
                            for claim in claims:
                                if claim.mainsnak.datavalue['value']['id'] == 'Q6465':  # department of France (Q6465)
                                    if 'P580' in claim.qualifiers_order and 'P582' not in claim.qualifiers_order:  # start time (P580) and end time (P582)
                                        d = datetime.strptime(claim.qualifiers.get('P580')[0].datavalue['value']['time'].replace('-00-00T', '-01-01T'), '+%Y-%m-%dT00:00:00Z')
                                        census = datetime.strptime(point_in_time, '+%Y-%m-%dT00:00:00Z')
                                        if d >= census:
                                            id_item = entity
                                            break
                                    if 'P582' in claim.qualifiers_order:  # end time (P582)
                                        final_items.remove(entity)  # If the item have an end time, we remove it from the list
                            else:
                                continue
                            break

                    if not id_item and len(final_items) == 1:  # if only one item remains, we take it
                        id_item = final_items.pop()

                    if id_item:
                        if row[3] == 'Paris':
                            logging.info('Skip Paris')
                            continue
                        logging.info(f'Write to Wikidata for {row[3]} ({row[2]}) {code_insee} to {id_item}')
                        try:
                            logging.debug('write')
                            update_item = wbi.item.get(id_item, props=['claims'])

                            for claim in update_item.claims.get('P1082'):
                                claim.rank = WikibaseRank.NORMAL

                                # Clean duplicate qualifiers
                                if len(claim.qualifiers.get('P585')) > 1:
                                    claim.qualifiers.remove(qualifier=Time(prop_nr='P585', time=point_in_time))

                                # Clean duplicate references
                                if len(claim.references.references) > 1:
                                    claim.references.remove(reference_to_remove=Item(value=stated_in, prop_nr='P248'))

                            update_item.claims.add(claims=Quantity(amount=population, prop_nr='P1082', references=references, qualifiers=qualifiers, rank=WikibaseRank.PREFERRED), action_if_exists=ActionIfExists.APPEND_OR_REPLACE)

                            bot.write(update_item, summary='Update population for ' + year)
                        except MWApiError as e:
                            logging.debug(e)
                        # finally:
                        #   exit(0)
                    else:
                        logging.info(f'Skipping {row[3]} ({row[2]}) for item {id_item}')
                else:
                    logging.info('Write not required')

print("--- %s seconds ---" % (time.time() - start_time))
