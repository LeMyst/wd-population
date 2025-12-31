import csv
import logging
import time
from datetime import datetime

from wikibaseintegrator import WikibaseIntegrator, wbi_fastrun, wbi_login
from wikibaseintegrator.datatypes import ExternalID, Item, Quantity, Time
from wikibaseintegrator.wbi_config import config as wbi_config
from wikibaseintegrator.wbi_enums import ActionIfExists, EntityField, WikibaseRank
from wikibaseintegrator.wbi_exceptions import MWApiError

# Import local config for user and password
import config

wbi_config['USER_AGENT'] = 'WikibaseIntegrator/1.0 Update French Population'

# login object
login_instance = wbi_login.Login(user=config.user, password=config.password)

wbi = WikibaseIntegrator(login=login_instance, is_bot=True)

logging.basicConfig(level=logging.DEBUG)

qualifiers = [
    Time(prop_nr='P585', time=config.point_in_time),  # point in time
    Item(prop_nr='P459', value='Q39825')  # determination method: census
]

references = [
    [
        Item(value=config.stated_in, prop_nr='P248')  # stated in: Populations légales 2020
    ]
]

base_filter = [
    Item(prop_nr='P31', value='Q484170'),  # instance of commune of France
    Item(prop_nr='P17', value='Q142'),  # country France
    ExternalID(prop_nr='P374')  # INSEE municipality code
]

def parse_wb_time(time_str):
    """Normalize and parse Wikibase time strings like '+2020-00-00T00:00:00Z'."""
    if not time_str:
        return None
    normalized = time_str.replace('-00-00T', '-01-01T')
    try:
        return datetime.strptime(normalized, '+%Y-%m-%dT%H:%M:%SZ')
    except Exception:
        return None

print('Creating fastrun container')
frc = wbi_fastrun.get_fastrun_container(base_filter=base_filter, use_qualifiers=True, use_references=True, use_rank=True, cache=True)

skip_to_insee = 0

print('Start parsing CSV')
with open('annees/' + config.year + '/donnees_communes.csv', newline='', encoding='utf-8') as csvfile:
    spamreader = csv.reader(csvfile, delimiter=';')
    start_time = time.time()
    for row in spamreader:
        id_item = None
        if row[0].isnumeric():
            code_insee = row[6]  # COM
            if int(code_insee.replace('A', '0').replace('B', '0')) > skip_to_insee:
                population = int(row[8])  # PMUN

                claims = [
                    ExternalID(prop_nr='P374', value=str(code_insee)),
                    Quantity(amount=population, prop_nr='P1082', references=references, qualifiers=qualifiers, rank=WikibaseRank.PREFERRED)
                ]

                entities = frc.get_entities(claims=claims, cache=True, query_limit=1000000)
                if not entities:
                    logging.info(f'No item found for {row[7]} ({row[2]}) {code_insee}')
                    continue

                write_required = frc.write_required(claims=claims, entity_filter=entities, property_filter='P1082', cache=True, query_limit=1000000)

                if write_required:
                    id_item = None
                    final_items = entities.copy()

                    if len(entities) > 1:
                        census = datetime.strptime(config.point_in_time, '+%Y-%m-%dT00:00:00Z')
                        for entity in entities:
                            test_item = wbi.item.get(entity, props=['claims'])

                            # Test if P576 exists, in this case remove the item from the list and continue
                            if 'P576' in test_item.claims:
                                dissolved_claims = test_item.claims.get('P576')
                                removed_by_dissolution = False
                                for dc in dissolved_claims:
                                    try:
                                        time_str = dc.mainsnak.datavalue.get('value', {}).get('time') if hasattr(dc.mainsnak, 'datavalue') else None
                                    except Exception:
                                        time_str = None
                                    d = parse_wb_time(time_str)
                                    if d and d < census:  # if dissolved before census, remove
                                        if entity in final_items:
                                            final_items.remove(entity)
                                        removed_by_dissolution = True
                                        logging.debug(f'remove {entity} with P576 before census date ({d.isoformat()})')
                                        break
                                if removed_by_dissolution:
                                    continue

                            if len(final_items) == 1:
                                break

                            claims = test_item.claims.get('P31')  # instance of
                            for claim in claims:
                                if claim.mainsnak.datavalue['value']['id'] == 'Q484170':  # commune of France (Q484170)
                                    if 'P580' in claim.qualifiers_order and 'P582' not in claim.qualifiers_order:
                                        stime_str = claim.qualifiers.get('P580')[0].datavalue['value']['time']
                                        d = parse_wb_time(stime_str)
                                        if d and d >= census:
                                            final_items.remove(entity)  # start time after census -> remove
                                            logging.info('start time is after census date, removing')
                                            continue
                                    if 'P582' in claim.qualifiers_order:  # end time (P582)
                                        final_items.remove(entity)  # If the item have an end time, we remove it from the list
                                        logging.info('end time found, removing')
                                        continue

                            if len(final_items) == 1:
                                break

                            # Find the insee code and remove the ones with end date
                            insee_claims = test_item.claims.get('P374')
                            for insee_claim in insee_claims:
                                logging.debug(f'insee_claim: {insee_claim.qualifiers_order}')
                                logging.debug(f"insee_claim value: {insee_claim.mainsnak.datavalue['value']}")
                                # Test if the insee value is the same as the one we are looking for
                                if insee_claim.mainsnak.datavalue['value'] != code_insee:
                                    logging.debug(f'remove {entity} with wrong insee code')
                                    final_items.remove(entity)
                                    continue
                                if 'P580' in insee_claim.qualifiers_order and 'P582' not in insee_claim.qualifiers_order:
                                    stime_str = insee_claim.qualifiers.get('P580')[0].datavalue['value']['time']
                                    d = parse_wb_time(stime_str)
                                    if d and d >= census:
                                        logging.debug(f'found {entity} with start time ({d.isoformat()})')
                                        id_item = entity
                                        continue
                                if 'P582' in insee_claim.qualifiers_order:
                                    logging.debug(f'remove {entity} with end time')
                                    if entity in final_items:
                                        final_items.remove(entity)

                    if not id_item and len(final_items) == 1:  # if only one item remains, we take it
                        id_item = final_items.pop()

                    if id_item:
                        logging.info(f'Write to Wikidata for {row[7]} ({row[2]}) {code_insee} to {id_item}')
                        try:
                            logging.debug('write')
                            update_item = wbi.item.get(id_item, props=['claims'])

                            for claim in update_item.claims.get('P1082'):
                                claim.rank = WikibaseRank.NORMAL

                                # Clean duplicate qualifiers
                                if len(claim.qualifiers.get('P585')) > 1:
                                    claim.qualifiers.remove(qualifier=Time(prop_nr='P585', time=config.point_in_time))

                                # Clean duplicate references
                                if len(claim.references.references) > 1:
                                    claim.references.remove(reference_to_remove=Item(value=config.stated_in, prop_nr='P248'))

                            update_item.claims.add(claims=Quantity(amount=population, prop_nr='P1082', references=references, qualifiers=qualifiers, rank=WikibaseRank.PREFERRED), action_if_exists=ActionIfExists.APPEND_OR_REPLACE)

                            update_item.write(summary='Update population for ' + config.year, limit_claims=['P1082'], fields_to_update=EntityField.CLAIMS)
                        except MWApiError as e:
                            logging.debug(e)
                        finally:
                            exit(0)
                    else:
                        logging.info(f'Skipping {row[7]} ({row[2]}) for item {id_item}')
                        logging.debug(f'Final items: {final_items}')
                else:
                    logging.info(f'Write not required for {row[7]} ({row[2]})')

print("--- %s seconds ---" % (time.time() - start_time))
