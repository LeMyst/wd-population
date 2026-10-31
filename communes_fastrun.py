import csv
import json
import logging
import os
import time
from collections import Counter
from datetime import datetime

from wikibaseintegrator import wbi_fastrun
from wikibaseintegrator.datatypes import ExternalID, Item, Quantity, Time
from wikibaseintegrator.wbi_enums import ActionIfExists, WikibaseRank
from wikibaseintegrator.wbi_exceptions import MWApiError

from common import Bot, parse_wb_time, point_in_time, qualifiers, references, stated_in, year

bot = Bot('Update French communes population on Wikidata')
wbi = bot.wbi

is_latest = year == '2023'

# Date for geographic point in time (2 years later)
# Because population take into account the new communes created until 1st January of year+2 inclusive
geographic_point_in_time = f'+{str(int(year) + 2)}-01-01T00:00:00Z'

qualifiers_preferred = qualifiers + [Item(prop_nr='P7452', value='Q71533355')]


def snak_keys(snaks):
    """The snaks of the JSON of qualifiers or of a reference as (property, value) pairs, without their hashes."""
    return sorted((snak['property'], json.dumps([snak['snaktype'], snak.get('datavalue')], sort_keys=True)) for property_snaks in snaks.values() for snak in property_snaks)


def population_statements(item):
    """
    The population (P1082) statements of the item by id, to compare them before and after a change, without the hashes
    and the orders that change when a statement is serialized again.
    """
    statements = {}
    for claim in item.claims.get('P1082'):
        data = claim.get_json()
        statements[data.get('id')] = {
            'value': json.dumps(data['mainsnak'].get('datavalue'), sort_keys=True),
            'rank': data['rank'],
            'remove': 'remove' in data,
            'qualifiers': Counter(snak_keys(data.get('qualifiers', {}))),
            'references': Counter(tuple(snak_keys(reference['snaks'])) for reference in data.get('references', []))
        }
    return statements


def only_adds_p7452(before, after):
    """Whether the only change between the population statements before and after is P7452 added to existing ones."""
    if before == after or before.keys() != after.keys():
        return False
    for statement_id, statement in after.items():
        old = before[statement_id]
        if any(statement[key] != old[key] for key in ('value', 'rank', 'remove', 'references')):
            return False
        # No qualifier removed, and only P7452 added
        if old['qualifiers'] - statement['qualifiers'] or any(prop != 'P7452' for prop, _ in statement['qualifiers'] - old['qualifiers']):
            return False
    return True

base_filter = [
    Item(prop_nr='P31', value='Q484170'),  # instance of commune of France
    Item(prop_nr='P17', value='Q142'),  # country France
    ExternalID(prop_nr='P374')  # INSEE municipality code
]

print('Creating fastrun container')
frc = wbi_fastrun.get_fastrun_container(base_filter=base_filter, use_qualifiers=True, use_references=True, use_rank=is_latest, cache=True)

skip_to_insee = None

if not skip_to_insee:
    # Load last edited INSEE code from file
    try:
        with open(f'annees/{year}/last_edited.txt', 'r') as f:
            skip_to_insee = f.read().strip()
            logging.info(f'Skipping to INSEE code {skip_to_insee}')
    except FileNotFoundError:
        skip_to_insee = '0'

if year >= '2019':
    file_name = 'donnees_communes.csv'
else:
    file_name = 'Communes.csv'

print('Start parsing CSV')
with open(f'annees/{year}/{file_name}', newline='', encoding='utf-8') as csvfile:
    spamreader = csv.reader(csvfile, delimiter=';')
    start_time = time.time()
    for row in spamreader:
        id_item = None
        if row[0].isnumeric():
            code_insee = row[6]  # COM
            if code_insee > skip_to_insee:
                population = int(row[8])  # PMUN

                claims = [
                    ExternalID(prop_nr='P374', value=str(code_insee)),
                    Quantity(amount=population, prop_nr='P1082', references=references, qualifiers=qualifiers_preferred, rank=WikibaseRank.PREFERRED)
                ]

                entities = frc.get_entities(claims=claims, cache=True, query_limit=1000000)
                if not entities:
                    logging.info(f'No item found for {row[7]} ({row[2]}) {code_insee}')
                    continue

                write_required = frc.write_required(claims=claims, entity_filter=entities, property_filter='P1082', cache=True, query_limit=1000000)

                if len(entities) > 1:
                    write_required = True

                if write_required:
                    id_item = None
                    final_items = entities.copy()

                    if len(entities) > 1:
                        census = datetime.strptime(geographic_point_in_time, '+%Y-%m-%dT00:00:00Z')
                        for entity in entities:
                            logging.debug(f'entity: {entity}')
                            test_item = wbi.item.get(entity, props=['claims'])
                            should_remove = False

                            # Test if P576 exists, in this case remove the item from the list and continue
                            if 'P576' in test_item.claims:
                                dissolved_claims = test_item.claims.get('P576')
                                for dc in dissolved_claims:
                                    try:
                                        time_str = dc.mainsnak.datavalue.get('value', {}).get('time') if hasattr(dc.mainsnak, 'datavalue') else None
                                    except Exception:
                                        time_str = None
                                    d = parse_wb_time(time_str)
                                    if d and d < census:  # if dissolved before census, remove
                                        should_remove = True
                                        logging.debug(f'remove {entity} with P576 before census date ({d.isoformat()})')
                                        break

                            if not should_remove:
                                claims_p31 = test_item.claims.get('P31')  # instance of
                                commune_claims = [c for c in claims_p31 if c.mainsnak.datavalue['value']['id'] == 'Q484170']

                                if commune_claims:
                                    # Filter by rank: if any preferred claim exists, only consider preferred claims
                                    preferred_claims = [c for c in commune_claims if c.rank == WikibaseRank.PREFERRED]
                                    if preferred_claims:
                                        commune_claims = preferred_claims

                                    has_valid_commune_claim = False
                                    for claim in commune_claims:
                                        is_this_claim_valid = True
                                        if 'P580' in claim.qualifiers_order:
                                            stime_str = claim.qualifiers.get('P580')[0].datavalue['value']['time']
                                            d = parse_wb_time(stime_str)
                                            if d and d > census:
                                                is_this_claim_valid = False

                                        if is_this_claim_valid and 'P582' in claim.qualifiers_order:  # end time (P582)
                                            etime_str = claim.qualifiers.get('P582')[0].datavalue['value']['time']
                                            d = parse_wb_time(etime_str)
                                            if d and d < census:
                                                is_this_claim_valid = False

                                        if is_this_claim_valid:
                                            has_valid_commune_claim = True
                                            break

                                    if not has_valid_commune_claim:
                                        should_remove = True
                                        logging.info('No valid P31 Q484170 claim for census date, removing')

                            # Find the insee code and remove the ones with end date
                            if not should_remove:
                                insee_claims = test_item.claims.get('P374')
                                has_valid_insee = False
                                for insee_claim in insee_claims:
                                    logging.debug(f'insee_claim: {insee_claim.qualifiers_order}')
                                    logging.debug(f"insee_claim value: {insee_claim.mainsnak.datavalue['value']}")
                                    # Test if the insee value is the same as the one we are looking for
                                    if insee_claim.mainsnak.datavalue['value'] != code_insee:
                                        continue

                                    is_claim_valid = True
                                    if 'P582' in insee_claim.qualifiers_order:
                                        etime_str = insee_claim.qualifiers.get('P582')[0].datavalue['value']['time']
                                        d = parse_wb_time(etime_str)
                                        if d and d < census:
                                            logging.debug(f'claim for {code_insee} has end time before census')
                                            is_claim_valid = False

                                    if is_claim_valid:
                                        has_valid_insee = True
                                        if 'P580' in insee_claim.qualifiers_order and 'P582' not in insee_claim.qualifiers_order:
                                            stime_str = insee_claim.qualifiers.get('P580')[0].datavalue['value']['time']
                                            d = parse_wb_time(stime_str)

                                            if d and d >= census:
                                                logging.debug(f'found {entity} with start time ({d.isoformat()})')
                                                id_item = entity

                                if not has_valid_insee:
                                    logging.debug(f'remove {entity} with wrong insee code')
                                    should_remove = True

                            if should_remove:
                                if entity in final_items:
                                    final_items.remove(entity)

                                is_future = False
                                if 'P31' in test_item.claims:
                                    for claim in test_item.claims.get('P31'):
                                        if claim.mainsnak.datavalue['value']['id'] == 'Q484170':
                                            if 'P580' in claim.qualifiers_order:
                                                stime_str = claim.qualifiers.get('P580')[0].datavalue['value']['time']
                                                d = parse_wb_time(stime_str)
                                                if d and d > census:
                                                    is_future = True
                                                    break
                                if is_future:
                                    logging.debug(f'Skipping removal for {entity} because it starts in the future')
                                    continue

                                claims_to_remove = []
                                for claim in test_item.claims.get('P1082'):
                                    if 'P585' in claim.qualifiers_order:
                                        for q in claim.qualifiers.get('P585'):
                                            try:
                                                val = q.datavalue['value']['time']
                                                dt = parse_wb_time(val)
                                                if dt == census:
                                                    claims_to_remove.append(claim)
                                            except:
                                                pass
                                if claims_to_remove:
                                    logging.info(f'Removing {len(claims_to_remove)} wrong P1082 claims from {entity}')
                                    for c in claims_to_remove:
                                        c.remove()
                                    bot.write(test_item, summary='Remove wrong population for ' + year)

                    if not id_item and len(final_items) == 1:  # if only one item remains, we take it
                        id_item = final_items.pop()

                    if id_item:
                        real_write_required = frc.write_required(claims=claims, entity_filter=[id_item], property_filter='P1082', cache=True)
                        if real_write_required:
                            logging.info(f'Write to Wikidata for {row[7]} ({row[2]}) {code_insee} to {id_item}')
                            try:
                                logging.debug('write')
                                update_item = wbi.item.get(id_item, props=['claims'])
                                statements_before = population_statements(update_item)

                                current_census_date = parse_wb_time(point_in_time)
                                most_recent_date = None

                                # Check for most recent date in existing claims
                                for claim in update_item.claims.get('P1082'):
                                    if 'P585' in claim.qualifiers_order:
                                        for q in claim.qualifiers.get('P585'):
                                            try:
                                                qt = q.datavalue['value']['time']
                                                dt = parse_wb_time(qt)
                                                if dt:
                                                    if most_recent_date is None or dt > most_recent_date:
                                                        most_recent_date = dt
                                            except:
                                                pass

                                is_most_recent = True
                                if most_recent_date and current_census_date < most_recent_date:
                                    is_most_recent = False

                                for claim in update_item.claims.get('P1082'):
                                    if is_most_recent:
                                        claim.rank = WikibaseRank.NORMAL

                                        # Clean P7452 from previous preferred claims
                                        if 'P7452' in claim.qualifiers_order:
                                            claim.qualifiers.remove(qualifier=Item(prop_nr='P7452', value='Q71533355'))

                                    # Clean duplicate qualifiers
                                    if len(claim.qualifiers.get('P585')) > 1:
                                        claim.qualifiers.remove(qualifier=Time(prop_nr='P585', time=point_in_time))

                                    # Clean duplicate references
                                    if len(claim.references.references) > 1:
                                        claim.references.remove(reference_to_remove=Item(value=stated_in, prop_nr='P248'))

                                new_rank = WikibaseRank.PREFERRED if is_most_recent else WikibaseRank.NORMAL
                                # Without P7452, so that the claim of this census is updated instead of duplicated when it doesn't have P7452 yet
                                new_claim = Quantity(amount=population, prop_nr='P1082', references=references, qualifiers=qualifiers, rank=new_rank)

                                # Re-adding an existing claim with the normal rank keeps its current rank, set it explicitly
                                for claim in update_item.claims.get('P1082'):
                                    if claim == new_claim:
                                        claim.rank = new_rank

                                update_item.claims.add(claims=new_claim, action_if_exists=ActionIfExists.APPEND_OR_REPLACE)

                                if is_most_recent:
                                    census_claim = next(claim for claim in update_item.claims.get('P1082') if claim == new_claim)
                                    census_claim.qualifiers.add(Item(prop_nr='P7452', value='Q71533355'))  # reason for preferred rank: most recent value

                                # The population of this census is often already there and only lacks its reason for the preferred rank
                                if only_adds_p7452(statements_before, population_statements(update_item)):
                                    summary = 'Add reason for preferred rank to population for ' + year
                                else:
                                    summary = 'Update population for ' + year
                                bot.write(update_item, summary=summary)

                                # Save the latest edited edited entity insee code
                                if not bot.dry_run:
                                    with open(f'annees/{year}/last_edited.txt', 'w') as f:
                                        f.write(code_insee)
                            except MWApiError as e:
                                logging.debug(e)
                            # finally:
                            #     exit(0)
                        else:
                            logging.info(f'Write not required for {row[7]} ({row[2]}) to {id_item}')
                    else:
                        logging.info(f'Skipping {row[7]} ({row[2]}) for item {id_item}')
                        logging.debug(f'Final items: {final_items}')
                else:
                    logging.info(f'Write not required for {row[7]} ({row[2]})')

# Remove last edited file when done (kept on dry run to not lose the resume point of a real run)
if not bot.dry_run:
    try:
        os.remove(f'annees/{year}/last_edited.txt')
    except FileNotFoundError:
        pass

print("--- %s seconds ---" % (time.time() - start_time))
