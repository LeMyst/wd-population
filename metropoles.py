import csv
import logging
from datetime import datetime
from importlib.machinery import SourceFileLoader

from wikibaseintegrator import WikibaseIntegrator, wbi_fastrun, wbi_login
from wikibaseintegrator.datatypes import ExternalID, Item, Quantity, Time
from wikibaseintegrator.wbi_config import config as wbi_config
from wikibaseintegrator.wbi_enums import ActionIfExists, EntityField, WikibaseRank
from wikibaseintegrator.wbi_exceptions import MWApiError

import config

wbi_config['USER_AGENT'] = 'WikibaseIntegrator/1.0 Update French Population'

login_instance = wbi_login.Login(user=config.user, password=config.password)
wbi = WikibaseIntegrator(login=login_instance, is_bot=True)

logging.basicConfig(level=logging.DEBUG)

year = str(config.year)
is_latest = year == '2023'

# Date of the census
point_in_time = f'+{year}-01-01T00:00:00Z'
geographic_point_in_time = f'+{str(int(year) + 2)}-01-01T00:00:00Z'

qualifiers = [
    Time(prop_nr='P585', time=point_in_time),
    Item(prop_nr='P459', value='Q39825')  # method: census
]

stated_in = SourceFileLoader('stated_in', 'annees/' + year + '/config.py').load_module().stated_in

references = [
    [Item(value=stated_in, prop_nr='P248')]
]

# Definition of the Metropoles and their Arrondissements ranges
targets = {
    '75056': {'name': 'Paris', 'range': range(75101, 75121), 'population': 0},
    '69123': {'name': 'Lyon', 'range': range(69381, 69390), 'population': 0},
    '13055': {'name': 'Marseille', 'range': range(13201, 13217), 'population': 0}
}

base_filter = [
    Item(prop_nr='P17', value='Q142'),  # country -> France
    ExternalID(prop_nr='P374')  # INSEE municipality code
]


def parse_wb_time(time_str):
    if not time_str:
        return None
    normalized = time_str.replace('-00-00T', '-01-01T')
    try:
        return datetime.strptime(normalized, '+%Y-%m-%dT%H:%M:%SZ')
    except Exception:
        return None


print('Creating fastrun container')
frc = wbi_fastrun.get_fastrun_container(base_filter=base_filter, use_qualifiers=True, use_references=True, use_rank=is_latest, cache=True)

if year >= '2019':
    file_name = 'donnees_communes.csv'
else:
    file_name = 'Communes.csv'

print(f'Start parsing CSV: annees/{year}/{file_name}')
with open(f'annees/{year}/{file_name}', newline='', encoding='utf-8') as csvfile:
    spamreader = csv.reader(csvfile, delimiter=';')
    next(spamreader)  # Skip header if present, though commune_fastrun checks row[0].isnumeric()

    for row in spamreader:
        if len(row) > 6 and row[0].isnumeric():
            code_insee = row[6]  # COM
            try:
                val_insee = int(code_insee.replace('A', '0').replace('B', '0'))
                population = int(row[8])  # PMUN
            except ValueError:
                continue

            for target_insee, data in targets.items():
                if val_insee in data['range']:
                    data['population'] += population

# Process updates
for code_insee, data in targets.items():
    population = data['population']
    name = data['name']

    if population == 0:
        logging.warning(f'No population found for {name} ({code_insee})')
        continue

    logging.info(f'Processing {name} ({code_insee}): {population}')

    claims = [
        ExternalID(prop_nr='P374', value=code_insee),
        Quantity(amount=population, prop_nr='P1082', references=references, qualifiers=qualifiers, rank=WikibaseRank.PREFERRED)
    ]

    entities = frc.get_entities(claims=claims, cache=True)
    if not entities:
        logging.info(f'No item found for {name} ({code_insee})')
        continue

    write_required = frc.write_required(claims=claims, entity_filter=entities, property_filter='P1082', cache=True)

    if write_required:
        id_item = None
        final_items = entities.copy()

        if len(entities) > 1:
            # Logic from communes_fastrun to pick the right item
            census = datetime.strptime(geographic_point_in_time, '+%Y-%m-%dT00:00:00Z')
            for entity in entities:
                test_item = wbi.item.get(entity, props=['claims'])
                should_remove = False

                if 'P576' in test_item.claims:
                    dissolved_claims = test_item.claims.get('P576')
                    for dc in dissolved_claims:
                        time_str = dc.mainsnak.datavalue.get('value', {}).get('time') if hasattr(dc.mainsnak, 'datavalue') else None
                        d = parse_wb_time(time_str)
                        if d and d < census:
                            should_remove = True
                            break

                if not should_remove:
                    insee_claims = test_item.claims.get('P374')
                    for insee_claim in insee_claims:
                        if insee_claim.mainsnak.datavalue['value'] != code_insee:
                            should_remove = True
                            break
                        if 'P582' in insee_claim.qualifiers_order:
                            etime_str = insee_claim.qualifiers.get('P582')[0].datavalue['value']['time']
                            d = parse_wb_time(etime_str)
                            if d and d < census:
                                should_remove = True
                                break

                if should_remove and entity in final_items:
                    final_items.remove(entity)

        if len(final_items) == 1:
            id_item = final_items[0]
        else:
            logging.warning(f'Could not determine unique item for {name} ({code_insee}). Candidates: {final_items}')
            # For PLM, we can probably hardcode or fallback.
            # Paris Q90, Lyon Q456, Marseille Q23485.
            # But let's trust the logic first.
            if len(final_items) > 1:
                # fallback to simple "is not dissolved" logic is mostly done above.
                pass

        if id_item:
            try:
                update_item = wbi.item.get(id_item, props=['claims'])

                # Logic to determine rank and cleanup
                current_census_date = parse_wb_time(point_in_time)
                most_recent_date = None

                for claim in update_item.claims.get('P1082'):
                    if 'P585' in claim.qualifiers_order:
                        for q in claim.qualifiers.get('P585'):
                            qt = q.datavalue['value']['time']
                            dt = parse_wb_time(qt)
                            if dt:
                                if most_recent_date is None or dt > most_recent_date:
                                    most_recent_date = dt

                is_most_recent = True
                if most_recent_date and current_census_date < most_recent_date:
                    is_most_recent = False

                for claim in update_item.claims.get('P1082'):
                    if is_most_recent:
                        claim.rank = WikibaseRank.NORMAL
                    if len(claim.qualifiers.get('P585')) > 1:
                        claim.qualifiers.remove(qualifier=Time(prop_nr='P585', time=point_in_time))
                    if len(claim.references.references) > 1:
                        claim.references.remove(reference_to_remove=Item(value=stated_in, prop_nr='P248'))

                new_rank = WikibaseRank.PREFERRED if is_most_recent else WikibaseRank.NORMAL
                update_item.claims.add(claims=Quantity(amount=population, prop_nr='P1082', references=references, qualifiers=qualifiers, rank=new_rank), action_if_exists=ActionIfExists.APPEND_OR_REPLACE)

                update_item.write(summary='Update population (metropole) for ' + year, limit_claims=['P1082'], fields_to_update=EntityField.CLAIMS)
                logging.info(f'Successfully updated {name} ({id_item})')
            except MWApiError as e:
                logging.error(f'Error updating {name}: {e}')
    else:
        logging.info(f'No update required for {name}')

print('Done.')
