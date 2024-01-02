import csv
import logging
import time

from wikibaseintegrator import WikibaseIntegrator, wbi_fastrun, wbi_login
from wikibaseintegrator.datatypes import ExternalID, Item, Quantity, Time
from wikibaseintegrator.entities import ItemEntity
from wikibaseintegrator.wbi_config import config as wbi_config
from wikibaseintegrator.wbi_enums import WikibaseRank
from wikibaseintegrator.wbi_exceptions import MWApiError

# Import local config for user and password
import config

wbi_config['USER_AGENT'] = 'Update French Population'

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
    Item(prop_nr='P31', value='Q6465'),  # instance of department of France
    Item(prop_nr='P17', value='Q142'),  # country France
    ExternalID(prop_nr='P2586')  # INSEE department code
]

print('Creating fastrun container')
frc = wbi_fastrun.get_fastrun_container(base_filter=base_filter, use_qualifiers=False, use_references=False)

skip_to_insee = 0

print('Start parsing CSV')
with open('annees/' + config.year + '/donnees_departements.csv', newline='', encoding='utf-8') as csvfile:
    spamreader = csv.reader(csvfile, delimiter=';')
    start_time = time.time()
    for row in spamreader:
        if row[0].isnumeric():
            code_insee = row[2]
            if int(code_insee.replace('A', '0').replace('B', '0')) > skip_to_insee:
                population = int(row[7])  # PMUN

                item = ItemEntity()
                item.claims.add(claims=[ExternalID(prop_nr='P2586', value=str(code_insee)),
                                        Quantity(amount=population, prop_nr='P1082', references=references, qualifiers=qualifiers, rank=WikibaseRank.PREFERRED)])

                write_required = frc.write_required(entity=item, cache=True)

                if write_required and code_insee in frc.get_entities(claims=item.claims, cache=True):
                    logging.info(f'Write to Wikidata for {row[6]} ({row[2]}) {code_insee}')
                    try:
                        item.write(summary='Update population for ' + config.year)
                    except MWApiError as e:
                        print(e)
                    finally:
                        exit(0)
                else:
                    logging.info(f'Skipping {row[6]} ({row[2]})')

print("--- %s seconds ---" % (time.time() - start_time))
