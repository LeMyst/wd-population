"""
Shared logic of the population update scripts: census year configuration, command line options, Wikidata login, choice
of the item holding each INSEE code, update of its population statements, and writes (dry run, EditGroups batch,
constraint checks, resume point).
"""
import argparse
import csv
import json
import logging
import runpy
import sys
import time
from collections import Counter
from datetime import datetime
from decimal import Decimal
from pathlib import Path

from wikibaseintegrator import WikibaseIntegrator, wbi_fastrun, wbi_helpers, wbi_login
from wikibaseintegrator.datatypes import ExternalID, Item, Quantity, Time
from wikibaseintegrator.entities import ItemEntity
from wikibaseintegrator.wbi_config import config as wbi_config
from wikibaseintegrator.wbi_editgroups import EditGroup
from wikibaseintegrator.wbi_enums import ActionIfExists, EntityField, WikibaseRank
from wikibaseintegrator.wbi_exceptions import MWApiError

# Import local config for user, password and year
import config

wbi_config['USER_AGENT'] = 'WikibaseIntegrator/1.0 Update French Population'

# A run lasts hours: wait out a short outage of Wikidata or of the query service instead of stopping. The failed
# requests (connection errors, HTTP 5xx) are tried 10 times, waiting 1, 2, 4... seconds, 4 to 8 minutes in all, instead
# of 5 times (15 seconds) by default since WikibaseIntegrator 0.14.
wbi_config['BACKOFF_MAX_TRIES'] = 10

# Wait for Wikidata as long as it asks the bots to slow down (maxlag, rate limiting, read-only mode): by default since
# WikibaseIntegrator 0.14, a read stops the run after 20 tries, about 16 minutes of maxlag. The failures (connection
# errors, HTTP 5xx) stay limited by BACKOFF_MAX_TRIES.
MAX_RETRIES = 1000

# Load the statements of the whole country in one query
QUERY_LIMIT = 1000000

logging.basicConfig(level=logging.DEBUG)

year = str(config.year)

# Date of the census
point_in_time = f'+{year}-01-01T00:00:00Z'
census_date = datetime(int(year), 1, 1)

# The legal populations of a census are computed on the geography of the 1st January two years later: the population of
# a code goes to the item holding it on this date, a commune created on this date included
geographic_date = datetime(int(year) + 2, 1, 1)

# The census is the latest one when no later year has its folder in annees/
is_latest = year == max(path.name for path in Path('annees').iterdir() if path.name.isdigit())

# Load year specific config
stated_in = runpy.run_path(f'annees/{year}/config.py')['stated_in']

qualifiers = [
    Time(prop_nr='P585', time=point_in_time),  # point in time
    Item(prop_nr='P459', value='Q39825')  # determination method: census
]

references = [
    [
        Item(value=stated_in, prop_nr='P248')  # stated in: Populations légales of the year
    ]
]

# Reason for the preferred rank of the population of the most recent census
most_recent_value = Item(prop_nr='P7452', value='Q71533355')  # reason for preferred rank: most recent value


def csv_rows(file_name):
    """The rows of a CSV file of the census year, without its header."""
    with open(f'annees/{year}/{file_name}', newline='', encoding='utf-8') as csvfile:
        return [row for row in csv.reader(csvfile, delimiter=';') if row and row[0].isnumeric()]


def parse_wb_time(time_str):
    """Normalize and parse Wikibase time strings like '+2020-00-00T00:00:00Z' (year precision) or '+2020-05-00T00:00:00Z'."""
    if not time_str:
        return None
    normalized = time_str.replace('-00-00T', '-01-01T').replace('-00T', '-01T')
    try:
        return datetime.strptime(normalized, '+%Y-%m-%dT%H:%M:%SZ')
    except ValueError:
        return None


def snak_date(snak):
    """The date of a time snak, main snak or qualifier, or None for an unknown value or no value."""
    return parse_wb_time(snak.datavalue.get('value', {}).get('time'))


def qualifier_dates(claim, prop_nr):
    """The dates of the qualifiers of a property of a statement, without the unknown values."""
    return [date for date in map(snak_date, claim.qualifiers.get(prop_nr)) if date]


def valid_at(claim, date):
    """Whether a statement is valid on the date: not started (P580) after it, nor ended (P582) before it."""
    return all(start <= date for start in qualifier_dates(claim, 'P580')) and all(end >= date for end in qualifier_dates(claim, 'P582'))


def type_statements(item, instance_of):
    """The instance of (P31) statements of the type, only the preferred ones if there are some."""
    claims = [claim for claim in item.claims.get('P31') if claim.mainsnak.datavalue.get('value', {}).get('id') == instance_of]
    return [claim for claim in claims if claim.rank == WikibaseRank.PREFERRED] or claims


def code_statements(item, code_property, code):
    """The statements of the code valid on the geographic date of the census."""
    return [claim for claim in item.claims.get(code_property) if claim.mainsnak.datavalue.get('value') == code and valid_at(claim, geographic_date)]


def rejection(item, code_property, code, instance_of):
    """
    Why the item didn't hold the code on the geographic date of the census, or None if it did: dissolved (P576) before
    this date, not an instance of the type on this date, or without a statement of the code valid on this date.
    """
    for claim in item.claims.get('P576'):  # dissolved, abolished or demolished date
        dissolved = snak_date(claim.mainsnak)
        if dissolved and dissolved < geographic_date:
            return f'dissolved on {dissolved:%Y-%m-%d}'

    type_claims = type_statements(item, instance_of) if instance_of else []
    if type_claims and not any(valid_at(claim, geographic_date) for claim in type_claims):
        return f'not an instance of {instance_of} on {geographic_date:%Y-%m-%d}'

    if not code_statements(item, code_property, code):
        return f'no valid {code_property} statement on {geographic_date:%Y-%m-%d}'

    return None


def takes_code_on_geographic_date(item, code_property, code):
    """Whether the code of the item starts on the geographic date of the census, like a new commune taking the code of a merged one."""
    return any(start >= geographic_date for claim in code_statements(item, code_property, code) if not claim.qualifiers.get('P582') for start in qualifier_dates(claim, 'P580'))


def starts_after_geographic_date(item, instance_of):
    """Whether the item becomes an instance of the type after the geographic date of the census, like a commune created later."""
    return any(start > geographic_date for claim in type_statements(item, instance_of) for start in qualifier_dates(claim, 'P580'))


def expected_statement(population):
    """
    The population statement of the census expected on the item by the fastrun check: preferred, with the reason "most
    recent value", for the latest census. The rank of an older census is not checked, it depends on the later
    populations of the item.
    """
    if is_latest:
        return Quantity(amount=population, prop_nr='P1082', references=references, qualifiers=qualifiers + [most_recent_value], rank=WikibaseRank.PREFERRED)
    return Quantity(amount=population, prop_nr='P1082', references=references, qualifiers=qualifiers)


def is_census_statement(claim, population):
    """Whether the population statement is the one the bot writes for the census: same population, point in time and source."""
    amount = claim.mainsnak.datavalue.get('value', {}).get('amount')
    return (amount is not None and Decimal(amount) == population and census_date in qualifier_dates(claim, 'P585')
            and any(snak.datavalue.get('value', {}).get('id') == stated_in for reference in claim.references for snak in reference.snaks.get('P248')))


def has_most_recent_reason(claim):
    return any(snak.datavalue.get('value', {}).get('id') == most_recent_value.mainsnak.datavalue['value']['id'] for snak in claim.qualifiers.get('P7452'))


def set_most_recent_reason(claim, present):
    """Add or remove the reason for the preferred rank "most recent value" (P7452) of a statement."""
    if present and not has_most_recent_reason(claim):
        claim.qualifiers.add(most_recent_value)
    elif not present and has_most_recent_reason(claim):
        claim.qualifiers.remove(qualifier=most_recent_value)

    # Keep the qualifiers order in line with the qualifiers (an empty order is filled by Wikibase)
    if claim.qualifiers_order:
        properties = list(claim.qualifiers.get_json())
        order = [prop for prop in claim.qualifiers_order if prop in properties]
        claim.qualifiers_order = order + [prop for prop in properties if prop not in order]


def promote_most_recent(claims):
    """
    When no population statement is preferred, make the one with the most recent point in time preferred, with the
    reason "most recent value", so that the population of the item stays the most recent one.
    """
    claims = [claim for claim in claims if not claim.removed and claim.rank != WikibaseRank.DEPRECATED]
    if any(claim.rank == WikibaseRank.PREFERRED for claim in claims):
        return

    dated = [(max(qualifier_dates(claim, 'P585')), claim) for claim in claims if qualifier_dates(claim, 'P585')]
    if not dated:
        return

    latest = max(date for date, _ in dated)
    latest_claims = [claim for date, claim in dated if date == latest]
    if len(latest_claims) > 1:
        logging.warning(f'No population statement preferred, and {len(latest_claims)} statements for the most recent date {latest:%Y-%m-%d}')
        return

    latest_claims[0].rank = WikibaseRank.PREFERRED
    set_most_recent_reason(latest_claims[0], True)


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


def update_population(item, population):
    """
    Add or update the population statement of the census on the item, and set the ranks: the statement of the most
    recent census is preferred, with the reason "most recent value" (P7452), the others normal. The deprecated statements
    are left as they are.

    :return: The summary of the edit, or None if the statements don't change.
    """
    statements_before = population_statements(item)
    claims = item.claims.get('P1082')
    census_claims = [claim for claim in claims if claim == Quantity(amount=population, prop_nr='P1082', qualifiers=qualifiers)]
    if any(claim.rank == WikibaseRank.DEPRECATED for claim in census_claims):
        logging.warning(f'The population {population} of {year} is deprecated on {item.id}, left as it is')
        return None

    ranked_claims = [claim for claim in claims if claim.rank != WikibaseRank.DEPRECATED]
    is_most_recent = all(date <= census_date for claim in ranked_claims for date in qualifier_dates(claim, 'P585'))

    for claim in claims:
        if is_most_recent and claim.rank != WikibaseRank.DEPRECATED:
            if claim.rank == WikibaseRank.PREFERRED:
                claim.rank = WikibaseRank.NORMAL
            set_most_recent_reason(claim, False)

        # Clean duplicate qualifiers
        if len(claim.qualifiers.get('P585')) > 1:
            claim.qualifiers.remove(qualifier=Time(prop_nr='P585', time=point_in_time))

        # Clean duplicate references
        if len(claim.references.references) > 1:
            claim.references.remove(reference_to_remove=Item(value=stated_in, prop_nr='P248'))

    new_rank = WikibaseRank.PREFERRED if is_most_recent else WikibaseRank.NORMAL
    # Without P7452, so that the statement of this census is updated instead of duplicated when it doesn't have P7452 yet
    new_claim = Quantity(amount=population, prop_nr='P1082', references=references, qualifiers=qualifiers, rank=new_rank)
    claim_with_reason = Quantity(amount=population, prop_nr='P1082', qualifiers=qualifiers + [most_recent_value])

    for claim in claims:
        # A statement of this census with P7452 while it isn't the most recent one wouldn't be equal to the new one
        if claim == claim_with_reason:
            set_most_recent_reason(claim, False)
        # Re-adding an existing statement with the normal rank keeps its current rank, set it explicitly
        if claim == new_claim:
            claim.rank = new_rank

    item.claims.add(claims=new_claim, action_if_exists=ActionIfExists.APPEND_OR_REPLACE)

    census_claim = next(claim for claim in item.claims.get('P1082') if claim == new_claim)
    set_most_recent_reason(census_claim, is_most_recent)
    promote_most_recent(item.claims.get('P1082'))

    statements_after = population_statements(item)
    if statements_after == statements_before:
        return None
    # The population of this census is often already there and only lacks its reason for the preferred rank
    if only_adds_p7452(statements_before, statements_after):
        return 'Add reason for preferred rank to population for ' + year
    return 'Update population for ' + year


def remove_census_statements(item, population):
    """
    Remove the population statements of the census written by the bot on an item that didn't hold the code at the
    census, and make the most recent remaining statement preferred if the removed one was.

    :return: The number of removed statements
    """
    census_claims = [claim for claim in item.claims.get('P1082') if is_census_statement(claim, population)]
    for claim in census_claims:
        claim.remove()
    promote_most_recent(item.claims.get('P1082'))
    return len(census_claims)


class Bot:
    """
    Update the population of the items of a type from the INSEE codes of a CSV: read the command line options, login to
    Wikidata, choose the item holding each code, update its population statements and write them.
    """

    def __init__(self, description, code_property, instance_of=None, fastrun=True):
        """
        :param description: The description of the script, for --help
        :param code_property: The property of the INSEE code, e.g. P374 for the communes
        :param instance_of: The type of the items, e.g. Q484170 (commune of France), or None to not check it
        :param fastrun: Check the population of every item at once with fastrun. For a few items, False finds each one
            by its code instead, and only reads the items.
        """
        parser = argparse.ArgumentParser(description=description)
        parser.add_argument('--dry-run', action='store_true', help='do not write anything to Wikidata, only log what would be written')
        parser.add_argument('--check-constraints', action='store_true', help='log the constraint violations of the population statements after each write')
        parser.add_argument('--max-edits', type=int, metavar='N', help='stop after N edits, to check a few of them before a whole run')
        args = parser.parse_args()

        self.dry_run = args.dry_run
        self.check_constraints = args.check_constraints
        self.max_edits = args.max_edits
        self.edits = 0
        self.code_property = code_property
        self.instance_of = instance_of

        # A dry run reads anonymously without credentials. With them, it logs in like a real run: it checks the login,
        # and reads with the higher rate limits of the bot account.
        if self.dry_run and not (getattr(config, 'user', None) and getattr(config, 'password', None)):
            self.wbi = WikibaseIntegrator()
        else:
            login_instance = wbi_login.Login(user=config.user, password=config.password)
            self.wbi = WikibaseIntegrator(login=login_instance, is_bot=True)

        # Group the edits of the run in one EditGroups batch, to review or revert them at once
        self.editgroup = EditGroup()
        if not self.dry_run:
            logging.info(f'EditGroups batch of this run: {self.editgroup.url}')

        self.frc = None
        if fastrun:
            base_filter = [Item(prop_nr='P31', value=instance_of)] if instance_of else []  # instance of the type
            base_filter += [
                Item(prop_nr='P17', value='Q142'),  # country: France
                ExternalID(prop_nr=code_property)  # INSEE code
            ]

            logging.info('Creating fastrun container')
            # The rank is only checked for the latest census, see expected_statement()
            self.frc = wbi_fastrun.get_fastrun_container(base_filter=base_filter, use_qualifiers=True, use_references=True, use_rank=is_latest, cache=True)

        # The last code written by a real run of the script, to resume after it
        self.resume_file = Path(f'annees/{year}/last_edited_{Path(sys.argv[0]).stem}.txt')

    def run(self, rows):
        """
        Update the population of the item of each (code, label, population) row, in the order of the codes. A real run
        saves the last written code, to resume after it if stopped, and removes it once done.
        """
        resume_after = self.resume_file.read_text().strip() if self.resume_file.exists() else ''
        if resume_after:
            logging.info(f'Resuming after the code {resume_after} ({self.resume_file})')

        start_time = time.time()
        stopped = False
        for code, label, population in sorted(rows):
            if code <= resume_after:
                continue
            if self.max_edits is not None and self.edits >= self.max_edits:
                logging.info(f'Stopping after {self.edits} edits (--max-edits), before the code {code}')
                stopped = True
                break
            if self.update(code, label, population) and not self.dry_run:
                self.resume_file.write_text(code)

        # Kept on a dry run, to not lose the resume point of a real run, and when stopped before the end
        if not self.dry_run and not stopped:
            self.resume_file.unlink(missing_ok=True)

        logging.info(f'--- {time.time() - start_time:.0f} seconds ---')

    def update(self, code, label, population):
        """Update the population of the item holding the code. Return whether it was written."""
        claims = [ExternalID(prop_nr=self.code_property, value=code), expected_statement(population)]

        entities = self.find_items(code, claims)
        if not entities:
            logging.info(f'No item found for {label} {code}')
            return False

        if len(entities) == 1:
            if not self.write_required(claims, entities):
                logging.info(f'Write not required for {label} {code}')
                return False
            item = self.get_item(entities[0])
        else:
            # Always choose among several items, to also remove the population written on the wrong ones
            item = self.choose_item(entities, code, population)
            if not item:
                logging.info(f'Skipping {label} {code}: no single item held the code on {geographic_date:%Y-%m-%d}')
                return False
            if not self.write_required(claims, [item.id]):
                logging.info(f'Write not required for {label} {code} to {item.id}')
                return False

        summary = update_population(item, population)
        if not summary:
            logging.info(f'Population already up to date for {label} {code} on {item.id}')
            return False

        logging.info(f'Write to Wikidata for {label} {code} to {item.id}')
        return self.try_write(item, summary)

    def find_items(self, code, claims):
        """The ids of the items holding the code."""
        if self.frc:
            return self.frc.get_entities(claims=claims, cache=True, query_limit=QUERY_LIMIT)

        type_filter = f'wdt:P31 wd:{self.instance_of}; ' if self.instance_of else ''
        query = f'SELECT DISTINCT ?item WHERE {{ ?item {type_filter}wdt:P17 wd:Q142; p:{self.code_property}/ps:{self.code_property} "{code}". }}'
        return [result['item']['value'].rsplit('/', 1)[-1] for result in wbi_helpers.execute_sparql_query(query)['results']['bindings']]

    def write_required(self, claims, entities):
        """
        Whether the population of the census is missing or different on the items. Always True without fastrun, the
        items are read and update_population() tells if they change.
        """
        return self.frc is None or self.frc.write_required(claims=claims, entity_filter=entities, property_filter='P1082', cache=True, query_limit=QUERY_LIMIT)

    def choose_item(self, entities, code, population):
        """
        Choose, among the items holding the code, the one that held it on the geographic date of the census: the one
        taking the code on this date if any (a new commune taking the code of a merged one), else the only one left.
        Remove from the others the population of the census written by the bot, unless they start after this date.

        :return: The chosen item, or None if no single item held the code
        """
        held = []
        new = []
        for entity in entities:
            item = self.get_item(entity)
            reason = rejection(item, self.code_property, code, self.instance_of)
            if reason is None:
                held.append(item)
                if takes_code_on_geographic_date(item, self.code_property, code):
                    new.append(item)
                continue

            logging.info(f'{entity} did not hold the code {code} on {geographic_date:%Y-%m-%d}: {reason}')
            if self.instance_of and starts_after_geographic_date(item, self.instance_of):
                logging.debug(f'Not removing the population of {entity}, it starts after {geographic_date:%Y-%m-%d}')
                continue

            removed = remove_census_statements(item, population)
            if removed:
                logging.info(f'Removing {removed} wrong population statements from {entity}')
                self.try_write(item, summary='Remove wrong population for ' + year)

        if len(new) == 1:
            return new[0]
        if len(held) == 1:
            return held[0]
        logging.info(f'Items holding the code {code} on {geographic_date:%Y-%m-%d}: {[item.id for item in held]}')
        return None

    def get_item(self, entity_id):
        """Read the statements of an item."""
        return self.wbi.item.get(entity_id, props=['claims'], max_retries=MAX_RETRIES)

    def try_write(self, item, summary):
        """Write the item, logging an error instead of stopping the run if the API refuses it. Return whether it was written."""
        try:
            self.write(item, summary)
        except MWApiError as e:
            logging.error(f'Write to {item.id} failed: {e}')
            return False
        return True

    def write(self, item: ItemEntity, summary):
        """Write the population (P1082) statements of the item, or only log them on a dry run."""
        self.edits += 1
        if self.dry_run:
            logging.info(f'[dry-run] Would write to {item.id}: {summary}')
            for claim in item.claims.get('P1082'):
                logging.debug(f'[dry-run] {claim.get_json()}')
            return

        item.write(summary=self.editgroup.summary(summary), limit_claims=['P1082'], fields_to_update=EntityField.CLAIMS, max_retries=MAX_RETRIES, retry_after=300)

        if self.check_constraints:
            self.log_constraint_violations(item.id)

    @staticmethod
    def log_constraint_violations(entity_id):
        """Log the constraint violations and warnings of the population statements of an entity."""
        result = wbi_helpers.check_constraints(entity_id=entity_id, status=['violation', 'warning'])
        for statement in result['wbcheckconstraints'][entity_id]['claims'].get('P1082', []):
            for check in statement['mainsnak'].get('results', []):
                logging.warning(f"Constraint {check['status']} on {entity_id} ({statement['id']}): {check['constraint']['typeLabel']}")
