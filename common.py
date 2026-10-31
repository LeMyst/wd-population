"""
Shared setup of the population update scripts: census year configuration, command line options, Wikidata login and
writes (dry run, EditGroups batch and constraint checks).
"""
import argparse
import logging
import runpy
from datetime import datetime

from wikibaseintegrator import WikibaseIntegrator, wbi_helpers, wbi_login
from wikibaseintegrator.datatypes import Item, Time
from wikibaseintegrator.entities import ItemEntity
from wikibaseintegrator.wbi_config import config as wbi_config
from wikibaseintegrator.wbi_editgroups import EditGroup
from wikibaseintegrator.wbi_enums import EntityField

# Import local config for user, password and year
import config

wbi_config['USER_AGENT'] = 'WikibaseIntegrator/1.0 Update French Population'

# A run lasts hours: wait out a short outage of Wikidata or of the query service instead of stopping. The failed
# requests (connection errors, HTTP 5xx) are tried 10 times, waiting 1, 2, 4... seconds, 4 to 8 minutes in all, instead
# of 5 times (15 seconds) by default since WikibaseIntegrator 0.14.
wbi_config['BACKOFF_MAX_TRIES'] = 10

logging.basicConfig(level=logging.DEBUG)

year = str(config.year)

# Date of the census
point_in_time = f'+{year}-01-01T00:00:00Z'

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


def parse_wb_time(time_str):
    """Normalize and parse Wikibase time strings like '+2020-00-00T00:00:00Z'."""
    if not time_str:
        return None
    normalized = time_str.replace('-00-00T', '-01-01T')
    try:
        return datetime.strptime(normalized, '+%Y-%m-%dT%H:%M:%SZ')
    except Exception:
        return None


class Bot:
    """Read the command line options, login to Wikidata and write the population statements of the items."""

    def __init__(self, description):
        parser = argparse.ArgumentParser(description=description)
        parser.add_argument('--dry-run', action='store_true', help='do not write anything to Wikidata, only log what would be written')
        parser.add_argument('--check-constraints', action='store_true', help='log the constraint violations of the population statements after each write')
        args = parser.parse_args()

        self.dry_run = args.dry_run
        self.check_constraints = args.check_constraints

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

    def write(self, item: ItemEntity, summary):
        """Write the population (P1082) statements of the item, or only log them on a dry run."""
        if self.dry_run:
            logging.info(f'[dry-run] Would write to {item.id}: {summary}')
            for claim in item.claims.get('P1082'):
                logging.debug(f'[dry-run] {claim.get_json()}')
            return

        item.write(summary=self.editgroup.summary(summary), limit_claims=['P1082'], fields_to_update=EntityField.CLAIMS, retry_after=300)

        if self.check_constraints:
            self.log_constraint_violations(item.id)

    @staticmethod
    def log_constraint_violations(entity_id):
        """Log the constraint violations and warnings of the population statements of an entity."""
        result = wbi_helpers.check_constraints(entity_id=entity_id, status=['violation', 'warning'])
        for statement in result['wbcheckconstraints'][entity_id]['claims'].get('P1082', []):
            for check in statement['mainsnak'].get('results', []):
                logging.warning(f"Constraint {check['status']} on {entity_id} ({statement['id']}): {check['constraint']['typeLabel']}")
