"""Update the population of the municipal arrondissements of Paris, Lyon and Marseille on Wikidata, from the legal populations of INSEE."""
from common import Bot, csv_rows

bot = Bot('Update Paris, Lyon and Marseille municipal arrondissements population on Wikidata', code_property='P374', instance_of='Q702842')  # INSEE municipality code, municipal arrondissement

# COM, Commune (DEP), PMUN of the municipal arrondissements
bot.run((row[6], f'{row[7]} ({row[2]})', int(row[8])) for row in csv_rows('donnees_communes.csv') if row[7].startswith(('Paris ', 'Lyon ', 'Marseille ')))
