"""Update the population of the departments of France on Wikidata, from the legal populations of INSEE."""
from common import Bot, csv_rows

bot = Bot('Update French departments population on Wikidata', code_property='P2586', instance_of='Q6465')  # INSEE department code, department of France

# DEP, Département (DEP), PMUN. Paris is skipped: the department is the item of the commune (Q90), updated by metropoles.py
bot.run((row[2], f'{row[3]} ({row[2]})', int(row[7])) for row in csv_rows('donnees_departements.csv') if row[3] != 'Paris')
