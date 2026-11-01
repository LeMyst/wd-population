"""Update the population of the communes of France on Wikidata, from the legal populations of INSEE."""
from common import Bot, csv_rows, year

bot = Bot('Update French communes population on Wikidata', code_property='P374', instance_of='Q484170')  # INSEE municipality code, commune of France

file_name = 'donnees_communes.csv' if year >= '2019' else 'Communes.csv'

# COM, Commune (DEP), PMUN
bot.run((row[6], f'{row[7]} ({row[2]})', int(row[8])) for row in csv_rows(file_name))
