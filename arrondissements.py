"""Update the population of the arrondissements of France on Wikidata, from the legal populations of INSEE."""
from common import Bot, csv_rows

bot = Bot('Update French arrondissements population on Wikidata', code_property='P3423', instance_of='Q194203')  # INSEE arrondissement code, arrondissement of France

# ARR, Arrondissement (DEP), PMUN
bot.run((row[5], f'{row[6]} ({row[2]})', int(row[8])) for row in csv_rows('donnees_arrondissements.csv'))
