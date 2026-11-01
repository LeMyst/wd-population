"""Update the population of the cantons of France on Wikidata, from the legal populations of INSEE."""
from common import Bot, csv_rows

bot = Bot('Update French cantons population on Wikidata', code_property='P2506', instance_of='Q18524218')  # INSEE canton code, canton of France

# CAN, Canton (DEP), PMUN
bot.run((row[4], f'{row[5]} ({row[2]})', int(row[7])) for row in csv_rows('donnees_cantons.csv'))
