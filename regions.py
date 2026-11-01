"""Update the population of the regions of France on Wikidata, from the legal populations of INSEE."""
from common import Bot, csv_rows

bot = Bot('Update French regions population on Wikidata', code_property='P2585', instance_of='Q36784')  # INSEE region code, region of France

# REG, Région, PMUN
bot.run((row[0], row[1], int(row[5])) for row in csv_rows('donnees_regions.csv'))
