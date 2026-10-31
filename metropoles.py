"""
Update the population of the communes of Paris, Lyon and Marseille on Wikidata, from the legal populations of INSEE: the
sum of the populations of their municipal arrondissements.
"""
import logging

from common import Bot, csv_rows, year

bot = Bot('Update Paris, Lyon and Marseille population on Wikidata', code_property='P374')  # INSEE municipality code

# The communes and the codes of their municipal arrondissements
communes = {
    '75056': ('Paris', range(75101, 75121)),
    '69123': ('Lyon', range(69381, 69390)),
    '13055': ('Marseille', range(13201, 13217))
}

file_name = 'donnees_communes.csv' if year >= '2019' else 'Communes.csv'

populations = dict.fromkeys(communes, 0)
for row in csv_rows(file_name):
    code = row[6]  # COM
    for commune, (name, arrondissements) in communes.items():
        if code.isdigit() and int(code) in arrondissements:
            populations[commune] += int(row[8])  # PMUN

rows = []
for code, (name, _) in communes.items():
    if populations[code]:
        rows.append((code, name, populations[code]))
    else:
        logging.warning(f'No population found for {name} {code}')

bot.run(rows)
