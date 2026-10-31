Chiffres détaillés données démographique :
https://www.insee.fr/fr/statistiques?taille=100&debut=0&theme=1&categorie=5

* Population 2023 : https://www.insee.fr/fr/statistiques/8680726
* Population 2022 : https://www.insee.fr/fr/statistiques/8290591
* Population 2021 : https://www.insee.fr/fr/statistiques/7739582
* Population 2020 : https://www.insee.fr/fr/statistiques/6683035
* Population 2019 : https://www.insee.fr/fr/statistiques/6011070
* Population 2018 : https://www.insee.fr/fr/statistiques/4989724
* Population 2017 : https://www.insee.fr/fr/statistiques/4265429
* Population 2016 : https://www.insee.fr/fr/statistiques/3677785
* Population 2015 : https://www.insee.fr/fr/statistiques/3545833

## Utilisation

Nécessite Python 3.11 ou plus récent (WikibaseIntegrator 0.14).

```
python -m venv .venv
.venv\Scripts\python -m pip install -r requirements.txt
```

Créer un fichier `config.py` avec `user` et `password` (mot de passe de robot) et `year` (année du recensement), puis
lancer un script, par exemple `.venv\Scripts\python communes_fastrun.py`.

Options disponibles sur tous les scripts :

* `--dry-run` : n'écrit rien sur Wikidata, affiche seulement les modifications prévues (sans connexion).
* `--check-constraints` : affiche les violations de contraintes des déclarations de population après chaque
  modification.

Les modifications d'une exécution sont regroupées dans un lot [EditGroups](https://editgroups.toolforge.org/), dont le
lien est affiché au démarrage, pour pouvoir les vérifier ou les annuler en une fois.
