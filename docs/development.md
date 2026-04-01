# Medusa Development Guide

Ce document décrit comment configurer votre environnement de développement pour Medusa, en utilisant `pyenv` pour la gestion des versions Python et `poetry` pour les dépendances.

## Prérequis
- `pyenv` et `pyenv-virtualenv` installés
- `poetry` installé

## Installation de Python avec pyenv

Medusa supporte Python 3.9 à 3.12 (et `< 3.13`).

```bash
# Installer plusieurs versions de Python supportées
pyenv install 3.10.13
pyenv install 3.11.8
pyenv install 3.12.2

# Créer un environnement virtuel avec pyenv pour le développement (par ex: avec 3.12)
pyenv virtualenv 3.12.2 venv-medusa

# Activer l'environnement
pyenv shell venv-medusa
python --version
```

## Installation des dépendances

```bash
# Installation de base pour le développement et les tests
poetry install --with test

# Installation avec le module d'encryption activé (optionnel mais requis pour les tests d'encryption)
poetry install --with test -E encryption
```

## Tests Unitaires

### Configuration de l'environnement

Certains tests nécessitent que `JAVA_HOME` soit défini (notamment pour l'interaction avec Cassandra/ccm lors des tests d'intégration, mais c'est une bonne pratique de l'avoir défini globalement).

```bash
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-11.0.25.0.9-7.el9.x86_64
```

### Qualité du code (Flake8)

```bash
# Vérification des erreurs critiques
poetry run flake8 . --count --select=E9,F63,F7,F82 --show-source --statistics

# Vérification complète selon les standards du projet
poetry run flake8 . --count --exit-zero --max-complexity=10 --statistics --ignore=W503,E402,E713,E231,E201,E241,E702
```

### Lancer les tests unitaires via Tox

Tox est configuré pour lancer la suite de tests sur différentes versions de Python, avec et sans l'encryption.

```bash
# Lancer tous les tests unitaires configurés dans tox.ini
# (Il faut que les versions de python testées par tox, e.g. 3.10, 3.11, 3.12 soient installées via pyenv localement)
poetry run tox
```

#### Tester des environnements spécifiques

Vous pouvez cibler des versions de Python spécifiques ou des tests avec/sans encryption.

```bash
# Tester uniquement avec Python 3.12 (sans encryption)
poetry run tox -e py312

# Tester uniquement avec Python 3.12 (avec le module d'encryption activé)
poetry run tox -e py312-encryption
```

#### Tester via pytest (plus ciblé)

Si vous voulez lancer un fichier ou un dossier spécifique de tests sans utiliser tox :

```bash
# Lancer tous les tests sans coverage
poetry run pytest tests/

# Lancer un test spécifique
poetry run pytest tests/storage/s3_storage_test.py
```

## Tests d'Intégration

Les tests d'intégration nécessitent un environnement docker/minio fonctionnel.

```bash
# Exemple de lancement des tests d'intégration avec MinIO et Cassandra 4.1.9
./run_integration_tests.sh --minio --cassandra-version=4.1.9
```
