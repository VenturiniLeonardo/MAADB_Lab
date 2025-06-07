# MAADB Laboratorio 2025-2026

## Prerequisiti
- **Node.js** (versione 16 o superiore)
- **Python** (versione 3.8 o superiore)
- **MongoDB** (versione 4.4 o superiore)
- **Neo4j** (versione 4.0 o superiore)
- **npm** o **yarn**

## Struttura Dati
Il progetto richiede i seguenti file CSV nella struttura indicata:

```
test/
├── static/
│   ├── organisation_0_0.csv
│   ├── tagclass_0_0.csv
│   ├── place_0_0.csv
│   ├── place_isPartOf_place_0_0.csv
│   ├── tag_hasType_tagclass_0_0.csv
│   ├── tagclass_isSubclassOf_tagclass_0_0.csv
│   └── tag_0_0.csv
└── dynamic/
    ├── person_0_0.csv
    ├── person_isLocatedIn_place_0_0.csv
    ├── comment_0_0.csv
    ├── comment_replyOf_comment_0_0.csv
    ├── post_0_0.csv
    ├── forum_containerOf_post_0_0.csv
    ├── person_knows_person_0_0.csv
    ├── person_hasInterest_tag_0_0.csv
    ├── person_likes_comment_0_0.csv
    ├── person_likes_post_0_0.csv
    ├── forum_0_0.csv
    ├── forum_hasMember_person_0_0.csv
    ├── forum_hasModerator_person_0_0.csv
    ├── forum_hasTag_tag_0_0.csv
    ├── comment_hasTag_tag_0_0.csv
    ├── post_hasTag_tag_0_0.csv
    ├── person_workAt_organisation_0_0.csv
    ├── person_studyAt_organisation_0_0.csv
    ├── comment_hasCreator_person_0_0.csv
    ├── comment_0_0.csv
    └── post_hasCreator_person_0_0.csv

```

**Nota**: I file CSV devono utilizzare il separatore `|` (pipe).

## Configurazione

### Modificare i parametri di connessione

**MongoDB** (`population_mongo.py`):
```python
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "MAADB"
```

**Neo4j** (`population_neo4j.py`):
```python
URI = "neo4j://localhost:7687"
AUTH = ("neo4j", "qwerty123")
```

## Esecuzione

### 1. Popolamento Database MongoDB

```bash
python population_mongo.py
```

### 2. Popolamento Database Neo4j

```bash
python population_neo4j.py
```

### 3. Avvia il Server Express

```bash
npm start
```

