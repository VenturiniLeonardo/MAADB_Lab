import pandas as pd
from pymongo import MongoClient
import os

# Parametri
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "MAADB"

# File CSV da caricare e nome collezione Mongo corrispondente
csv_files = {
    "Organisation": "test/static/organisation_0_0.csv",
    "TagClass": "test/static/tagclass_0_0.csv",
    "Place": "test/static/place_0_0.csv",
    "PlaceIsPartOfPlace": "test/static/place_isPartOf_place_0_0.csv",
    "TagHasTypeTagClass": "test/static/tag_hasType_tagclass_0_0.csv",
    "TagClassIsSubclassOfTagClass": "test/static/tagclass_isSubclassOf_tagclass_0_0.csv",
    "IsLocatedInPlace": "test/dynamic/person_isLocatedIn_place_0_0.csv",
    "Comment": "test/dynamic/comment_0_0.csv",
    "CommentReplyOfComment": "test/dynamic/comment_replyOf_comment_0_0.csv",
    "Post": "test/dynamic/post_0_0.csv",
    "ForumContainerPost": "test/dynamic/forum_containerOf_post_0_0.csv"
}

# Connessione a MongoDB
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
curr_dir = os.getcwd()

# Caricamento dei file CSV
for collection_name, filename in csv_files.items():
   
    path = os.path.join(curr_dir, filename)
    

    try:
        # Leggi il CSV (modifica sep se serve)
        df = pd.read_csv(path, sep="|")

        # Rinomina le colonne

        has_id_1 = any(col.endswith('.id.1') for col in df.columns)
        
        new_columns = []
        for col in df.columns:
            if col.endswith('.id.1'):
                col = col.lower()
                new_col = col.replace('.id.1', 'From')
                new_columns.append(new_col)
            elif col.endswith('.id'):
                col = col.lower()
                if has_id_1:
                    new_col = col.replace('.id', 'To')
                else:
                    new_col = col.replace('.id', 'Id')
                new_columns.append(new_col)
            else:
                new_columns.append(col)
        
        df.columns = new_columns

        # Converti in dizionari
        data = df.to_dict(orient="records")

        # Crea collezione se non esiste
        if collection_name not in db.list_collection_names():
            db.create_collection(collection_name)
            print(f"Collezione '{collection_name}' creata.")
        else:
            print(f"Collezione '{collection_name}' già esistente. Inserisco comunque dati.")

        # Inserisci dati
        if data:
            db[collection_name].insert_many(data)
            print(f"Inseriti {len(data)} documenti in '{collection_name}'.")
        else:
            print(f"File CSV '{filename}' vuoto. Nessun dato inserito.")

    except Exception as e:
        print(f"Errore durante il caricamento di '{filename}': {e}")
