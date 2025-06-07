from neo4j import GraphDatabase
import os
import pandas as pd
import time
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Database connection parameters
URI = "neo4j://localhost:7687"
AUTH = ("neo4j", "qwerty123")

# Batch size for operations
BATCH_SIZE = 5000

def connect_to_db():
    """Connect to Neo4j database"""
    driver = GraphDatabase.driver(URI, auth=AUTH)
    try:
        driver.verify_connectivity()
        logger.info("Connected to Neo4j database")
        return driver
    except Exception as e:
        logger.error(f"Failed to connect to Neo4j: {e}")
        return None
    
def clear_db(driver):
    """Clear the database of all nodes and relationships"""
    with driver.session() as session:
        start_time = time.time()
        # Clear all relationships first, then nodes
        session.run("MATCH ()-[r]->() DELETE r")
        session.run("MATCH (n) DELETE n")
        logger.info(f"Cleared database in {time.time() - start_time:.2f} seconds")

def find_file(base_path):
    """Find a file using different path strategies"""
    if os.path.exists(base_path):
        return base_path
    
    # Extract filename from path
    filename = os.path.basename(base_path)
    alt_path = os.path.join(os.getcwd(), filename)
    
    if os.path.exists(alt_path):
        return alt_path
    
    logger.error(f"File not found: {base_path}")
    return None

def load_csv_data(path_name):
    """Load and preprocess CSV data"""
    file_path = find_file(path_name)
    if not file_path:
        return None
    
    try:
        data = pd.read_csv(file_path, sep="|", encoding="utf-8")
        logger.info(f"Loaded {len(data)} records from {file_path}")
        return data
    except Exception as e:
        logger.error(f"Error loading data: {e}")
        return None

def process_datetime_fields(df, date_columns):
    """Process datetime fields in the dataframe"""
    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
            # Format as ISO string for Neo4j
            df[col] = df[col].dt.strftime('%Y-%m-%dT%H:%M:%S')
    return df

def batch_process(data, batch_size=BATCH_SIZE):
    """Split data into batches for processing"""
    total_batches = (len(data) + batch_size - 1) // batch_size
    for i in range(total_batches):
        start_idx = i * batch_size
        end_idx = min((i + 1) * batch_size, len(data))
        yield data.iloc[start_idx:end_idx]

def create_nodes(driver, label, data, id_field='id', batch_size=BATCH_SIZE):
    """Create nodes in batches using efficient Cypher"""
    if data is None or len(data) == 0:
        logger.warning(f"No {label} data to insert")
        return
    
    total_records = len(data)
    processed = 0
    start_time = time.time()
    
    try:
        for batch in batch_process(data, batch_size):            
            # Convert batch to list of dictionaries for parameters
            records = batch.to_dict('records')
            
            # Create a parameterized Cypher query for the batch
            with driver.session() as session:
                # UNWIND is much more efficient for batch operations
                params = {
                    'batch': records
                }
                
                query = f"""
                UNWIND $batch AS row
                CREATE (n:{label})
                SET n = row
                """
                
                result = session.run(query, params)
                processed += len(batch)
                
                if processed % (batch_size * 5) == 0 or processed == total_records:
                    logger.info(f"Created {processed}/{total_records} {label} nodes ({processed/total_records*100:.1f}%)")
        
        # Verify insertion
        with driver.session() as session:
            result = session.run(f"MATCH (n:{label}) RETURN count(n) AS count")
            db_count = result.single()["count"]
            logger.info(f"Completed: {total_records} {label} nodes processed, {db_count} found in database")
            
            
    except Exception as e:
        logger.error(f"Error creating {label} nodes: {e}")

def create_relationships(driver, start_label, rel_type, end_label, data, 
                        start_id_field, end_id_field, props=None, batch_size=BATCH_SIZE):
    """Create relationships in batches using efficient Cypher"""
    if data is None or len(data) == 0:
        logger.warning(f"No {rel_type} relationship data to insert")
        return
    
    total_records = len(data)
    processed = 0
    start_time = time.time()
    
    try:
        for batch in batch_process(data, batch_size):
            # Create a parameterized Cypher query for the batch
            records = []
            
            # Extract only needed columns to reduce memory usage
            for _, row in batch.iterrows():
                record = {
                    'start_id': int(row[start_id_field]),
                    'end_id': int(row[end_id_field])
                }
                
                # Add any properties to the relationship
                if props:
                    for prop, field in props.items():
                        if field in row and row[field] != "":
                            record[prop] = row[field]
                
                records.append(record)
            
            # Execute the query
            with driver.session() as session:
                params = {'batch': records}
                
                prop_set = ""
                if props:
                    # Prepare dynamic property setting for relationships
                    prop_fields = []
                    for prop in props.keys():
                        prop_fields.append(f"r.{prop} = row.{prop}")
                    
                    if prop_fields:
                        prop_set = "SET " + ", ".join(prop_fields)
                
                query = f"""
                UNWIND $batch AS row
                MATCH (a:{start_label} {{id: row.start_id}}), (b:{end_label} {{id: row.end_id}})
                CREATE (a)-[r:{rel_type}]->(b)
                {prop_set}
                """
                
                session.run(query, params)
                processed += len(batch)
                
                if processed % (batch_size * 5) == 0 or processed == total_records:
                    logger.info(f"Created {processed}/{total_records} {rel_type} relationships ({processed/total_records*100:.1f}%)")
        
        # Verify insertion
        with driver.session() as session:
            result = session.run(f"MATCH ()-[r:{rel_type}]->() RETURN count(r) AS count")
            db_count = result.single()["count"]
            logger.info(f"Completed: {total_records} {rel_type} relationships processed, {db_count} found in database")
            
            
    except Exception as e:
        logger.error(f"Error creating {rel_type} relationships: {e}")

def import_person_data(driver):
    """Import person data"""
    person_path = "test/dynamic/person_0_0.csv"
    person_data = load_csv_data(person_path)
    
    if person_data is not None:
        # Process date columns
        person_data = process_datetime_fields(person_data, ['birthday', 'creationDate'])
        # Handle missing values
        person_data = person_data.fillna("")
        # Create nodes
        create_nodes(driver, "Person", person_data)
    else:
        logger.error(f"Failed to process person data from {person_path}")
        return False
    
    return True

def import_tag_data(driver):
    """Import tag data"""
    tag_path = "test/static/tag_0_0.csv"
    tag_data = load_csv_data(tag_path)
    
    if tag_data is not None:
        # Handle missing values
        tag_data = tag_data.fillna("")
        # Create nodes
        create_nodes(driver, "Tag", tag_data)
    else:
        logger.error(f"Failed to process tag data from {tag_path}")
        return False
    
    return True

def import_knows_relationships(driver):
    """Import knows relationships"""
    knows_path = "test/dynamic/person_knows_person_0_0.csv"
    knows_data = load_csv_data(knows_path)
    
    if knows_data is not None:
        # Process date columns
        knows_data = process_datetime_fields(knows_data, ['creationDate'])
        # Create relationships
        create_relationships(
            driver, 
            "Person", "KNOWS", "Person", 
            knows_data,
            "Person.id", "Person.id.1",
            props={"creationDate": "creationDate"}
        )
    else:
        logger.error(f"Failed to process knows relationships from {knows_path}")
        return False
    
    return True

def import_interest_relationships(driver):
    """Import interest relationships"""
    interest_path = "test/dynamic/person_hasInterest_tag_0_0.csv"
    interest_data = load_csv_data(interest_path)
    
    if interest_data is not None:
        # Create relationships
        create_relationships(
            driver, 
            "Person", "INTEREST", "Tag", 
            interest_data,
            "Person.id", "Tag.id"
        )
    else:
        logger.error(f"Failed to process interest relationships from {interest_path}")
        return False
    
    return True

def import_comment_data(driver):
    """Import comment data"""
    comment_path = "test/dynamic/comment_0_0.csv"
    comment_data = load_csv_data(comment_path)
    
    if comment_data is not None:
        # Only keep id column
        comment_data = comment_data[["id"]].fillna("")
        # Create nodes
        create_nodes(driver, "Comment", comment_data)
    else:
        logger.error(f"Failed to process comment data from {comment_path}")
        return False
    
    return True

def import_likes_comment_relationships(driver):
    """Import likes comment relationships"""
    likes_comment_path = "test/dynamic/person_likes_comment_0_0.csv"
    likes_comment_data = load_csv_data(likes_comment_path)
    
    if likes_comment_data is not None:
        # Create relationships
        create_relationships(
            driver, 
            "Person", "LIKES_COMMENT", "Comment", 
            likes_comment_data,
            "Person.id", "Comment.id"
        )
    else:
        logger.error(f"Failed to process likes comment relationships from {likes_comment_path}")
        return False
    
    return True

def import_forum_data(driver):
    """Import forum data"""
    forum_path = "test/dynamic/forum_0_0.csv"
    forum_data = load_csv_data(forum_path)
    
    if forum_data is not None:
        # Process date columns
        forum_data = process_datetime_fields(forum_data, ['creationDate'])
        # Handle missing values
        forum_data = forum_data.fillna("")
        # Create nodes
        create_nodes(driver, "Forum", forum_data)
    else:
        logger.error(f"Failed to process forum data from {forum_path}")
        return False
    
    return True

def import_forum_member_relationships(driver):
    """Import forum member relationships"""
    member_path = "test/dynamic/forum_hasMember_person_0_0.csv"
    member_data = load_csv_data(member_path)
    
    if member_data is not None:
        # Process date columns
        member_data = process_datetime_fields(member_data, ['joinDate'])
        # Create relationships
        create_relationships(
            driver, 
            "Person", "MEMBER", "Forum", 
            member_data,
            "Person.id", "Forum.id",
            props={"joinDate": "joinDate"}
        )
    else:
        logger.error(f"Failed to process forum member relationships from {member_path}")
        return False
    
    return True

def import_forum_moderator_relationships(driver):
    """Import forum moderator relationships"""
    moderator_path = "test/dynamic/forum_hasModerator_person_0_0.csv"
    moderator_data = load_csv_data(moderator_path)
    
    if moderator_data is not None:
        # Create relationships
        create_relationships(
            driver, 
            "Forum", "MODERATOR", "Person", 
            moderator_data,
            "Forum.id", "Person.id"
        )
    else:
        logger.error(f"Failed to process forum moderator relationships from {moderator_path}")
        return False
    
    return True

def import_forum_tag_relationships(driver):
    """Import forum tag relationships"""
    tag_forum_path = "test/dynamic/forum_hasTag_tag_0_0.csv"
    tag_forum_data = load_csv_data(tag_forum_path)
    
    if tag_forum_data is not None:
        # Create relationships
        create_relationships(
            driver, 
            "Forum", "HAS_TAG", "Tag", 
            tag_forum_data,
            "Forum.id", "Tag.id"
        )
    else:
        logger.error(f"Failed to process forum tag relationships from {tag_forum_path}")
        return False
    
    return True

def import_post_data(driver):
    """Import post data"""
    post_path = "test/dynamic/post_0_0.csv"
    post_data = load_csv_data(post_path)
    
    if post_data is not None:
        # Only keep id column
        post_data = post_data[["id"]].fillna("")
        # Create nodes
        create_nodes(driver, "Post", post_data)
    else:
        logger.error(f"Failed to process post data from {post_path}")
        return False
    
    return True

def import_likes_post_relationships(driver):
    """Import likes post relationships"""
    likes_post_path = "test/dynamic/person_likes_post_0_0.csv"
    likes_post_data = load_csv_data(likes_post_path)
    
    if likes_post_data is not None:
        # Create relationships
        create_relationships(
            driver, 
            "Person", "LIKES_POST", "Post", 
            likes_post_data,
            "Person.id", "Post.id"
        )
    else:
        logger.error(f"Failed to process likes post relationships from {likes_post_path}")
        return False
    
    return True

def import_tag_comment_relationships(driver):
    """Import tag comment relationships"""
    tag_comment_path = "test/dynamic/comment_hasTag_tag_0_0.csv"
    tag_comment_data = load_csv_data(tag_comment_path)
    
    if tag_comment_data is not None:
        # Create relationships
        create_relationships(
            driver, 
            "Comment", "TAGGED", "Tag", 
            tag_comment_data,
            "Comment.id", "Tag.id"
        )
    else:
        logger.error(f"Failed to process tag comment relationships from {tag_comment_path}")
        return False
    
    return True

def import_tag_post_relationships(driver):
    """Import tag post relationships"""
    tag_post_path = "test/dynamic/post_hasTag_tag_0_0.csv"
    tag_post_data = load_csv_data(tag_post_path)
    
    if tag_post_data is not None:
        # Create relationships
        create_relationships(
            driver, 
            "Post", "TAGGED", "Tag", 
            tag_post_data,
            "Post.id", "Tag.id"
        )
    else:
        logger.error(f"Failed to process tag post relationships from {tag_post_path}")
        return False
    
    return True

def import_organization_data(driver):
    """Import organization data"""
    org_path = "test/static/organisation_0_0.csv"
    org_data = load_csv_data(org_path)
    
    if org_data is not None:
        # Filter by organization type
        university_data = org_data[org_data["type"] == "university"][["id", "type"]]
        company_data = org_data[org_data["type"] == "company"][["id", "type"]]
        
        # Create nodes
        create_nodes(driver, "University", university_data)
        create_nodes(driver, "Company", company_data)
    else:
        logger.error(f"Failed to process organization data from {org_path}")
        return False
    
    return True

def import_work_at_relationships(driver):
    """Import work at relationships"""
    work_at_path = "test/dynamic/person_workAt_organisation_0_0.csv"
    work_at_data = load_csv_data(work_at_path)
    
    if work_at_data is not None:
        # Create relationships
        create_relationships(
            driver, 
            "Person", "WORK_AT", "Company", 
            work_at_data,
            "Person.id", "Organisation.id",
            props={"workFrom": "workFrom"}
        )
    else:
        logger.error(f"Failed to process work at relationships from {work_at_path}")
        return False
    
    return True

def import_study_at_relationships(driver):
    """Import study at relationships"""
    study_at_path = "test/dynamic/person_studyAt_organisation_0_0.csv"
    study_at_data = load_csv_data(study_at_path)
    
    if study_at_data is not None:
        # Create relationships
        create_relationships(
            driver, 
            "Person", "STUDY_AT", "University", 
            study_at_data,
            "Person.id", "Organisation.id",
            props={"classYear": "classYear"}
        )
    else:
        logger.error(f"Failed to process study at relationships from {study_at_path}")
        return False
    
    return True

def import_comment_hasCreator_person(driver):
    """Import comment has creator relationships"""
    comment_creator_path = "test/dynamic/comment_hasCreator_person_0_0.csv"
    comment_creator_data = load_csv_data(comment_creator_path)
    
    if comment_creator_data is not None:
        # Create relationships
        create_relationships(
            driver, 
            "Comment", "HAS_CREATOR_COMMENT", "Person", 
            comment_creator_data,
            "Comment.id", "Person.id"
        )
    else:
        logger.error(f"Failed to process comment has creator relationships from {comment_creator_path}")
        return False
    
    return True

def import_post_hasCreator_person(driver):
    """Import post has creator relationships"""
    post_creator_path = "test/dynamic/post_hasCreator_person_0_0.csv"
    post_creator_data = load_csv_data(post_creator_path)
    
    if post_creator_data is not None:
        # Create relationships
        create_relationships(
            driver, 
            "Post", "HAS_CREATOR_POST", "Person", 
            post_creator_data,
            "Post.id", "Person.id"
        )
    else:
        logger.error(f"Failed to process post has creator relationships from {post_creator_path}")
        return False
    
    return True

def main():
    # Connect to database
    driver = connect_to_db()
    if not driver:
        return
    
    try:
        # Clear existing data
        clear_db(driver)
        
        # Create indices to speed up operations
        create_indices(driver)
        
        # Import nodes first, then relationships
        
        # Import Person nodes
        if not import_person_data(driver):
            return
        
        # Import Tag nodes
        if not import_tag_data(driver):
            return
        
        # Import Comment nodes
        if not import_comment_data(driver):
            return
        
        # Import Forum nodes
        if not import_forum_data(driver):
            return
        
        # Import Post nodes
        if not import_post_data(driver):
            return
        
        # Import Organization nodes (Universities and Companies)
        if not import_organization_data(driver):
            return
        
        # Now import relationships
        
        # Person knows Person
        if not import_knows_relationships(driver):
            return
        
        # Person has interest in Tag
        if not import_interest_relationships(driver):
            return
        
        # Person likes Comment
        if not import_likes_comment_relationships(driver):
            return
        
        # Person is member of Forum
        if not import_forum_member_relationships(driver):
            return
        
        # Forum has moderator Person
        if not import_forum_moderator_relationships(driver):
            return
        
        # Forum has Tag
        if not import_forum_tag_relationships(driver):
            return
        
        # Person likes Post
        if not import_likes_post_relationships(driver):
            return
        
        # Comment is tagged with Tag
        if not import_tag_comment_relationships(driver):
            return
        
        # Post is tagged with Tag
        if not import_tag_post_relationships(driver):
            return
        
        # Person works at Company
        if not import_work_at_relationships(driver):
            return
        
        # Person studies at University
        if not import_study_at_relationships(driver):
            return
        
        # Comment has creator Person
        if not import_comment_hasCreator_person(driver):
            return
        
        # Post has creator Person
        if not import_post_hasCreator_person(driver):
            return
        
    except Exception as e:
        logger.error(f"Error during import process: {e}")
    finally:
        driver.close()

if __name__ == "__main__":
    main()