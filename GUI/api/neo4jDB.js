
const neo4j = require('neo4j-driver');

const uri = 'bolt://localhost:7687';
const user = 'neo4j';
const password = 'qwerty123';

const driver = neo4j.driver(uri, neo4j.auth.basic(user, password));

async function connectDBNeo4J() {
    try {
        const session = driver.session();
        await session.run('RETURN 1');
        console.log('Connected to Neo4j');
        await session.close();
    } catch (err) {
        console.error('Neo4j connection error:', err);
    }
}

function getNeo4J() {
    return driver.session(); // puoi specificare { database: 'nome' } se serve
}

module.exports = { driver, getNeo4J, connectDBNeo4J };
