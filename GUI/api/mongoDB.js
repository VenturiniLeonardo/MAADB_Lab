// ./db/mongoDB.js
const { MongoClient } = require("mongodb");

const uri = "mongodb://localhost:27017";
const client = new MongoClient(uri);
let db;

async function connectDBMongo() {
    try {
        await client.connect();
        db = client.db("MAADB");
        console.log("MongoDB connected");
    } catch (err) {
        console.error(err);
    }
}

function getMongo() {
    return db;
}

module.exports = { connectDBMongo, getMongo };
