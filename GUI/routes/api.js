const express = require('express');
const router = express.Router();
const { getMongo } = require('../api/mongoDB');
const {getNeo4J } = require('../api/neo4jDB');

router.get('/queryLookUp1', async (req, res) => {
    try {
        const { type, name } = req.query;

        if (!type || !name) {
            return res.status(400).json({ error: 'Both type and name are required' });
        }

        const db_mongo = getMongo();
        let locationIds = [];

        // Trova gli ID delle location in base al tipo e nome specificato
        switch(type.toLowerCase()) {
            case 'city':
                const city = await db_mongo.collection('Place').findOne({
                    name: name,
                    type: 'city'
                });

                if (city) locationIds.push(city.id);
                break;

            case 'country':
                const country = await db_mongo.collection('Place').findOne({
                    name: name,
                    type: 'country'
                });

                if (country) {
                    const cities = await db_mongo.collection('PlaceIsPartOfPlace').find({
                        "placeFrom": country.id,
                    }).toArray();

                    locationIds = cities.map(city => city.placeTo);
                }
                break;

            case 'continent':
                const continent = await db_mongo.collection('Place').findOne({
                    name: name,
                    type: 'continent'
                });

                if (continent) {
                    const countries = await db_mongo.collection('PlaceIsPartOfPlace').find({
                        "placeFrom": continent.id,
                    }).toArray();

                    for (const country of countries) {
                        const cities = await db_mongo.collection('PlaceIsPartOfPlace').find({
                            "placeFrom": country.placeTo,
                        }).toArray();

                        locationIds = [...locationIds, ...cities.map(city => city.placeTo)];
                    }
                }
                break;

            default:
                return res.status(400).json({ error: 'Location type must be city, country, or continent' });
        }

        if (locationIds.length === 0) {
            return res.json({ message: 'No locations found matching the criteria', people: [] });
        }

        const persons = await db_mongo.collection('IsLocatedInPlace').find({
            placeId: { $in: locationIds }
        }).toArray();

        if (persons.length === 0) {
            return res.json({ message: 'No people found in this location', people: [] });
        }

        const personIds = persons.map(p => p.personId);
        const session = getNeo4J();

        const result = await session.run(
            `MATCH (p:Person)
             WHERE p.id IN $personIds
             RETURN p.id AS id, p.firstName AS firstName, p.lastName AS lastName, 
                    p.gender AS gender, p.birthday AS birthday, p.email AS email`,
            { personIds }
        );

        const people = result.records.map(record => ({
            id: record.get('id'),
            firstName: record.get('firstName'),
            lastName: record.get('lastName')
        }));

        const locationData = {
            type: type,
            name: name,
            totalPeople: people.length
        };

        res.json({ location: locationData, people });

    } catch (err) {
        console.error('Error in people-by-location query:', err);
        res.status(500).json({ error: err.message });
    }
});

router.get('/queryLookUp2', async (req, res) => {
    try {
        const { tagName } = req.query;

        if (!tagName) {
            return res.status(400).json({ error: 'Tag name is required' });
        }

        const session = getNeo4J();

        const personTag = await session.run(
            `MATCH (person:Person)-[:LIKES_COMMENT]->(comment:Comment)-[:TAGGED]->(tag:Tag {name: $tagName})
             RETURN person.id AS id, person.firstName AS firstName, person.lastName AS lastName`,
            { tagName }
        );

        const people = personTag.records.map(record => ({
            id: record.get('id').toString(),
            firstName: record.get('firstName'),
            lastName: record.get('lastName')
        }));

        res.json({ people });

    } catch (err) {
        console.error('Error in people-by-comment-tag query:', err);
        res.status(500).json({ error: err.message });
    }
});

router.get('/queryLookUp3', async (req, res) => {
    try {
        const { language } = req.query;
        if (!language) {
            return res.status(400).json({ error: 'Language is required' });
        }

        const db_mongo = getMongo();
        const posts = await db_mongo.collection('Post').find(
            { language }
        ).project({ id: 1 }).toArray();

        const postIds = posts.map(post => post.id);

        if (postIds.length === 0) {
            return res.json({ message: 'No posts found in this language', forums: [] });
        }

        const forumPostRelations = await db_mongo.collection('ForumContainerPost').find(
            { postId: { $in: postIds } }
        ).toArray();

        const forumIds = [...new Set(forumPostRelations.map(relation => relation.forumId))];

        if (forumIds.length === 0) {
            return res.json({ message: 'No forums found containing posts in this language', forums: [] });
        }

        const session = getNeo4J();
        const result = await session.run(
            `MATCH (forum:Forum) 
                WHERE forum.id IN $forumIds 
                RETURN forum.id AS id, forum.title AS title`,
            { forumIds }
        );

        const forums = result.records.map(record => ({
            id: record.get('id'),
            title: record.get('title')
        }));

        res.json({ forums });

    } catch (err) {
        console.error('Error in forums-by-post-language query:', err);
        res.status(500).json({ error: err.message });
    }
});

router.get('/queryAnalitica1', async (req, res) => {
    try {
        const session = getNeo4J();

        const result = await session.run(
            `MATCH (creator:Person)-[:HAS_CREATOR_POST]-(post:Post)-[:LIKES_POST]-(liker:Person),
                          (creator)-[:STUDY_AT]-(university:University),
                          (liker)-[:STUDY_AT]-(university)
                    WITH university.id AS universityId, count(liker) AS likeCount
                    RETURN universityId, likeCount
                    ORDER BY likeCount DESC`
        );

        const count_like = result.records.map(record => ({
            universityId: record.get('universityId').toString(),
            likes: record.get('likeCount').toString()
        }));

        const db_mongo = getMongo();
        const universities = await db_mongo.collection('Organisation').find(
            {id: { $in: count_like.map(cl => parseInt(cl.universityId))}}
        ).toArray();

        const count_like_with_names = count_like.map(cl => ({
            ...cl,
            universityName: universities.find(u => u.id === parseInt(cl.universityId))?.name || 'Unknown'
        }));

        res.json({ count_like_with_names });

        session.close()
    } catch (err) {
        console.error('Error in people-by-comment-tag query:', err);
        res.status(500).json({ error: err.message });
    }
});

router.get('/queryAnalitica2', async (req, res) => {
    try {
        const session = getNeo4J();

        const result = await session.run(
            'MATCH (forum:Forum)-[r:MODERATOR]->(person:Person) ' +
            'RETURN DISTINCT person.firstName AS name, person.id AS id'
        );

        const mods = result.records.map(record => ({
            name: record.get('name'),
            id: record.get('id').toString()
        }));

        // Calcola l'età media per ogni moderatore
        for (const mod of mods) {
            try {
                const avgAgeResult = await session.run(
                    `MATCH (person:Person {id: $idMod})-[:KNOWS]->(knownPerson:Person)
                 WHERE knownPerson.birthday IS NOT NULL
                 WITH person, knownPerson,
                      duration.between(datetime(knownPerson.birthday), datetime()).years AS knownPersonAge
                 RETURN AVG(knownPersonAge) AS AverageAgeOfKnownPeople,
                       COUNT(knownPerson) AS NumberOfKnownPeople`,
                    { idMod: parseInt(mod.id) }
                );

                if (avgAgeResult.records.length > 0) {
                    const avgAge = avgAgeResult.records[0].get('AverageAgeOfKnownPeople');
                    mod.averageAgeOfKnownPeople = avgAge !== null ? parseFloat(avgAge).toFixed(1) : 'No data';
                    mod.numberOfKnownPeople = avgAgeResult.records[0].get('NumberOfKnownPeople').toInt();
                } else {
                    mod.averageAgeOfKnownPeople = 'No data';
                    mod.numberOfKnownPeople = 0;
                }
            } catch (avgErr) {
                console.error(`Error calculating average age for moderator ${mod.name}:`, avgErr);
                mod.averageAgeOfKnownPeople = 'Error';
                mod.numberOfKnownPeople = 0;
            }
        }

        await session.close();
        res.json({ mods });

    } catch (err) {
        console.error('Error in moderators with average age query:', err);
        res.status(500).json({ error: err.message });
    }
})

router.get('/queryAnalitica3', async (req, res) => {
    try {
        const session = getNeo4J();

        const result = await session.run(
            `MATCH (person:Person)-[:INTEREST]->(tag:Tag)
                    WHERE person.gender IS NOT NULL
                    WITH tag.name AS tagName, person.gender AS gender, COUNT(person) AS count
                    WITH tagName, gender, count
                    ORDER BY tagName, count DESC
                    WITH tagName, COLLECT({gender: gender, count: count}) AS genderCounts,
                         SUM(count) AS totalCount
                    WITH tagName, 
                         genderCounts[0].gender AS mostCommonGender,
                         genderCounts[0].count AS count,
                         toFloat(genderCounts[0].count) / totalCount AS dominanceRatio,
                         size(genderCounts) AS numberOfGenders
                    WHERE numberOfGenders > 1
                    RETURN tagName, mostCommonGender, count, dominanceRatio
                    ORDER BY count DESC`
        );

        const tagGenderData = result.records.map(record => ({
            tagName: record.get('tagName'),
            mostCommonGender: record.get('mostCommonGender'),
            count: record.get('count').toNumber(),
            dominanceRatio: record.get('dominanceRatio')
        }));

        res.json({ tags: tagGenderData });

        await session.close();
    } catch (err) {
        console.error('Error in tag-gender-interest query:', err);
        res.status(500).json({ error: err.message });
    }
});

module.exports = router;