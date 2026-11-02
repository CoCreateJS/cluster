export default {
    "organization_id": "",
    "key": "",
    "host": "",
    "directories": [
        {
            "entry": "./demo",
            "array": "files",
            "object": {
                "name": "{{name}}",
                "src": "{{source}}",
                "host": [
                    "*"
                ],
                "directory": "/demo/cluster/{{directory}}",
                "path": "{{path}}",
                "pathname": "{{pathname}}",
                "content-type": "{{content-type}}",
                "public": "true"
            }
        }
    ],
    "sources": [
        {
            "array": "files",
            "object": {
                "_id": "61a12db2a8b6b4001a9f5a2e",
                "name": "index.html",
                "path": "/docs/cluster",
                "pathname": "/docs/cluster/index.html",
                "src": "{{./docs/index.html}}",
                "host": [
                    "*"
                ],
                "directory": "cluster",
                "content-type": "{{content-type}}",
                "public": "true"
            }
        }
    ],
    modules: {
        // --- Database & Storage Modules ---
        "amazon-redshift": {
            import: "@cocreate/amazon-redshift",
            path: "../../CoCreate-storages/amazon-redshift",
            repo: "github.com/CoCreateJS/CoCreate-amazon-redshift.git"
        },
        auroradb: {
            import: "@cocreate/auroradb",
            path: "../../CoCreate-storages/auroradb",
            repo: "github.com/CoCreateJS/CoCreate-auroradb.git"
        },
        cockroachdb: {
            import: "@cocreate/cockroachdb",
            path: "../../CoCreate-storages/cockroachdb",
            repo: "github.com/CoCreateJS/CoCreate-cockroachdb.git"
        },
        cosmosdb: {
            import: "@cocreate/cosmosdb",
            path: "../../CoCreate-storages/cosmosdb",
            repo: "github.com/CoCreateJS/CoCreate-cosmosdb.git"
        },
        documentdb: {
            import: "@cocreate/documentdb",
            path: "../../CoCreate-storages/documentdb",
            repo: "github.com/CoCreateJS/CoCreate-documentdb.git"
        },
        dqlite: {
            import: "@cocreate/dqlite",
            path: "../../CoCreate-storages/dqlite",
            repo: "github.com/CoCreateJS/CoCreate-dqlite.git"
        },
        dynamodb: {
            import: "@cocreate/dynamodb",
            path: "../../CoCreate-storages/dynamodb",
            repo: "github.com/CoCreateJS/CoCreate-dynamodb.git"
        },
        ferretdb: {
            import: "@cocreate/ferretdb",
            path: "../../CoCreate-storages/ferretdb",
            repo: "github.com/CoCreateJS/CoCreate-ferretdb.git"
        },
        firestore: {
            import: "@cocreate/firestore",
            path: "../../CoCreate-storages/firestore",
            repo: "github.com/CoCreateJS/CoCreate-firestore.git"
        },
        mariadb: {
            import: "@cocreate/mariadb",
            path: "../../CoCreate-storages/mariadb",
            repo: "github.com/CoCreateJS/CoCreate-mariadb.git"
        },
        mongodb: {
            import: "@cocreate/mongodb",
            path: "../../CoCreate-storages/mongodb",
            repo: "github.com/CoCreateJS/CoCreate-mongodb.git"
        },
        mysql: {
            import: "@cocreate/mysql",
            path: "../../CoCreate-storages/mysql",
            repo: "github.com/CoCreateJS/CoCreate-mysql.git"
        },
        oracledb: {
            import: "@cocreate/oracledb",
            path: "../../CoCreate-storages/oracledb",
            repo: "github.com/CoCreateJS/CoCreate-oracledb.git"
        },
        planetscale: {
            import: "@cocreate/planetscale",
            path: "../../CoCreate-storages/planetscale",
            repo: "github.com/CoCreateJS/CoCreate-planetscale.git"
        },
        postgresql: {
            import: "@cocreate/postgresql",
            path: "../../CoCreate-storages/postgresql",
            repo: "github.com/CoCreateJS/CoCreate-postgresql.git"
        },
        rqlite: {
            import: "@cocreate/rqlite",
            path: "../../CoCreate-storages/rqlite",
            repo: "github.com/CoCreateJS/CoCreate-rqlite.git"
        },
        qdrant: {
            import: "@cocreate/qdrant",
            path: "../../CoCreate-storages/qdrant",
            repo: "github.com/CoCreateJS/CoCreate-qdrant.git"
        },
        sqlite: {
            import: "@cocreate/sqlite",
            path: "../../CoCreate-storages/sqlite",
            repo: "github.com/CoCreateJS/CoCreate-sqlite.git"
        },
        tidb: {
            import: "@cocreate/tidb",
            path: "../../CoCreate-storages/tidb",
            repo: "github.com/CoCreateJS/CoCreate-tidb.git"
        },
        timescaledb: {
            import: "@cocreate/timescaledb",
            path: "../../CoCreate-storages/timescaledb",
            repo: "github.com/CoCreateJS/CoCreate-timescaledb.git"
        },
        turso: {
            import: "@cocreate/turso",
            path: "../../CoCreate-storages/turso",
            repo: "github.com/CoCreateJS/CoCreate-turso.git"
        },
        yugabytedb: {
            import: "@cocreate/yugabytedb",
            path: "../../CoCreate-storages/yugabytedb",
            repo: "github.com/CoCreateJS/CoCreate-yugabytedb.git"
        }
    }
};