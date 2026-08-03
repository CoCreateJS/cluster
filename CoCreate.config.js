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
                "directory": "/demo/server/{{directory}}",
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
                "path": "/docs/server",
                "pathname": "/docs/server/index.html",
                "src": "{{./docs/index.html}}",
                "host": [
                    "*"
                ],
                "directory": "server",
                "content-type": "{{content-type}}",
                "public": "true"
            }
        }
    ],
    modules: {
        // --- Database & Storage Modules ---
        "amazon-redshift": {
            import: "@cocreate/amazon-redshift",
            path: "../../CoCreate-storages/CoCreate-amazon-redshift",
            repo: "github.com/CoCreate-app/CoCreate-amazon-redshift.git"
        },
        auroradb: {
            import: "@cocreate/auroradb",
            path: "../../CoCreate-storages/CoCreate-auroradb",
            repo: "github.com/CoCreate-app/CoCreate-auroradb.git"
        },
        cockroachdb: {
            import: "@cocreate/cockroachdb",
            path: "../../CoCreate-storages/CoCreate-cockroachdb",
            repo: "github.com/CoCreate-app/CoCreate-cockroachdb.git"
        },
        cosmosdb: {
            import: "@cocreate/cosmosdb",
            path: "../../CoCreate-storages/CoCreate-cosmosdb",
            repo: "github.com/CoCreate-app/CoCreate-cosmosdb.git"
        },
        documentdb: {
            import: "@cocreate/documentdb",
            path: "../../CoCreate-storages/CoCreate-documentdb",
            repo: "github.com/CoCreate-app/CoCreate-documentdb.git"
        },
        dqlite: {
            import: "@cocreate/dqlite",
            path: "../../CoCreate-storages/CoCreate-dqlite",
            repo: "github.com/CoCreate-app/CoCreate-dqlite.git"
        },
        dynamodb: {
            import: "@cocreate/dynamodb",
            path: "../../CoCreate-storages/CoCreate-dynamodb",
            repo: "github.com/CoCreate-app/CoCreate-dynamodb.git"
        },
        ferretdb: {
            import: "@cocreate/ferretdb",
            path: "../../CoCreate-storages/CoCreate-ferretdb",
            repo: "github.com/CoCreate-app/CoCreate-ferretdb.git"
        },
        firestore: {
            import: "@cocreate/firestore",
            path: "../../CoCreate-storages/CoCreate-firestore",
            repo: "github.com/CoCreate-app/CoCreate-firestore.git"
        },
        mariadb: {
            import: "@cocreate/mariadb",
            path: "../../CoCreate-storages/CoCreate-mariadb",
            repo: "github.com/CoCreate-app/CoCreate-mariadb.git"
        },
        mongodb: {
            import: "@cocreate/mongodb",
            path: "../../CoCreate-storages/CoCreate-mongodb",
            repo: "github.com/CoCreate-app/CoCreate-mongodb.git"
        },
        mysql: {
            import: "@cocreate/mysql",
            path: "../../CoCreate-storages/CoCreate-mysql",
            repo: "github.com/CoCreate-app/CoCreate-mysql.git"
        },
        oracledb: {
            import: "@cocreate/oracledb",
            path: "../../CoCreate-storages/CoCreate-oracledb",
            repo: "github.com/CoCreate-app/CoCreate-oracledb.git"
        },
        planetscale: {
            import: "@cocreate/planetscale",
            path: "../../CoCreate-storages/CoCreate-planetscale",
            repo: "github.com/CoCreate-app/CoCreate-planetscale.git"
        },
        postgresql: {
            import: "@cocreate/postgresql",
            path: "../../CoCreate-storages/CoCreate-postgresql",
            repo: "github.com/CoCreate-app/CoCreate-postgresql.git"
        },
        rqlite: {
            import: "@cocreate/rqlite",
            path: "../../CoCreate-storages/CoCreate-rqlite",
            repo: "github.com/CoCreate-app/CoCreate-rqlite.git"
        },
        qdrant: {
            import: "@cocreate/qdrant",
            path: "../../CoCreate-storages/CoCreate-qdrant",
            repo: "github.com/CoCreate-app/CoCreate-qdrant.git"
        },
        sqlite: {
            import: "@cocreate/sqlite",
            path: "../../CoCreate-storages/CoCreate-sqlite",
            repo: "github.com/CoCreate-app/CoCreate-sqlite.git"
        },
        tidb: {
            import: "@cocreate/tidb",
            path: "../../CoCreate-storages/CoCreate-tidb",
            repo: "github.com/CoCreate-app/CoCreate-tidb.git"
        },
        timescaledb: {
            import: "@cocreate/timescaledb",
            path: "../../CoCreate-storages/CoCreate-timescaledb",
            repo: "github.com/CoCreate-app/CoCreate-timescaledb.git"
        },
        turso: {
            import: "@cocreate/turso",
            path: "../../CoCreate-storages/CoCreate-turso",
            repo: "github.com/CoCreate-app/CoCreate-turso.git"
        },
        yugabytedb: {
            import: "@cocreate/yugabytedb",
            path: "../../CoCreate-storages/CoCreate-yugabytedb",
            repo: "github.com/CoCreate-app/CoCreate-yugabytedb.git"
        }
    }
};