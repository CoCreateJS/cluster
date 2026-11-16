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
            path: "../../storages/amazon-redshift",
            repo: "github.com/CoCreateJS/CoCreate-amazon-redshift.git"
        },
        auroradb: {
            import: "@cocreate/auroradb",
            path: "../../storages/auroradb",
            repo: "github.com/CoCreateJS/CoCreate-auroradb.git"
        },
        cockroachdb: {
            import: "@cocreate/cockroachdb",
            path: "../../storages/cockroachdb",
            repo: "github.com/CoCreateJS/CoCreate-cockroachdb.git"
        },
        cosmosdb: {
            import: "@cocreate/cosmosdb",
            path: "../../storages/cosmosdb",
            repo: "github.com/CoCreateJS/CoCreate-cosmosdb.git"
        },
        documentdb: {
            import: "@cocreate/documentdb",
            path: "../../storages/documentdb",
            repo: "github.com/CoCreateJS/CoCreate-documentdb.git"
        },
        dqlite: {
            import: "@cocreate/dqlite",
            path: "../../storages/dqlite",
            repo: "github.com/CoCreateJS/CoCreate-dqlite.git"
        },
        dynamodb: {
            import: "@cocreate/dynamodb",
            path: "../../storages/dynamodb",
            repo: "github.com/CoCreateJS/CoCreate-dynamodb.git"
        },
        ferretdb: {
            import: "@cocreate/ferretdb",
            path: "../../storages/ferretdb",
            repo: "github.com/CoCreateJS/CoCreate-ferretdb.git"
        },
        firestore: {
            import: "@cocreate/firestore",
            path: "../../storages/firestore",
            repo: "github.com/CoCreateJS/CoCreate-firestore.git"
        },
        mariadb: {
            import: "@cocreate/mariadb",
            path: "../../storages/mariadb",
            repo: "github.com/CoCreateJS/CoCreate-mariadb.git"
        },
        mongodb: {
            import: "@cocreate/mongodb",
            path: "../../storages/mongodb",
            repo: "github.com/CoCreateJS/CoCreate-mongodb.git"
        },
        mysql: {
            import: "@cocreate/mysql",
            path: "../../storages/mysql",
            repo: "github.com/CoCreateJS/CoCreate-mysql.git"
        },
        oracledb: {
            import: "@cocreate/oracledb",
            path: "../../storages/oracledb",
            repo: "github.com/CoCreateJS/CoCreate-oracledb.git"
        },
        planetscale: {
            import: "@cocreate/planetscale",
            path: "../../storages/planetscale",
            repo: "github.com/CoCreateJS/CoCreate-planetscale.git"
        },
        postgresql: {
            import: "@cocreate/postgresql",
            path: "../../storages/postgresql",
            repo: "github.com/CoCreateJS/CoCreate-postgresql.git"
        },
        rqlite: {
            import: "@cocreate/rqlite",
            path: "../../storages/rqlite",
            repo: "github.com/CoCreateJS/CoCreate-rqlite.git"
        },
        qdrant: {
            import: "@cocreate/qdrant",
            path: "../../storages/qdrant",
            repo: "github.com/CoCreateJS/CoCreate-qdrant.git"
        },
        sqlite: {
            import: "@cocreate/sqlite",
            path: "../../storages/sqlite",
            repo: "github.com/CoCreateJS/CoCreate-sqlite.git"
        },
        tidb: {
            import: "@cocreate/tidb",
            path: "../../storages/tidb",
            repo: "github.com/CoCreateJS/CoCreate-tidb.git"
        },
        timescaledb: {
            import: "@cocreate/timescaledb",
            path: "../../storages/timescaledb",
            repo: "github.com/CoCreateJS/CoCreate-timescaledb.git"
        },
        turso: {
            import: "@cocreate/turso",
            path: "../../storages/turso",
            repo: "github.com/CoCreateJS/CoCreate-turso.git"
        },
        yugabytedb: {
            import: "@cocreate/yugabytedb",
            path: "../../storages/yugabytedb",
            repo: "github.com/CoCreateJS/CoCreate-yugabytedb.git"
        }
    }
};