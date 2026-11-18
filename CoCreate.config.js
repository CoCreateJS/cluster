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
    ]
};
