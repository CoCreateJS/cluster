# [2.0.0](https://github.com/CoCreateJS/cluster/compare/v1.8.0...v2.0.0) (2026-11-02)


### Features

* align CoCreate repository and package naming ([c262a60](https://github.com/CoCreateJS/cluster/commit/c262a606f628c4a8ca1d1e928654d625e6ac8586))


### BREAKING CHANGES

* consumers must use the renamed CoCreate package and repository identities; the previous prefixed names are no longer supported.

# [1.8.0](https://github.com/CoCreateJS/cluster/compare/v1.7.0...v1.8.0) (2026-11-02)


### Bug Fixes

* missing dependencies ([eda2bd7](https://github.com/CoCreateJS/cluster/commit/eda2bd768c5e85ec66d9c82a21a5d78e7f77a0e3))
* **server:** update renamed server package references ([26fd600](https://github.com/CoCreateJS/cluster/commit/26fd60050e44b20f4e9c80153721de2c6c5e674b))


### Features

* initialize workflow server ([3fbaf6e](https://github.com/CoCreateJS/cluster/commit/3fbaf6e9ef41ac9d0c86ab1e9cc96c5fa5b8ce51))

# [1.7.0](https://github.com/CoCreateJS/cluster/compare/v1.6.1...v1.7.0) (2026-11-01)


### Bug Fixes

* add Deno specific entries to .gitignore ([7429985](https://github.com/CoCreateJS/cluster/commit/742998512833235d3836cf62379ec92984aa346b))
* api.webhook ([5b6bed0](https://github.com/CoCreateJS/cluster/commit/5b6bed0f4df430deacefc98214d7035c1ddc2d92))
* bump [@cocreate](https://github.com/cocreate) dependencies ([d9b5875](https://github.com/CoCreateJS/cluster/commit/d9b5875babb57b71c4b6b462d9e36e192034d241))
* bump versions ([222bdf6](https://github.com/CoCreateJS/cluster/commit/222bdf6e17d29c1c24ad44bc3f07393fb418fd33))
* bump versions ([108cbc1](https://github.com/CoCreateJS/cluster/commit/108cbc15506d97fc7c34bf97e5a117da2a85bdc7))
* enhance logging for primary and worker processes, update UUID generation method ([a675550](https://github.com/CoCreateJS/cluster/commit/a67555036405f979a74fcf3246a9c2a59b5af8ea))
* implement fallback servers for primary node and enhance edge listener management ([7f51f8f](https://github.com/CoCreateJS/cluster/commit/7f51f8f818bdad826ed3a8dec015ae5aa8df741f))
* instance renamed to vm ([9118ce2](https://github.com/CoCreateJS/cluster/commit/9118ce2371788c35c5c89dcedeea8100fc872873))
* move HTTP edge listener setup to HTTPS initialization and update server ID handling ([feeba7c](https://github.com/CoCreateJS/cluster/commit/feeba7cb47b6033e29deae8a1a91264f3591311d))
* persist server lifecycle state ([8a76f8a](https://github.com/CoCreateJS/cluster/commit/8a76f8a2f174fc226c19aacd67687faa6bb0980a))
* remove outdated dependency for @cocreate/server in package.json ([7d2d5e5](https://github.com/CoCreateJS/cluster/commit/7d2d5e57cef728ab21ad5be3717b3c1eb40d10fb))
* remove trailing slash from node_modules entry in .gitignore ([29b2eef](https://github.com/CoCreateJS/cluster/commit/29b2eef91ed6c82cf7b6b0d0c84a6b815b7563d0))
* remove unnecessary log statement for worker state reset in CoCreateServer ([6931ac4](https://github.com/CoCreateJS/cluster/commit/6931ac4247172b0973cccb681bed787f443e3597))
* reset persisted worker profiles before startup ([92f6731](https://github.com/CoCreateJS/cluster/commit/92f6731f4cd9de7ce3e68c1ed44336e8f855a021))
* stamp worker origin in single-process IPC sends ([55195b0](https://github.com/CoCreateJS/cluster/commit/55195b01e87cb2d58945d6726c1c4683d6c52de5))
* update @cocreate/acme dependency version to ^1.5.2 and ensure consistency in package.json ([310aae4](https://github.com/CoCreateJS/cluster/commit/310aae45ea6fe5a00561bb3264527a449f2876f7))
* update @cocreate/crud-server dependency to version 1.39.0 ([36757bd](https://github.com/CoCreateJS/cluster/commit/36757bd2f083ba3a5b8aa1f69bd028052aa759e0))
* update Config initialization to use request method for improved configuration handling ([b49e201](https://github.com/CoCreateJS/cluster/commit/b49e201ee669d9463d2365b2ba42087d8c23aae3))
* update git url ([c5ece6a](https://github.com/CoCreateJS/cluster/commit/c5ece6a97bb598f7d05447bdda792f97a1f343c1))
* update GitHub Actions to use latest versions and sync package.json description ([6e3b595](https://github.com/CoCreateJS/cluster/commit/6e3b5951436907d2c95ef1e1cb1bd27a98adb442))
* update process label format in isCordon logging for clarity ([3c25928](https://github.com/CoCreateJS/cluster/commit/3c2592853f72013fa9b7482b08617f86b60c2d60))
* update server state handling ([ee89353](https://github.com/CoCreateJS/cluster/commit/ee893536aad42c28c44aa11405d64e34da592047))
* update TLS dependency and improve route handling logic ([5427923](https://github.com/CoCreateJS/cluster/commit/5427923c158e3cee1095fee0079dfa7ee0546ab6))


### Features

* add TLSManager for handling SSL certificates and HTTPS options ([d61a879](https://github.com/CoCreateJS/cluster/commit/d61a879c4021f3f4349d23bd88afc71363b75f38))
* added storage artifacts ([2bf1d89](https://github.com/CoCreateJS/cluster/commit/2bf1d89c241936168ae9e610ee9babacfd04ca9a))
* bump all dependencies ([7b29af3](https://github.com/CoCreateJS/cluster/commit/7b29af312f142b9ce52a53ed97215c32a23d9c05))
* bumped all dependency versions ([00994ac](https://github.com/CoCreateJS/cluster/commit/00994acfddc828812ada4af3d6d49fb307e6ef8d))
* enhance state management and shutdown handling in CoCreateServer ([8dff3c5](https://github.com/CoCreateJS/cluster/commit/8dff3c5031b0985ba845f85faa61a761c65c41f7))
* expand server message routing and state controls ([3db28a6](https://github.com/CoCreateJS/cluster/commit/3db28a67a3faf1742a07824ece9c14a0b87b055b))
* implement ClusterStateManager for robust state management across cluster nodes ([8f1a1f6](https://github.com/CoCreateJS/cluster/commit/8f1a1f69b8eff63faf0e1ffdbcd09937f9498552))
* implement StateManager for enhanced server state management and lifecycle control ([7f09eba](https://github.com/CoCreateJS/cluster/commit/7f09ebabdcdc96809684027cf41abc80d7810ca1))
* improved init and api ([2ade021](https://github.com/CoCreateJS/cluster/commit/2ade021a3a17e201b782fa6ff5333911a2705a09))
* integrate usage tracking into CoCreateServer ([c07224f](https://github.com/CoCreateJS/cluster/commit/c07224fbd7b8771f86edd54dfaf434f22b1acf21))
* overhaul state management and remove TLS module ([c037371](https://github.com/CoCreateJS/cluster/commit/c0373716413e8b7742974810af11379fa114a1b0))
* Refactor cluster state management and introduce IPC and storage modules ([6700f49](https://github.com/CoCreateJS/cluster/commit/6700f4912a47cd0722d6416a10f25b0353dfdcf0))
* refactor CoCreateServer to use 'master' terminology and introduce LogManager for centralized logging ([1dc9162](https://github.com/CoCreateJS/cluster/commit/1dc91629b9524b386b0229c4fc192082de2fbd2b))
* refactor server to use getInfrastructure and include location in state management ([1a9e14e](https://github.com/CoCreateJS/cluster/commit/1a9e14ed67536b7165e3c2c8d132fb7d592b9d45))
* refactor Storage class to use 'server' terminology and improve state synchronization handling ([97fdcd9](https://github.com/CoCreateJS/cluster/commit/97fdcd93a6cd4783d7668702ccbdbe8a9bb721ea))
* refine organization IPC routing ([5432d10](https://github.com/CoCreateJS/cluster/commit/5432d1082b7ac56c97cb5871b248760cc51cc3df))
* remove console log statements for cleaner server and state management ([3c395fe](https://github.com/CoCreateJS/cluster/commit/3c395fef1052c515bef91e63881ab9fd7748767a))
* rename Acme to Certificates and enhance SNI support in HTTPS options ([2573df0](https://github.com/CoCreateJS/cluster/commit/2573df03a428e4b81ee6b100908c165c1f562959))
* Replace acme with certificates ([adcdf0b](https://github.com/CoCreateJS/cluster/commit/adcdf0b77b7709ef09a0dbeecae4dcfbe64408f6))
* requestHandler return gates and resoponse if organization balance 0 ([336ca81](https://github.com/CoCreateJS/cluster/commit/336ca8178bdf7a1013a690d4c2b583df663d2283))
* streamline CoCreateServer initialization by removing unused properties and enhancing worker service setup ([74b5fc0](https://github.com/CoCreateJS/cluster/commit/74b5fc0dd708de6d0980efd719cc5695132fdff8))
* unify IPC routing across workers and servers ([6c14bc1](https://github.com/CoCreateJS/cluster/commit/6c14bc187621ee9d7cde76bde5c310ff12122ab6))
* update IP retrieval to use getClientInfo for improved accuracy ([a0e4c8c](https://github.com/CoCreateJS/cluster/commit/a0e4c8c5b5eb75565389e50fdaf8781a299a3442))
* update redirect handling to use 301 for HTTPS and improve fallback response ([f4ce5ce](https://github.com/CoCreateJS/cluster/commit/f4ce5ced60d3616a6cd0d74ca52f2ea7c185b0ee))

## [1.6.1](https://github.com/CoCreate-app/CoCreate-server/compare/v1.6.0...v1.6.1) (2026-07-19)


### Bug Fixes

* set default branch to main ([37bf6bb](https://github.com/CoCreate-app/CoCreate-server/commit/37bf6bb580d5d73126b513060df3496da5ab4600))

# [1.6.0](https://github.com/CoCreate-app/CoCreate-server/compare/v1.5.1...v1.6.0) (2026-07-18)


### Features

* add ingress state management with cordon, shedding, and draining events ([b52a198](https://github.com/CoCreate-app/CoCreate-server/commit/b52a1986c566fe9a73c8e694542be69d808ae3eb))
* implement graceful shutdown process with selective shedding and draining for improved resource management ([5c367e0](https://github.com/CoCreate-app/CoCreate-server/commit/5c367e0284f587965bd5b4248b1f1f97a4b51b2f))

## [1.5.1](https://github.com/CoCreate-app/CoCreate-server/compare/v1.5.0...v1.5.1) (2026-07-17)


### Bug Fixes

* bump dependencies ([347c732](https://github.com/CoCreate-app/CoCreate-server/commit/347c73285a45f956d489c58470b9e2753f3a7acd))

# [1.5.0](https://github.com/CoCreate-app/CoCreate-server/compare/v1.4.0...v1.5.0) (2026-07-17)


### Features

* enhance worker configuration to reserve CPU core for master process ([0a05a51](https://github.com/CoCreate-app/CoCreate-server/commit/0a05a516b4917cafd508a81443855fb5956348bc))
* replace Metrics with ServerTelemetry for enhanced performance monitoring ([c1de789](https://github.com/CoCreate-app/CoCreate-server/commit/c1de789aa938bc1fa07b95e40728c4fe348ace6b))
* replace SocketMesh with ServerMesh and add ServerAutoscaler for improved orchestration ([02ac503](https://github.com/CoCreate-app/CoCreate-server/commit/02ac503f709a6d8d4dd680f5d660fc7025cecb1d))
* set workerId for single Primary process and ensure SocketMesh initialization ([d80084a](https://github.com/CoCreate-app/CoCreate-server/commit/d80084af3e2b72ffa30157be9091d0110302d882))

# [1.4.0](https://github.com/CoCreate-app/CoCreate-server/compare/v1.3.0...v1.4.0) (2026-07-16)


### Features

* enhance CoCreateServer with IPC routing and edge request handling ([4da9381](https://github.com/CoCreate-app/CoCreate-server/commit/4da9381214806b022c129a2d1a25a1a7ad0a6276))
* update automated workflow and release configuration for improved semantic release handling ([8d442c2](https://github.com/CoCreate-app/CoCreate-server/commit/8d442c243f05a9daa8cdcd3241f26f4dfb18022b))

# [1.3.0](https://github.com/CoCreate-app/CoCreate-server/compare/v1.2.2...v1.3.0) (2026-07-15)


### Features

* add infrastructure and IP resolution modules for cloud provider metadata retrieval ([bb7b6cf](https://github.com/CoCreate-app/CoCreate-server/commit/bb7b6cf06a8dcbae0f68c8d3c644ffc40d553d97))
* enhance server initialization with improved configuration handling and dynamic module imports ([9bd44c3](https://github.com/CoCreate-app/CoCreate-server/commit/9bd44c32ad3be2274b53a0fc859bb05346c34f91))
* integrate Acme, Nginx, SocketMesh, and Metrics modules for enhanced server capabilities ([1d84a1c](https://github.com/CoCreate-app/CoCreate-server/commit/1d84a1c061d8421433fcb93d183e768d63eada10))

## [1.2.2](https://github.com/CoCreate-app/CoCreate-server/compare/v1.2.1...v1.2.2) (2026-07-14)


### Bug Fixes

* refactor server initialization to include API server and improve module loading ([97fe5bf](https://github.com/CoCreate-app/CoCreate-server/commit/97fe5bfa94b98020d042024e3969312684dd7a68))
* removed post install ([7ecf61a](https://github.com/CoCreate-app/CoCreate-server/commit/7ecf61ae0e5dd37244ecdbfd9c1cf434ef36e676))
* semantic version handling. Reorganize .gitignore for improved clarity and structure ([27f9506](https://github.com/CoCreate-app/CoCreate-server/commit/27f9506ddb71641dc93b32cef6224a194c8bd009))
* Update GitHub Actions workflow to use latest versions of actions and Node.js ([043f46e](https://github.com/CoCreate-app/CoCreate-server/commit/043f46e0a3a16c8186ebc7189e00fdfbffd201f7))
* update module export to ES6 syntax in release.config.js ([2643676](https://github.com/CoCreate-app/CoCreate-server/commit/2643676e58b1ceec917da4780e569b55d65d5abb))
* update package.json and refactor index.js for module compatibility ([0e21383](https://github.com/CoCreate-app/CoCreate-server/commit/0e21383964b0280f020d9da82058133e7b4d7eed))

## [1.2.1](https://github.com/CoCreate-app/CoCreate-server/compare/v1.2.0...v1.2.1) (2025-05-01)


### Bug Fixes

* query attributes ([e9d7101](https://github.com/CoCreate-app/CoCreate-server/commit/e9d7101713b387b6a61a10141290d630bc288a36))
* update [@cocreate](https://github.com/cocreate) dependencies ([b703fc3](https://github.com/CoCreate-app/CoCreate-server/commit/b703fc308a0ef9a29573a709859a8254579b8cff))

# [1.2.0](https://github.com/CoCreate-app/CoCreate-server/compare/v1.1.7...v1.2.0) (2024-11-04)


### Bug Fixes

* prettier.config options ([7efaaaf](https://github.com/CoCreate-app/CoCreate-server/commit/7efaaaf1af097d760e344c6c36f49ee41db1408b))


### Features

* add prettier.config.js and format files ([eeb944d](https://github.com/CoCreate-app/CoCreate-server/commit/eeb944df0bbf418801439627c2e0cf29ad5c4581))

## [1.1.7](https://github.com/CoCreate-app/CoCreate-server/compare/v1.1.6...v1.1.7) (2024-06-23)


### Bug Fixes

* bump dependencies ([0bbc860](https://github.com/CoCreate-app/CoCreate-server/commit/0bbc860bf23e7011f17a975ae61ef9a6814da44b))

## [1.1.6](https://github.com/CoCreate-app/CoCreate-server/compare/v1.1.5...v1.1.6) (2024-06-23)


### Bug Fixes

* update dependencies ([b053ecf](https://github.com/CoCreate-app/CoCreate-server/commit/b053ecfe70ecea985a0a7d55758c4ef384dfaac5))

## [1.1.5](https://github.com/CoCreate-app/CoCreate-server/compare/v1.1.4...v1.1.5) (2024-06-23)


### Bug Fixes

* demo css ([80eac38](https://github.com/CoCreate-app/CoCreate-server/commit/80eac38203ecad232206594177a7c7c3194f50d9))

## [1.1.4](https://github.com/CoCreate-app/CoCreate-server/compare/v1.1.3...v1.1.4) (2024-03-18)


### Bug Fixes

* replaced keyword spaces with hyphens and lowercase letters only ([0430ae6](https://github.com/CoCreate-app/CoCreate-server/commit/0430ae6a5deb4afa5affda493dac0abc1e23e807))

## [1.1.3](https://github.com/CoCreate-app/CoCreate-server/compare/v1.1.2...v1.1.3) (2024-03-18)


### Bug Fixes

* description ([4a7044a](https://github.com/CoCreate-app/CoCreate-server/commit/4a7044ab74efda9d020cfcb570396432ea1e24ec))

## [1.1.2](https://github.com/CoCreate-app/CoCreate-server/compare/v1.1.1...v1.1.2) (2024-02-05)


### Bug Fixes

* Removed https://cdn.cocreate.app/latest/CoCreate.min.css ([3c43ac8](https://github.com/CoCreate-app/CoCreate-server/commit/3c43ac8338775f6914cbe3511b79ac00154dde26))

## [1.1.1](https://github.com/CoCreate-app/CoCreate-server/compare/v1.1.0...v1.1.1) (2024-01-08)


### Bug Fixes

* return if ssl false ([500e7c7](https://github.com/CoCreate-app/CoCreate-server/commit/500e7c7898f3a3a5ecc676ca68cd105cc1f29a6f))

# [1.1.0](https://github.com/CoCreate-app/CoCreate-server/compare/v1.0.2...v1.1.0) (2024-01-08)


### Bug Fixes

* update server object properties ([762b209](https://github.com/CoCreate-app/CoCreate-server/commit/762b209ffbb22ebf9bb992d14f96eeeac2391bda))


### Features

* bumped CoCreate dependencies to their latest versions ([e6f76a5](https://github.com/CoCreate-app/CoCreate-server/commit/e6f76a57be9e4581c82fcf3e8e57768df35dfcd8))

## [1.0.2](https://github.com/CoCreate-app/CoCreate-server/compare/v1.0.1...v1.0.2) (2024-01-04)


### Bug Fixes

* add '@cocreate/certificates' ([73557b9](https://github.com/CoCreate-app/CoCreate-server/commit/73557b97d0a8f47d5cbb2b0b6f66dc87b5aa7a29))

## [1.0.1](https://github.com/CoCreate-app/CoCreate-server/compare/v1.0.0...v1.0.1) (2024-01-02)


### Bug Fixes

* remove dependencies ([91eb9f2](https://github.com/CoCreate-app/CoCreate-server/commit/91eb9f2643b154590ea58a431930b137645fd8e3))

# 1.0.0 (2024-01-02)


### Features

* Initial Release ([b76695c](https://github.com/CoCreate-app/CoCreate-server/commit/b76695cc4c04692a415f7a920369111507ec52a7))
