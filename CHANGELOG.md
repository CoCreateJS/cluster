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
