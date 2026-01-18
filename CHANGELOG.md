# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.0](https://github.com/FalkorDB/falkordb-rs/releases/tag/v0.2.0) - 2026-01-18

### Other

- update cargo lock ([#154](https://github.com/FalkorDB/falkordb-rs/pull/154))
- Expose UDF API for loading, listing, and managing user-defined functions ([#152](https://github.com/FalkorDB/falkordb-rs/pull/152))
- *(deps)* bump strum from 0.27.1 to 0.27.2
- Merge branch 'main' into dependabot/cargo/main/regex-1.12.2
- *(deps)* bump which from 7.0.3 to 8.0.0
- Merge branch 'main' into dependabot/cargo/main/thiserror-2.0.17
- Merge branch 'main' into dependabot/github_actions/main/actions/checkout-6
- *(deps)* bump actions/checkout from 4 to 6
- Fix cargo-deny action to skip advisory database check ([#146](https://github.com/FalkorDB/falkordb-rs/pull/146))
- Add support for embedded FalkorDB server with comprehensive test coverage ([#135](https://github.com/FalkorDB/falkordb-rs/pull/135))
- Update wordlist.txt
- Create spellcheck.yml ([#107](https://github.com/FalkorDB/falkordb-rs/pull/107))
- clean deny errors
- update deps

## [0.1.11](https://github.com/FalkorDB/falkordb-rs/compare/v0.1.10...v0.1.11) - 2025-02-13

### Fixed

- fix deny errors and warns

### Other

- *(deps)* bump thiserror from 2.0.6 to 2.0.11 (#81)
- Fix example add mut to node ([#88](https://github.com/FalkorDB/falkordb-rs/pull/88))
- update lock file
- *(deps)* bump redis from 0.28.1 to 0.28.2
- Merge branch 'main' into dependabot/cargo/main/redis-0.28.1
- Update coverage.yml
- *(deps)* bump tokio from 1.42.0 to 1.43.0

## [0.1.10](https://github.com/FalkorDB/falkordb-rs/compare/v0.1.9...v0.1.10) - 2024-12-10

### Fixed

- fix read only query builder ([#77](https://github.com/FalkorDB/falkordb-rs/pull/77))

### Other

- *(deps)* bump codecov/codecov-action from 4 to 5 (#71)

## [0.1.9](https://github.com/FalkorDB/falkordb-rs/compare/v0.1.8...v0.1.9) - 2024-12-04

### Other

- Update dependencies ([#74](https://github.com/FalkorDB/falkordb-rs/pull/74))
- Fix async connection leak and co ([#72](https://github.com/FalkorDB/falkordb-rs/pull/72))

## [0.1.8](https://github.com/FalkorDB/falkordb-rs/compare/v0.1.7...v0.1.8) - 2024-11-14

### Other

- Allow to trigger github workflow manually ([#68](https://github.com/FalkorDB/falkordb-rs/pull/68))
- Update version in readme

## [0.1.7](https://github.com/FalkorDB/falkordb-rs/compare/v0.1.6...v0.1.7) - 2024-11-14

### Other

- Update coverage.yml ([#55](https://github.com/FalkorDB/falkordb-rs/pull/55))
- Add test for when sending cypher query with syntax error the result s… ([#67](https://github.com/FalkorDB/falkordb-rs/pull/67))
- Update dependencies ([#66](https://github.com/FalkorDB/falkordb-rs/pull/66))
- Pass redis error to upstream instead of sending the generic FalkorDBError::ParsingArray ([#64](https://github.com/FalkorDB/falkordb-rs/pull/64))

## [0.1.6](https://github.com/FalkorDB/falkordb-rs/compare/v0.1.5...v0.1.6) - 2024-10-14

### Other

- Prepare for release v0.1.6
- Re add fields to FalkorIndex, test.
- Fix error message of ParsingArrayToStructElementCount
- Improve Vec32 parsing and add comprehensive tests
- Fix failing unit tests, add more vec32 test
- add support with the new vec32

## [0.1.5](https://github.com/FalkorDB/falkordb-rs/compare/v0.1.4...v0.1.5) - 2024-08-15

### Fixed
- Update crates to avoid CVEs ([#35](https://github.com/FalkorDB/falkordb-rs/pull/35))

### Other
- crates io bug
- Update deny action
- Update redis and tokio version, and fix compatibility issues
- Update redis and tokio version, and fix compatibility issues
- Update README
- Remove deny from needs
- *(deps)* bump thiserror from 1.0.61 to 1.0.62 ([#30](https://github.com/FalkorDB/falkordb-rs/pull/30))
- *(deps)* bump strum from 0.26.2 to 0.26.3 ([#28](https://github.com/FalkorDB/falkordb-rs/pull/28))

## [0.1.4](https://github.com/FalkorDB/falkordb-rs/compare/v0.1.3...v0.1.4) - 2024-06-20

### Fixed
- Use MIT license ([#25](https://github.com/FalkorDB/falkordb-rs/pull/25))

## [0.1.3](https://github.com/FalkorDB/falkordb-rs/compare/v0.1.2...v0.1.3) - 2024-06-18

### Fixed
- Add codecov yaml
- outdated README

## [0.1.2](https://github.com/FalkorDB/falkordb-rs/compare/v0.1.1...v0.1.2) - 2024-06-18

### Added
- Implement Async Graph API ([#23](https://github.com/FalkorDB/falkordb-rs/pull/23))
- implement QueryResult properly ([#19](https://github.com/FalkorDB/falkordb-rs/pull/19))

### Fixed
- Avert Parsing Latency By Rewriting Parser ([#22](https://github.com/FalkorDB/falkordb-rs/pull/22))

### Other
- *(deps)* bump regex from 1.10.4 to 1.10.5 ([#17](https://github.com/FalkorDB/falkordb-rs/pull/17))

## [0.1.1](https://github.com/FalkorDB/falkordb-rs/compare/v0.1.0...v0.1.1) - 2024-06-09

### Added
- LazyResultSet implementation, allowing one-by-one parsing ([#14](https://github.com/FalkorDB/falkordb-rs/pull/14))

### Fixed
- Update badges

## [0.1.0](https://github.com/FalkorDB/falkordb-rs/releases/tag/v0.1.0) - 2024-06-06

### Fixed
- Update readme file name before releasing, update job name ([#7](https://github.com/FalkorDB/falkordb-rs/pull/7))
- cleaned CI and fixed Code Coverage job, which was queued indefinitely ([#5](https://github.com/FalkorDB/falkordb-rs/pull/5))

### Other
- Fix doctests, some more code shrinking ([#9](https://github.com/FalkorDB/falkordb-rs/pull/9))
- Initial Development ([#1](https://github.com/FalkorDB/falkordb-rs/pull/1))
- gitignore
