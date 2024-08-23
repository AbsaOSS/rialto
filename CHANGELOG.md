# Change Log
All notable changes to this project will be documented in this file.

## 2.0.0 - 2024-mm-dd
   #### Runner
   - runner config now accepts environment variables
   - restructured runner config
     - added metadata and feature loader sections
     - target moved to pipeline
     - dependency date_col is mandatory
     - custom extras config is available in each pipeline and will be passed as dictionary
     - general section is renamed to runner
   - transformation header changed
   - added argument to skip dependency checking
   #### Jobs
   - config holder removed from jobs
   - metadata_manager and feature_loader are now available arguments, depending on configuration
   #### TableReader
   - function signatures changed
     - until -> date_until
     - info_date_from -> date_from, info_date_to -> date_to
     - date_column is now mandatory
   - removed TableReaders ability to infer schema from partitions or properties


## 1.3.0 - 2024-06-07


### Added
 - passing dependencies from runner to a Transformation
   - optional dependency names in the config that could be recalled via dictionary to access paths and date columns
 - Rialto now adds rialto_date_column property to written tables

### Changed
- signature of Transformation
- Allowed future dependencies

[//]: # (### Fixed)
