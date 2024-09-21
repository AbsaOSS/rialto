# Change Log
All notable changes to this project will be documented in this file.

## 2.0.0 - 2024-mm-dd
   #### Runner
   - runner config now accepts environment variables
   - restructured runner config
     - added metadata and feature loader sections
     - target moved to pipeline
     - dependency date_col is now mandatory
     - custom extras config is available in each pipeline and will be passed as dictionary available under pipeline_config.extras
     - general section is renamed to runner
     - info_date_shift is always a list
   - transformation header changed
   - added argument to skip dependency checking
   - added overrides parameter to allow for dynamic overriding of config values
   - removed date_from and date_to from arguments, use overrides instead
   #### Jobs
   - jobs are now the main way to create all pipelines
   - config holder removed from jobs
   - metadata_manager and feature_loader are now available arguments, depending on configuration
   - added @config decorator, similar use case to @datasource, for parsing configuration
   - reworked Resolver + Added ModuleRegister
     - datasources no longer just by importing, thus are no longer available for all jobs
     - register_dependency_callable and register_dependency_module added to register datasources
     - together, it's now possilbe to have 2 datasources with the same name, but different implementations for 2 jobs.
   #### TableReader
   - function signatures changed
     - until -> date_until
     - info_date_from -> date_from, info_date_to -> date_to
     - date_column is now mandatory
   - removed TableReaders ability to infer schema from partitions or properties
   #### Loader
   - removed DataLoader class, now only PysparkFeatureLoader is needed with additional parameters

## 1.3.0 - 2024-06-07


### Added
 - passing dependencies from runner to a Transformation
   - optional dependency names in the config that could be recalled via dictionary to access paths and date columns
 - Rialto now adds rialto_date_column property to written tables

### Changed
- signature of Transformation
- Allowed future dependencies

[//]: # (### Fixed)
