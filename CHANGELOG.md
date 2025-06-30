# Change Log
All notable changes to this project will be documented in this file.

## 2.0.9 - 2025-06-10
  ### Loader
  - PysparkFeatureLoader.feature_schema now also accepts a list of schemas
    - the features can now be split into multiple schemas (names of groups must remain unique across all schemas)
  - loader now also accepts storage table names interchangeably with feature group names
    - assumes that feature group names contain uppercase lettering

## 2.0.8 - 2025-05-xx
  ### Runner
  - expanded config overrides logging
  - added config validation
  - added owner attribute to GroupMetadata class

## 2.0.7 - 2025-04-15
  ### Runner
  - email reporting is now optional
  - refactored BookKeeper methods
    - bookkeeping table is no longer being overwritten, new records are appended instead

## 2.0.6 - 2025-02-26
  ### Job Metadata
  - @jobs can now request job_metadata
  - job_metadata contains
    - name of the job (e.g. model predict)
    - information about the job package: distribution_name and version (e.g. test-model, v 0.2.1)

## 2.0.3 - 2025-01-27
  ### Runner
  - added run time timestamp to record

## 2.0.2 - 2025-01-24
  ### Runner
  - new bookkeeping functionality
    - save reporting information to dataframe
    - added bookkeeping config option to runner config

## 2.0.1 - 2024-10-02
  ### bugfixes
   - compound keys now work properly in sequential features
   - module register now resets on reload while testing


## 2.0.0 - 2024-09-21
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
