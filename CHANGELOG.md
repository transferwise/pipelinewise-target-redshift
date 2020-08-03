1.6.0 (2020-08-03)
-------------------

- Add `aws_profile` option to use custom AWS profile

1.5.0 (2020-07-23)
-------------------

- Add `s3_acl` option to support ACL for S3 upload

1.4.1 (2020-06-17)
-------------------

- Switch jsonschema to use Draft7Validator

1.4.0 (2019-05-11)
-------------------

- Add `validate_records` option
- Add `skip_updates` option to skip updates in case of immutable records
- Add `temp_dir` optional parameter to config
- Fixed an issue when JSON values sometimes not sent correctly
- Support usage of reserved words as table and column names
- Use stream name as temp file suffix
- Log inserts, updates and csv size_bytes in a more consumable format
- Switch to `psychopg-binary` 2.8.5

1.3.0 (2019-02-18)
-------------------

- Support custom logging configuration by setting `LOGGING_CONF_FILE` env variable to the absolute path of a .conf file

1.2.1 (2019-12-30)
-------------------

- Fixed an issue when AWS credentials are not collected correctly from AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables

1.2.0 (2019-12-05)
-------------------

- Add `slices` option
- Add `compression` option
- Fixed an issue when ECS credentials not being correctly picked up by boto3 when no keys are provided

1.1.0 (2019-11-24)
-------------------

- Emit new state message as soon as data flushed to Redshift
- Add `flush_all_streams` option
- Add `max_parallelism` option
- Documentation fixes

1.0.8 (2019-10-04)
-------------------

- Allow STS token to be used
- Fix None being passed to iam_role in COPY command when role arn is None
- Allow S3 COPY role arn to be provided, remove required AWS credentials

1.0.7 (2019-10-01)
-------------------

- Add `parallelism` option
- Add `copy_options` option
- Fixed issue when permissions not granted properly

1.0.6 (2019-09-19)
-------------------

- Log SQLs only in debug mode

1.0.5 (2019-09-08)
-------------------

- Fixed loading float data types into double precision Redshift columns

1.0.4 (2019-09-08)
-------------------

- Set varchar column length dynamically

1.0.3 (2019-08-16)
-------------------

- Add license details

1.0.2 (2019-08-11)
-------------------

- Fixed loading numeric data types

1.0.1 (2019-08-11)
-------------------

- Fixed loading numeric data types

1.0.0 (2019-07-22)
-------------------

- Initial release
