# datagovuk-loader

A small package of utilities written in [Go](https://golang.org) for fetching, parsing and storing [Data.gov.uk datasets](https://data.gov.uk/data/search) into a [Posgresql database](https://www.postgresql.org/).

## Prerequisite

Make sure you have a working Go environment and a database to store the information. 

The tools make the required database tables if they don't exist.

## Running

```
./datagovuk-loader [DataLoader]
```

### Environment variables

| Variable | Purpose |
| -------- | ------- |
| *DB_HOST* | Database host, default: localhost |
| *DB_USER* | Database user, default: current user |
| *DB_PASSWORD* | Database user's password, optional |
| *DB_NAME* | Database name, default: datagovuk |

### Data Loaders

| Identifier | Source | Database Tables |
| ---------- | ------ | --------------- |
| postcode   | [OpenDataCommunities.org](http://opendatacommunities.org/data/postcodes) | post_code_areas, post_code_districts, post_code_sectors, post_code_units |

### License

Copyright 2016 Seyed Razavi

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.

You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
