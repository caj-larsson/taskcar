# Taskcar

Run tasks as scripts, distributed, only dependent on postgres.

## Setup

Run [migrate](https://github.com/golang-migrate/migrate) against your db. Don't
allow any applications access to the taskcar schema, it contains unecrypted
secrets.

## Write a task script
It must read a json object from STDIN when started. If you need some secrets,
insert them into the database. And configure them in the config.yaml.

The script must output only its result data, encoded as json, to STDOUT. All
logging must be done to STDERR.

The output data format is: 
```json
{
    "data": {...},
    "new_tasks": [
        {
            "queue": "$queueforthistask",
            "data": {...}
        }
    ]
}
```

