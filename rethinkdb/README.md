# Rethinkdb Integration

## Overview

Get metrics from rethinkdb service in real time to:

* Visualize and monitor rethinkdb stats
* Be notified about rethinkdb failures and events.
* Monitor rethinkdb tables and databases stats by filtering them by tags (rethinkdb_table, rethinkdb_db)

## Installation

Install the `dd-check-rethinkdb` package manually or with your favorite configuration manager

## Configuration

Edit the `rethinkdb.yaml` file to point to your server and port(add authentication details if required), set the masters to monitor

## Validation

When you run `datadog-agent info` you should see something like the following:

    Checks
    ======

        rethinkdb
        -----------
          - instance #0 [OK]
          - Collected 39 metrics, 0 events & 7 service checks

## Compatibility

The rethinkdb check is compatible with all major platforms
