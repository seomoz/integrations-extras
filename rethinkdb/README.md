# Rethinkdb Integration

## Overview

Get metrics from rethinkdb service in real time to:

* Visualize and monitor rethinkdb states
* Be notified about rethinkdb failovers and events.

## Installation

Install the `dd-check-rethinkdb` package manually or with your favorite configuration manager

## Configuration

Edit the `rethinkdb.yaml` file to point to your server and port, set the masters to monitor

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
