# Bind Integration

## Overview

Get metrics from bind service in real time to:

* Visualize and monitor bind states
* Be notified about bind failovers and events.

## Installation

Install the `dd-check-bind` package manually or with your favorite configuration manager

## Configuration

Edit the `bind.yaml` file to point to your server and port, set the masters to monitor

## Validation

When you run `datadog-agent info` you should see something like the following:

    Checks
    ======

        bind
        -----------
          - instance #0 [OK]
          - Collected 39 metrics, 0 events & 7 service checks

## Compatibility

The bind check is compatible with all major platforms
