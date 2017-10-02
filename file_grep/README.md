# File_grep Integration

## Overview

Get metrics from file_grep service in real time to:

* Visualize and monitor file_grep states
* Be notified about file_grep failovers and events.

## Installation

Install the `dd-check-file_grep` package manually or with your favorite configuration manager

## Configuration

Edit the `file_grep.yaml` file to point to your server and port, set the masters to monitor

## Validation

When you run `datadog-agent info` you should see something like the following:

    Checks
    ======

        file_grep
        -----------
          - instance #0 [OK]
          - Collected 39 metrics, 0 events & 7 service checks

## Compatibility

The file_grep check is compatible with all major platforms
