# Status_process Integration

## Overview

Get metrics from status_process service in real time to:

* Visualize and monitor status_process states
* Be notified about status_process failovers and events.

## Installation

Install the `dd-check-status_process` package manually or with your favorite configuration manager

## Configuration

Edit the `status_process.yaml` file to point to your server and port, set the masters to monitor

## Validation

When you run `datadog-agent info` you should see something like the following:

    Checks
    ======

        status_process
        -----------
          - instance #0 [OK]
          - Collected 39 metrics, 0 events & 7 service checks

## Compatibility

The status_process check is compatible with all major platforms
