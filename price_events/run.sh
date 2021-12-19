#!/bin/bash

# Run Service
nameko run --config config.yml price_events --backdoor 3000
