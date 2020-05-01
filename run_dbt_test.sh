#!/bin/bash

fbname=$(basename "$1" .sql)

dbt test -m "$fbname"
