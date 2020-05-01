#!/bin/bash

#use with pre_commit (https://pre-commit.com/) to run tests on any models that you commit changes to
fbname=$(basename "$1" .sql)

dbt test -m "$fbname"
