#!/bin/bash

#use with pre_commit (https://pre-commit.com/) to lint any models that you commit changes to
sqlfluff fix "$1" -f --rules L010,L016,L030 && sqlfluff lint "$1"