# dbt_tools
Helpers to get the best out of dbt.

1. Clone the repo
2. Create a virtual env, as it always helps.
3. Install the requirements
```pip install -r requirements.txt```

## Using pre-commit to run tests on changed dbt models

4. Install the git hook scripts
```pre-commit install```

Each time you commit a model, the pre-commit hook will run `dbt test` against it. Of course, any model will pass if you havent written any tests.
