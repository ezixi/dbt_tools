# dbt_tools
Helpers to get the best out of dbt.

1. Clone the repo
2. Create a virtual env, as it always helps.
3. Install the requirements
```pip install -r requirements.txt```

## Using pre-commit to run tests on changed dbt models

4. Install the git hook scripts
```pre-commit install```

Each time you commit a model, the pre-commit hook will run `dbt test` against it. Of course, any model will pass if you haven't written any tests. It also runs `sqlfluff` that will lint your models and fix _some_ common formatting issues.

## Airflow helpers
Although dbt does have a Python API, it is [not currently documented](https://docs.getdbt.com/docs/running-a-dbt-project/dbt-api).
A workaround is to create [a helper class that returns the dbt commands as Bash Operators](https://github.com/ezixi/dbt_tools/blob/master/airflow/dags/dbt_common.py) and instantiating a new instance of the dbt object in your dag.

### Usage
```python
from dbt_common import DbtCommon

dag = DAG(
    dag_id="ETL_load_test_data_mart",
    default_args=args,
    start_date=datetime(2019, 10, 17),
    schedule_interval="*/30 * * * *",
    description="Call dbt to populate the tables in the test data mart.",
)

test_data_mart = DbtCommon(
    project="dbt_project_name",
    profile="prod",
    data_mart="test_data_mart",
    dag=dag
)


test_data_mart.refresh_data_mart()
```
