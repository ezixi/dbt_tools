from airflow.operators.bash_operator import BashOperator
from os.path import expanduser


class DbtCommon:
    def __init__(self, project, profile, data_mart, dag, branch="master"):
        self.project = project
        self.profile = profile
        self.data_mart = data_mart
        self.dag = dag
        self.branch = branch
        self.home = expanduser("~")

    def get_latest_dbt_models(self):
        """
        Pulls the latest code from the repo using the branch
        specified.
        """
        return BashOperator(
            task_id="get_latest_dbt_models",
            bash_command=f"cd ~/{self.project} && git fetch --all && git reset --hard origin/{self.branch}",
            dag=self.dag,
        )

    def refresh_dbt_deps(self):
        """
        dbt deps pulls the most recent version of the dependencies listed in
        your packages.yml from git.
        """
        return BashOperator(
            task_id="refresh_dbt_deps",
            bash_command=f"cd ~/{self.project} && dbt deps --profile={self.profile}",
            dag=self.dag,
        )

    def clean_dbt_docs(self):
        """
        dbt clean is a utility function that deletes all folders specified in
        the clean-targets list specified in dbt_project.yml. This is useful
        for deleting the dbt_modules and target directories.
        """
        return BashOperator(
            task_id="clean_dbt_docs",
            bash_command=f"cd ~/{self.project} && dbt clean --profile={self.profile}",
            dag=self.dag,
        )

    def create_dbt_snapshot(self, snapshot):
        """
        The dbt snapshot command executes the Snapshots defined in your project.
        dbt will looks for Snapshots in the snapshot-paths paths defined in your
        dbt_project.yml file. By default, the snapshot-paths path is snapshots/.
        """
        return BashOperator(
            task_id="create_dbt_snapshot",
            bash_command=f"cd ~/{self.project} && dbt snapshot --profile={self.profile} -s {snapshot}",
            dag=self.dag,
        )

    def load_static_csvs(self):
        """
        dbt seed loads data from csv files into your data warehouse. Because these
        csv files are located in your dbt repository, they are version controlled
        and code reviewable. Thus, dbt seed is appropriate for loading static data
        which changes infrequently.

        The dbt seed command will load csv files located in the data-paths directory
        of your dbt project into your data warehouse.
        """
        return BashOperator(
            task_id="load_static_csvs",
            bash_command=f"cd ~/{self.project} && dbt seed --profile={self.profile}",
            dag=self.dag,
        )

    def compile_dbt_sql(self):
        """
        dbt compile generates executable SQL from source model, test, and analysis
        files. You can find these compiled SQL files in the target/ directory of
        your dbt project. The compile command is useful for:

        * Visually inspecting the compiled output of model files. This is useful for
        validating complex jinja logic or macro usage.
        * Manually running compiled SQL. While debugging a model or schema test, it's
        often useful to execute the underlying select statement to find the source of
        the bug.
        * Compiling analysis files.
        """
        return BashOperator(
            task_id="compile_dbt_sql",
            bash_command=f"cd ~/{self.project} && dbt compile --profile={self.profile}",
            dag=self.dag,
        )

    def generate_dbt_docs(self):
        """
        The command is responsible for generating your project's documentation website by
        * copying the website index.html file into the target/ directory
        * compiling the project to target/manifest.json
        * producing the target/catalog.json file, which contains metadata about the tables
        and views produced by the models in your project.
        """
        return BashOperator(
            task_id="generate_dbt_docs",
            bash_command=f"cd ~/{self.project} && dbt docs generate",
            dag=self.dag,
        )

    def refresh_project(self):
        """
        dbt run executes compiled sql model files against the current target database.
        dbt connects to the target database and runs the relevant SQL required to materialize
        all data models using the specified materialization strategies. Models are run in the
        order defined by the dependency graph generated during compilation. Intelligent
        multi-threading is used to minimize execution time without violating dependencies.
        """
        return BashOperator(
            task_id="refresh_project",
            bash_command=f"cd ~/{self.project} && dbt run --profile={self.profile}",
            dag=self.dag,
        )

    def refresh_data_mart(self, is_incremental=True):
        """
        dbt run executes compiled sql model files against the current target database.
        dbt connects to the target database and runs the relevant SQL required to materialize
        all data models _within the specified data_mart_ using the specified materialization
        strategies. Models are run in the order defined by the dependency graph generated
        during compilation. Intelligent multi-threading is used to minimize execution time
        without violating dependencies.
        """
        if is_incremental:
            command = f"cd ~/{self.project} && dbt run --models marts.{self.data_mart} --profile={self.profile}"
        else:
            command = f"cd ~/{self.project} && dbt run --models marts.{self.data_mart} --full_refresh --profile={self.profile}"
        return BashOperator(
            task_id=f"refresh_data_mart_{self.data_mart}",
            bash_command=command,
            trigger_rule="all_done",
            dag=self.dag,
        )

    def refresh_model(self, model, is_incremental=True):
        """
        dbt run executes compiled sql model files against the current target database.
        dbt connects to the target database and runs the relevant SQL required to materialize
        _the specified_ data model including its parents. Models are run in the
        order defined by the dependency graph generated during compilation. Intelligent
        multi-threading is used to minimize execution time without violating dependencies.
        """
        if is_incremental:
            command = f"cd ~/{self.project} && dbt run --models {model} --profile={self.profile}"
        else:
            command = f"cd ~/{self.project} && dbt run --models {model} --full-refresh --profile={self.profile}"
        return BashOperator(
            task_id=f"refresh_model_{model}", bash_command=command, dag=self.dag
        )
