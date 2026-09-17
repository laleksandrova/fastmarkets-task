from pathlib import Path

from dagster_dbt import DbtCliResource, DbtProject

# Points at the existing dbt project (built earlier, unchanged) rather than
# duplicating any dbt logic here - Dagster just orchestrates it.
DBT_PROJECT_DIR = Path(__file__).parent.parent / "dbt"

dbt_project = DbtProject(project_dir=DBT_PROJECT_DIR)

# Regenerates the dbt manifest automatically when running `dagster dev`,
# so Dagster's asset graph always reflects the current state of the dbt
# project without a manual `dbt parse` step.
dbt_project.prepare_if_dev()

# No explicit profiles_dir needed: dagster-dbt looks for profiles.yml
# inside the project folder by default (dbt/profiles.yml), which is why
# a copy lives there alongside the one at ~/.dbt/profiles.yml used by
# the plain dbt CLI - see dbt/profiles.yml's header comment for why
# that's safe to have in two places.
dbt_resource = DbtCliResource(project_dir=dbt_project)
