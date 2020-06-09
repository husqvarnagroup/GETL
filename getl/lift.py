"""Entry point for using the getl."""
from pyspark.sql import SparkSession

from getl.lift_definition import resolve_lift_definition
from getl.manager import Manager


def lift(spark: SparkSession, lift_def: str, parameters: dict) -> dict:
    """Lift function for doing ETL jobs."""
    lift_def = resolve_lift_definition(lift_def, parameters)

    # Give lift definition to manager
    manager = Manager(spark)

    # Apply a file registry if it exists in the lift definition
    if "FileRegistry" in lift_def:
        manager.init_file_registry(lift_def["FileRegistry"])

    return manager.execute_lift_job(lift_def["LiftJob"])
