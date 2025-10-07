from great_expectations import DataContext
from great_expectations.core.batch import RuntimeBatchRequest

def validate_data(path_to_file: str, ge_context_path: str):
    """
    Validate a CSV file using Great Expectations checkpoint.
    
    Args:
        path_to_file: path ของไฟล์ CSV ที่จะ validate
        ge_context_path: path ของ GE DataContext
    """
    context = DataContext(ge_context_path)
    
    batch_request = RuntimeBatchRequest(
        datasource_name="covid_datasource",
        data_connector_name="default_runtime_data_connector_name",
        data_asset_name="covid_19_clean",
        runtime_parameters={"path": path_to_file},
        batch_identifiers={"default_identifier_name": "covid_batch"},
    )
    
    result = context.run_checkpoint(
        checkpoint_name="covid_checkpoint",
        batch_request=batch_request
    )
    
    if not result["success"]:
        raise ValueError(f"Data validation failed for {path_to_file}")
    
    print(f"Validation passed for {path_to_file}")
    return result
