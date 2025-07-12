# ==============================================================================
# Databricks Smoke Test Script
# ==============================================================================
# Purpose: Validates Databricks deployment by checking table existence and data
# Authentication: Uses OIDC credentials from environment variables  
# Validation: Confirms specified table exists and contains at least one row
# Exit Codes: 0 for success, 1 for validation failure, 2 for connection error
# ==============================================================================

import os
import sys
import logging
from typing import Optional

# Configure logging for test execution tracking
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_environment_variable(var_name: str, required: bool = True) -> Optional[str]:
    """
    Retrieve environment variable with optional requirement validation.
    
    Args:
        var_name (str): Name of the environment variable
        required (bool): Whether the variable is required (default: True)
        
    Returns:
        Optional[str]: Environment variable value or None if not required and missing
        
    Raises:
        SystemExit: If required variable is missing
    """
    value = os.environ.get(var_name)
    
    if required and not value:
        logger.error(f"Required environment variable '{var_name}' is not set")
        sys.exit(2)
        
    return value


def validate_databricks_connection() -> bool:
    """
    Validate Databricks connection using OIDC authentication.
    
    Returns:
        bool: True if connection successful, False otherwise
    """
    try:
        # Import Databricks SDK components
        from databricks.sdk import WorkspaceClient
        from databricks.sdk.core import Config
        
        # Get OIDC authentication parameters from environment
        azure_client_id = get_environment_variable('AZURE_CLIENT_ID')
        azure_tenant_id = get_environment_variable('AZURE_TENANT_ID') 
        azure_client_secret = get_environment_variable('AZURE_CLIENT_SECRET')
        databricks_host = get_environment_variable('DATABRICKS_HOST')
        
        logger.info("Configuring Databricks connection with OIDC authentication")
        
        # Configure Databricks client with OIDC
        config = Config(
            host=databricks_host,
            azure_client_id=azure_client_id,
            azure_tenant_id=azure_tenant_id,
            azure_client_secret=azure_client_secret
        )
        
        # Initialize workspace client
        workspace_client = WorkspaceClient(config=config)
        
        # Test connection by listing workspace info
        current_user = workspace_client.current_user.me()
        logger.info(f"Successfully connected to Databricks as user: {current_user.user_name}")
        
        return True
        
    except ImportError as e:
        logger.error(f"Databricks SDK not available: {e}")
        return False
    except Exception as e:
        logger.error(f"Failed to connect to Databricks: {e}")
        return False


def validate_table_exists_and_has_data() -> bool:
    """
    Validate that the specified table exists and contains at least one row.
    
    Returns:
        bool: True if table exists and has data, False otherwise
    """
    try:
        # Import required SQL execution components
        from databricks.sdk import WorkspaceClient
        from databricks.sdk.core import Config
        from databricks.sdk.service.sql import StatementState
        
        # Get connection and table configuration from environment
        azure_client_id = get_environment_variable('AZURE_CLIENT_ID')
        azure_tenant_id = get_environment_variable('AZURE_TENANT_ID')
        azure_client_secret = get_environment_variable('AZURE_CLIENT_SECRET') 
        databricks_host = get_environment_variable('DATABRICKS_HOST')
        table_name = get_environment_variable('SMOKE_TEST_TABLE_NAME')
        warehouse_id = get_environment_variable('DATABRICKS_WAREHOUSE_ID')
        
        logger.info(f"Validating table: {table_name}")
        
        # Configure Databricks client
        config = Config(
            host=databricks_host,
            azure_client_id=azure_client_id,
            azure_tenant_id=azure_tenant_id,
            azure_client_secret=azure_client_secret
        )
        
        workspace_client = WorkspaceClient(config=config)
        
        # Execute SQL query to check table existence and row count
        sql_query = f"SELECT COUNT(*) as row_count FROM {table_name} LIMIT 1"
        
        logger.info(f"Executing SQL query: {sql_query}")
        
        # Submit SQL statement execution
        statement_execution = workspace_client.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            statement=sql_query,
            wait_timeout="30s"
        )
        
        # Check execution status
        if statement_execution.status.state == StatementState.SUCCEEDED:
            # Extract row count from results
            if statement_execution.result and statement_execution.result.data_array:
                row_count = int(statement_execution.result.data_array[0][0])
                logger.info(f"Table '{table_name}' contains {row_count} rows")
                
                if row_count > 0:
                    logger.info("✓ Table validation successful - table exists and contains data")
                    return True
                else:
                    logger.error("✗ Table validation failed - table exists but is empty")
                    return False
            else:
                logger.error("✗ Table validation failed - no data returned from query")
                return False
        else:
            logger.error(f"✗ SQL execution failed with state: {statement_execution.status.state}")
            if statement_execution.status.error:
                logger.error(f"Error details: {statement_execution.status.error.message}")
            return False
            
    except Exception as e:
        logger.error(f"✗ Table validation failed with exception: {e}")
        return False


def main():
    """
    Main smoke test execution function.
    
    Performs connection validation and table data validation.
    Exits with appropriate code based on test results.
    """
    logger.info("Starting Databricks smoke test validation")
    
    # Step 1: Validate Databricks connection
    logger.info("Step 1: Validating Databricks connection")
    if not validate_databricks_connection():
        logger.error("Smoke test failed - unable to connect to Databricks")
        sys.exit(2)
    
    # Step 2: Validate table existence and data
    logger.info("Step 2: Validating table existence and data")
    if not validate_table_exists_and_has_data():
        logger.error("Smoke test failed - table validation unsuccessful")
        sys.exit(1)
    
    # Success - all validations passed
    logger.info("🎉 Smoke test completed successfully - all validations passed")
    print("SUCCESS: Databricks deployment validation completed successfully")
    sys.exit(0)


if __name__ == "__main__":
    main()