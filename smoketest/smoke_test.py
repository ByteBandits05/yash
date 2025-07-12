# ==============================================================================
# Databricks Smoke Test Script
# ==============================================================================
# Purpose: Validates Databricks table existence and data availability using OIDC
# Author: DevOps Engineering Team
# Version: 1.0
# Last Updated: 2024
# Dependencies: databricks-sdk, azure-identity
# ==============================================================================

import os
import sys
import logging
from typing import Optional

try:
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.core import Config
    from azure.identity import ClientSecretCredential
except ImportError as e:
    print(f"ERROR: Required package not installed: {e}")
    print("Please install: pip install databricks-sdk azure-identity")
    sys.exit(1)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DatabricksSmokeTester:
    """
    Databricks smoke test class for validating table existence and data availability.
    """
    
    def __init__(self):
        """Initialize the smoke tester with environment-based configuration."""
        self.workspace_client: Optional[WorkspaceClient] = None
        self.table_name: str = ""
        self.config = self._load_configuration()
    
    def _load_configuration(self) -> dict:
        """
        Load configuration from environment variables.
        
        Returns:
            dict: Configuration dictionary with all required parameters
            
        Raises:
            SystemExit: If required environment variables are missing
        """
        required_env_vars = [
            'DATABRICKS_HOST',
            'AZURE_CLIENT_ID',
            'AZURE_CLIENT_SECRET',
            'AZURE_TENANT_ID',
            'SMOKE_TEST_TABLE_NAME'
        ]
        
        config = {}
        missing_vars = []
        
        for var in required_env_vars:
            value = os.getenv(var)
            if not value:
                missing_vars.append(var)
            else:
                config[var.lower()] = value
        
        if missing_vars:
            logger.error(f"Missing required environment variables: {', '.join(missing_vars)}")
            print("ERROR: Missing required environment variables:")
            for var in missing_vars:
                print(f"  - {var}")
            sys.exit(1)
        
        # Optional environment variables with defaults
        config['max_retries'] = int(os.getenv('SMOKE_TEST_MAX_RETRIES', '3'))
        config['timeout_seconds'] = int(os.getenv('SMOKE_TEST_TIMEOUT_SECONDS', '300'))
        config['min_row_count'] = int(os.getenv('SMOKE_TEST_MIN_ROW_COUNT', '1'))
        
        self.table_name = config['smoke_test_table_name']
        
        return config
    
    def _authenticate_with_oidc(self) -> WorkspaceClient:
        """
        Authenticate to Databricks using Azure AD OIDC.
        
        Returns:
            WorkspaceClient: Authenticated Databricks workspace client
            
        Raises:
            Exception: If authentication fails
        """
        try:
            logger.info("Authenticating to Databricks using Azure AD OIDC...")
            
            # Create Azure credential
            credential = ClientSecretCredential(
                tenant_id=self.config['azure_tenant_id'],
                client_id=self.config['azure_client_id'],
                client_secret=self.config['azure_client_secret']
            )
            
            # Configure Databricks client
            databricks_config = Config(
                host=self.config['databricks_host'],
                azure_client_id=self.config['azure_client_id'],
                azure_client_secret=self.config['azure_client_secret'],
                azure_tenant_id=self.config['azure_tenant_id']
            )
            
            # Create workspace client
            workspace_client = WorkspaceClient(config=databricks_config)
            
            logger.info("Successfully authenticated to Databricks")
            return workspace_client
            
        except Exception as e:
            logger.error(f"Failed to authenticate to Databricks: {str(e)}")
            raise
    
    def _validate_table_exists(self) -> bool:
        """
        Validate that the specified table exists in Databricks.
        
        Returns:
            bool: True if table exists, False otherwise
        """
        try:
            logger.info(f"Checking if table '{self.table_name}' exists...")
            
            # Parse table name (catalog.schema.table or schema.table)
            table_parts = self.table_name.split('.')
            if len(table_parts) == 3:
                catalog, schema, table = table_parts
            elif len(table_parts) == 2:
                catalog = "hive_metastore"  # Default catalog
                schema, table = table_parts
            else:
                raise ValueError(f"Invalid table name format: {self.table_name}")
            
            # Check if table exists
            tables = self.workspace_client.tables.list(
                catalog_name=catalog,
                schema_name=schema
            )
            
            table_exists = any(t.name == table for t in tables)
            
            if table_exists:
                logger.info(f"Table '{self.table_name}' exists")
                return True
            else:
                logger.error(f"Table '{self.table_name}' does not exist")
                return False
                
        except Exception as e:
            logger.error(f"Error checking table existence: {str(e)}")
            return False
    
    def _validate_table_has_data(self) -> bool:
        """
        Validate that the specified table contains data.
        
        Returns:
            bool: True if table has data, False otherwise
        """
        try:
            logger.info(f"Checking if table '{self.table_name}' contains data...")
            
            # Execute count query
            sql_query = f"SELECT COUNT(*) as row_count FROM {self.table_name}"
            
            # Execute SQL query using SQL execution API
            response = self.workspace_client.statement_execution.execute_statement(
                warehouse_id=os.getenv('DATABRICKS_WAREHOUSE_ID'),
                statement=sql_query,
                wait_timeout=f"{self.config['timeout_seconds']}s"
            )
            
            # Extract row count from response
            if response.result and response.result.data_array:
                row_count = int(response.result.data_array[0][0])
                logger.info(f"Table '{self.table_name}' contains {row_count} rows")
                
                if row_count >= self.config['min_row_count']:
                    logger.info(f"Table has sufficient data (>= {self.config['min_row_count']} rows)")
                    return True
                else:
                    logger.error(f"Table has insufficient data ({row_count} < {self.config['min_row_count']} rows)")
                    return False
            else:
                logger.error("Failed to retrieve row count from query response")
                return False
                
        except Exception as e:
            logger.error(f"Error checking table data: {str(e)}")
            return False
    
    def run_smoke_test(self) -> bool:
        """
        Execute the complete smoke test suite.
        
        Returns:
            bool: True if all tests pass, False otherwise
        """
        try:
            logger.info("Starting Databricks smoke test...")
            
            # Step 1: Authenticate to Databricks
            self.workspace_client = self._authenticate_with_oidc()
            
            # Step 2: Validate table exists
            if not self._validate_table_exists():
                logger.error("Smoke test FAILED: Table does not exist")
                return False
            
            # Step 3: Validate table has data
            if not self._validate_table_has_data():
                logger.error("Smoke test FAILED: Table is empty or has insufficient data")
                return False
            
            logger.info("Smoke test PASSED: All validations successful")
            return True
            
        except Exception as e:
            logger.error(f"Smoke test FAILED with exception: {str(e)}")
            return False


def main():
    """Main function to execute the smoke test."""
    print("=" * 80)
    print("Databricks Smoke Test Execution")
    print("=" * 80)
    
    # Create and run smoke test
    smoke_tester = DatabricksSmokeTester()
    
    try:
        success = smoke_tester.run_smoke_test()
        
        if success:
            print("\n✅ SMOKE TEST PASSED")
            print("All validations completed successfully")
            sys.exit(0)
        else:
            print("\n❌ SMOKE TEST FAILED")
            print("One or more validations failed")
            sys.exit(1)
            
    except KeyboardInterrupt:
        print("\n⚠️  SMOKE TEST INTERRUPTED")
        print("Test execution was interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n💥 SMOKE TEST ERROR: {str(e)}")
        print("Test execution failed due to unexpected error")
        sys.exit(1)


if __name__ == "__main__":
    main()