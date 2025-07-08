# vivarium/tests/deploy/test_orchestrator.py
import unittest
from unittest.mock import MagicMock, patch, call
import sys
import os

# Adjust sys.path to import modules from the vivarium project
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Class being tested
from deploy.src.deploy_orchestrator import DeployOrchestrator

# Import dependencies that will be mocked
# Note: When patching, you usually patch where the object is *looked up*, not where it's defined.
# So, if DeployOrchestrator imports LogHelper, you patch 'deploy.src.orchestrator.LogHelper'.

class TestDeployOrchestrator(unittest.TestCase):
    """
    Unit tests for the DeployOrchestrator class.

    These tests mock external dependencies to isolate the orchestrator's logic.
    """

    # Using @patch.multiple for multiple mocks at once, and setUp for common setup
    @patch('deploy.src.deploy_orchestrator.LogHelper')
    @patch('deploy.src.deploy_orchestrator.FileConfig')
    @patch('deploy.src.deploy_orchestrator.DatabaseConfig')
    @patch('deploy.src.deploy_orchestrator.SupabaseConfig')
    # Patch the concrete strategy classes where Orchestrator imports/uses them
    @patch('deploy.src.deploy_orchestrator.PostgresSetup')
    @patch('deploy.src.deploy_orchestrator.SupabaseSetup')
    
    # Patch PathUtils if the orchestrator directly uses it (it's used by setup strategies, not orchestrator directly)
    # @patch('deploy.src.orchestrator.PathUtils')
    def setUp(self, MockSupabaseSetup, MockPostgresSetup, MockSupabaseConfig, MockDatabaseConfig, MockFileConfig, MockLogHelper):
        """
        Set up mocks for each test. This method is run before every test method.
        """
        # Store mock objects on the instance for easy access in test methods
        self.mock_log_helper = MockLogHelper
        self.mock_logger = MockLogHelper.get_logger.return_value # get_logger returns a logger instance
        self.mock_file_config = MockFileConfig.return_value
        self.mock_db_config = MockDatabaseConfig.return_value
        self.mock_supabase_config = MockSupabaseConfig.return_value
        self.mock_postgres_setup = MockPostgresSetup
        self.mock_supabase_setup = MockSupabaseSetup

        # Set common return values for mocks
        # Mock FileConfig attributes if the orchestrator uses them during init or setup
        self.mock_file_config.supabase_schema = 'supabase_schema.sql'
        self.mock_file_config.schema_file = 'postgres_schema.sql'

        # Set up default successful responses for mocked strategies
        # Mock the instance returned by the strategy constructor
        self.mock_db_ops = MagicMock() # Mock the DatabaseOperations instance that strategies return
        self.mock_postgres_setup.return_value.execute_full_setup.return_value = (True, self.mock_db_ops)
        self.mock_supabase_setup.return_value.execute_full_setup.return_value = (True, self.mock_db_ops)

        # Clear logs before each test to prevent interference
        self.mock_logger.reset_mock()

    # --- Test Cases for __init__ method ---

    def test_init_postgres_local(self):
        """Test initialization for PostgreSQL local connection."""
        orchestrator = DeployOrchestrator(database_type='postgres', connection_type='local')
        self.assertIsInstance(orchestrator.db_setup_strategy, MagicMock) # It's a mock instance
        self.mock_postgres_setup.assert_called_once_with(db_config=self.mock_db_config)
        self.mock_supabase_setup.assert_not_called()
        self.assertEqual(orchestrator.db_type, 'postgres')
        self.assertEqual(orchestrator.connection_type, 'local')
        self.mock_logger.info.assert_any_call(
            "DeployOrchestrator: Initializing for database type 'postgres' with connection type 'local'."
        )

    def test_init_supabase_remote(self):
        """Test initialization for Supabase remote connection."""
        orchestrator = DeployOrchestrator(database_type='supabase', connection_type='remote')
        self.assertIsInstance(orchestrator.db_setup_strategy, MagicMock)
        self.mock_supabase_setup.assert_called_once_with(supabase_config=self.mock_supabase_config)
        self.mock_postgres_setup.assert_not_called()
        self.assertEqual(orchestrator.db_type, 'supabase')
        self.assertEqual(orchestrator.connection_type, 'remote')
        self.mock_logger.info.assert_any_call(
            "DeployOrchestrator: Initializing for database type 'supabase' with connection type 'remote'."
        )

    def test_init_supabase_local_warns(self):
        """Test initialization for Supabase local connection warns but treats as remote."""
        orchestrator = DeployOrchestrator(database_type='supabase', connection_type='local')
        self.mock_supabase_setup.assert_called_once_with(supabase_config=self.mock_supabase_config)
        self.mock_logger.warning.assert_called_once_with(
            "Supabase is a cloud service. 'local' connection type specified, "
            "but will be treated as 'remote' by the Supabase setup strategy."
        )
        self.assertEqual(orchestrator.db_type, 'supabase')
        self.assertEqual(orchestrator.connection_type, 'local')

    def test_init_unsupported_db_type_raises_error(self):
        """Test initialization with an unsupported database type raises ValueError."""
        with self.assertRaisesRegex(ValueError, "Unsupported database type: 'unsupported'."):
            DeployOrchestrator(database_type='unsupported')
        self.mock_postgres_setup.assert_not_called()
        self.mock_supabase_setup.assert_not_called()
        self.mock_logger.info.assert_any_call(
            "DeployOrchestrator: Initializing for database type 'unsupported' with connection type 'local'."
        ) # Initial log still happens before the error is raised

    def test_init_skip_setup(self):
        """Test initialization when setup is skipped (internal 'none' type)."""
        orchestrator = DeployOrchestrator(database_type='none', connection_type='none')
        self.assertIsNone(orchestrator.db_setup_strategy)
        self.mock_postgres_setup.assert_not_called()
        self.mock_supabase_setup.assert_not_called()
        self.mock_logger.info.assert_any_call(
            "DeployOrchestrator initialized without a specific database setup strategy (setup skipped)."
        )

    # --- Test Cases for run_setup method ---

    def test_run_setup_success(self):
        """Test successful execution of database setup."""
        orchestrator = DeployOrchestrator(database_type='postgres')
        
        # Call the method under test
        success, db_ops = orchestrator.run_setup(sql_script_name='test_schema.sql')

        self.assertTrue(success)
        self.assertEqual(db_ops, self.mock_db_ops) # Should return the mocked db_ops instance
        
        # Verify that the underlying strategy's execute_full_setup was called
        orchestrator.db_setup_strategy.execute_full_setup.assert_called_once_with(
            sql_script_name='test_schema.sql'
        )
        self.mock_logger.info.assert_any_call("Database setup process completed successfully.")

    def test_run_setup_failure(self):
        """Test failed execution of database setup."""
        orchestrator = DeployOrchestrator(database_type='postgres')
        # Configure the mock strategy to return failure
        orchestrator.db_setup_strategy.execute_full_setup.return_value = (False, None)

        success, db_ops = orchestrator.run_setup(sql_script_name='test_schema.sql')

        self.assertFalse(success)
        self.assertIsNone(db_ops)
        orchestrator.db_setup_strategy.execute_full_setup.assert_called_once()
        self.mock_logger.error.assert_any_call("Database setup process finished with errors. Data loading will be skipped.")

    def test_run_setup_no_strategy(self):
        """Test run_setup when no strategy is defined (e.g., skip flag)."""
        orchestrator = DeployOrchestrator(database_type='none', connection_type='none') # Initialize with 'none' to skip strategy setup

        success, db_ops = orchestrator.run_setup(sql_script_name='ignored.sql')

        self.assertFalse(success)
        self.assertIsNone(db_ops)
        self.mock_logger.warning.assert_called_once_with(
            "No setup strategy defined for 'none'. Skipping automated setup."
        )

    def test_run_setup_exception(self):
        """Test that run_setup handles unexpected exceptions from strategy."""
        orchestrator = DeployOrchestrator(database_type='postgres')
        # Make the mocked strategy raise an exception
        orchestrator.db_setup_strategy.execute_full_setup.side_effect = Exception("Test error")

        success, db_ops = orchestrator.run_setup(sql_script_name='test.sql')

        self.assertFalse(success)
        self.assertIsNone(db_ops)
        self.mock_logger.error.assert_called_once() # Check that an error was logged
        self.assertIn("An unexpected error occurred during database setup delegation:", self.mock_logger.error.call_args[0][0])


    # --- Test Cases for run_data_loading method ---

    @patch('deploy.src.orchestrator.DataLoaderStrategy') # Patch the abstract base class where it's used
    def test_run_data_loading_success(self, MockDataLoaderStrategy):
        """Test successful execution of data loading."""
        # Setup an orchestrator instance (type doesn't strictly matter for data loading phase)
        orchestrator = DeployOrchestrator(database_type='postgres')
        
        # Create a mock data loader instance
        mock_data_loader = MockDataLoaderStrategy.return_value
        mock_data_loader.execute_full_data_load.return_value = True

        success = orchestrator.run_data_loading(data_loader_strategy=mock_data_loader)

        self.assertTrue(success)
        mock_data_loader.execute_full_data_load.assert_called_once()
        self.mock_logger.info.assert_any_call(
            f"DeployOrchestrator: Delegating data loading to the {type(mock_data_loader).__name__} strategy."
        )

    @patch('deploy.src.orchestrator.DataLoaderStrategy')
    def test_run_data_loading_failure(self, MockDataLoaderStrategy):
        """Test failed execution of data loading."""
        orchestrator = DeployOrchestrator(database_type='postgres')
        mock_data_loader = MockDataLoaderStrategy.return_value
        mock_data_loader.execute_full_data_load.return_value = False

        success = orchestrator.run_data_loading(data_loader_strategy=mock_data_loader)

        self.assertFalse(success)
        mock_data_loader.execute_full_data_load.assert_called_once()
        self.mock_logger.error.assert_any_call("An unexpected error occurred during data loading delegation: False") # Error message with simple False as exception

    @patch('deploy.src.orchestrator.DataLoaderStrategy')
    def test_run_data_loading_exception(self, MockDataLoaderStrategy):
        """Test run_data_loading handles exceptions from data loader strategy."""
        orchestrator = DeployOrchestrator(database_type='postgres')
        mock_data_loader = MockDataLoaderStrategy.return_value
        mock_data_loader.execute_full_data_load.side_effect = RuntimeError("Data load error")

        success = orchestrator.run_data_loading(data_loader_strategy=mock_data_loader)

        self.assertFalse(success)
        mock_data_loader.execute_full_data_load.assert_called_once()
        self.mock_logger.error.assert_called_once() # Check that an error was logged
        self.assertIn("An unexpected error occurred during data loading delegation:", self.mock_logger.error.call_args[0][0])


# This makes the test runnable directly
if __name__ == '__main__':
    unittest.main()