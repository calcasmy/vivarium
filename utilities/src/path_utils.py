'''* This is the most reliable and portable way to get the directory of the current script.
* `__file__` is a special variable that holds the path to the current file.
* `os.path.abspath(__file__)` gets the absolute path to the current file.
* `os.path.dirname(...)` extracts the directory from the absolute path.'''

# import os


# class Path_Utils():
#     """
#     A class fetch or construct folder path.
#     """

#     def project_root():
#         # Get the directory of the current script
#         file_directory = os.path.dirname(os.path.abspath(__file__))
    


# # Construct the full path to config.ini
# config_path = os.path.join(script_dir, "config.ini")

# print(f"Config file path: {config_path}")

# if not os.path.exists(config_path):
#     raise FileNotFoundError(f"Config file not found at {config_path}")

# # Use config_path to open your file

# '''* Benefits:
#     * Always resolves to the correct directory, regardless of how the script is run.
#     * Portable across different operating systems.
#     * Clear and easy to understand.'''

import os

# # Option 1: Define PROJECT_ROOT relative to this file
# file_path = os.path.abspath(__file__)
# file_directory = os.path.dirname(file_path)
# file_src = os.path.dirname(file_directory)
# PROJECT_ROOT = os.path.dirname(file_src)  # Go up three levels from src/utilities

# def get_project_root():
#     return PROJECT_ROOT

# Option 2: Create a class to fetch the folder and file path details.
class PathUtils:
    """
    A utility class for fetching or constructing folder paths.
    """

    @staticmethod
    def get_file_directory(filename: str = __file__):
        """
        Gets the directory of the current file.

        Returns:
            str: The absolute path to the directory of the current file.
        """
        return os.path.dirname(os.path.abspath(filename))

    @staticmethod
    def get_project_root():
        """
        Gets the project root directory (three levels above the current file).

        Returns:
            str: The absolute path to the project root.
        """
        file_directory = PathUtils.get_file_directory()
        file_src = os.path.dirname(file_directory)
        return os.path.dirname(file_src)

    @staticmethod
    def get_config_path(config_file="config.ini"):
        """
        Constructs the full path to a configuration file.

        Args:
            config_file (str, optional): The name of the configuration file.
                Defaults to "config.ini".

        Returns:
            str: The absolute path to the configuration file.

        Raises:
            FileNotFoundError: If the configuration file does not exist.
        """
        project_root = PathUtils.get_project_root()
        config_path = os.path.join(project_root, config_file)
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Config file not found at {config_path}")
        return config_path

# Example Usage (outside the class)
# config_path = PathUtils.get_config_path()
# print(f"Config file path: {config_path}")

# project_root = PathUtils.get_project_root()
# print(f"Project root: {project_root}")
