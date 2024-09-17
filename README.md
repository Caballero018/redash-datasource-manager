# Redash DataSource Manager

This project is a Python utility designed to manage Redash Data Sources via its API. You can perform actions like creating, deleting, and restoring Data Sources, as well as testing connectivity in an automated and concurrent manner.

## Requirements

Before running the project, ensure that you have the following dependencies installed:

- Python 3.8+
- Packages listed in the `requirements.txt` file:

  ```bash
  aiohttp
  python-dotenv

## Installation

1. Clone this repository to your local machine.
2. Install the required dependencies:
  ```bash
  pip install -r requirements.txt
  ```
3. Create a `.env` file in the root directory with the following variables:
  ```bash
  DEV_REDASH_URL=<development_url>
  DEV_API_KEY=<development_api_key>
  PROD_REDASH_URL=<production_url>
  PROD_API_KEY=<production_api_key>
  ```

## Usage

This script provides two main functionalities:

1. **Delete Data Sources**
  * You can delete Data Sources by database name, failed test results, or by ID.
  * Deletions are managed concurrently using semaphores to limit the number of simultaneous requests to Redash.
2. **Restore Data Sources**
  * You can restore previously deleted Data Sources using a backup JSON file generated during the deletion process.

## Running the Script

To launch the interactive menu, run the following command:
```bash
python main.py
```

## Menu Options

1. **Delete Data Sources:** You will be asked whether you want to perform the action in the development or production environment. The options include:
* By database name (without country prefix)
* By failed test result
* By ID
2. **Restore Data Sources:** Allows you to restore Data Sources from a previously generated backup file.
3. **Exit:** Option to exit the program.

## Environment Variables

The script uses environment variables stored in the .env file to manage the URLs and API keys required to interact with Redash:

* `DEV_REDASH_URL`: Development environment URL.
* `DEV_API_KEY`: API Key for the development environment.
* `PROD_REDASH_URL`: Production environment URL.
* `PROD_API_KEY`: API Key for the production environment.

## Data Source Backup

Whenever you delete Data Sources, the script automatically generates a backup file in the `temp/` folder with a unique name. This file can be used to restore the Data Sources if necessary.
