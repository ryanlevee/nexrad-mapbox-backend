# NEXRAD Mapbox Backend

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**Live Demo:** [**NEXRAD Mapbox on Netlify**](https://nexradmapbox.netlify.app/)
###### **NOTE**: Current demo displays data from March 21, 2025 for the KPDT site only. Real-time data functionality is available, but turned off for the moment due to cost. 

**Frontend Repository:** [**NEXRAD Mapbox backend repository**](https://github.com/ryanlevee/nexrad-mapbox)


## Overview

This repository contains the backend services for the [NEXRAD Mapbox Viewer](https://nexradmapbox.netlify.app/) project. It consists of two main parts:

1.  **Data Processing Scripts:** Python scripts that fetch raw NEXRAD Level 2 and Level 3 data from public AWS S3 buckets (NOAA and Unidata), process them using Py-ART and Matplotlib to generate PNG image overlays and JSON metadata, and upload these processed files to a dedicated project S3 bucket.
2.  **Flask API:** A Python Flask web server that serves the processed data (images, metadata, file lists) from the project S3 bucket to the frontend application via a REST API.


## Architecture Overview

The backend utilizes a standard Python `src` layout for organization:

* **`src/nexrad_backend/`**: The main Python package containing all core logic.
    * **`api/`**: Handles the Flask web application setup (`app_factory.py`) and defines the HTTP API endpoints/routes (`routes.py`) using a Flask Blueprint.
    * **`services/`**: Contains modules that interact with external systems or manage specific data domains:
        * `s3_service.py`: Low-level functions for all interactions with AWS S3 (get, put, list, delete, check existence). Handles both project and public bucket clients.
        * `nexrad_fetcher.py`: Functions responsible for finding and downloading raw NEXRAD data (Level 2 from NOAA, Level 3 from Unidata) from their public S3 buckets.
        * `metadata_service.py`: Functions for managing the application's metadata stored in the *project's* S3 bucket (JSON file lists, product code options, update flags).
    * **`processing/`**: Contains the core scientific data processing logic:
        * `common.py`: Shared helper functions used by different processing steps (e.g., bounding box calculation, saving plots to buffer, file cleanup).
        * `level2.py`: Logic specific to reading Level 2 files, processing each radar sweep (tilt), generating plots/metadata using Py-ART/Matplotlib, and preparing results for S3 upload.
        * `level3.py`: Logic specific to reading Level 3 files, normalizing filenames, generating plots/metadata, and preparing results for S3 upload.
    * **`utils/`**: General utility functions (e.g., `list_helpers.py`).
    * **`config.py`**: Centralized configuration loading from environment variables (`.env`) and definition of constants (bucket names, paths, sites, etc.).
* **`scripts/`**: Contains standalone Python scripts that act as entry points for the data processing workflows. These scripts import and orchestrate calls to functions within the `nexrad_backend` services and processing modules.
    * `process_level2.py`: Runs the complete workflow for fetching, processing, and updating Level 2 data.
    * `process_level3.py`: Runs the complete workflow for fetching, processing, and updating Level 3 data.
* **`server.py`**: The main entry point at the project root for starting the Flask API server using Waitress. It uses the app factory from `nexrad_backend.api.app_factory`.
* **`pyproject.toml`**: Defines project metadata, dependencies, and build system configuration, enabling standard Python packaging and installation workflows.

**Data Processing Flow (Triggered Periodically):**

```ascii
+-----------------------------+
| Public S3 (NOAA L2/Unidata L3)|  Raw Data Source
+-------------┬---------------+
              │ Finds/Downloads
              │ [nexrad_fetcher service]
              ▼
+-----------------------------+
|   scripts/*.py              |  Orchestration (Scheduled)
| (Downloads to Local Temp)   |
+-------------┬---------------+
              │ Processes Files
              │ [processing.L2/L3 modules] using PyArt/Matplotlib
              ▼
+-----------------------------+
|   s3_service                |  Uploads Processed Data & Metadata
|   (Handles S3 Put/Delete)   |  (Also called by metadata_service)
+-------------┬---------------+
              │ Stores Processed Files & Updates Metadata Files
              ▼
+-----------------------------+
| Project S3 (nexrad-mapbox)  |  Central Storage
|  - plots_level*/ (PNG/JSON) |
|  - lists/*.json             |
|  - codes/options.json       |
|  - flags/update_flags.json  |
+-----------------------------+
```

**API Serving Flow (Handles Frontend Requests):**
```ascii
+-----------------------------+
| Project S3 (nexrad-mapbox)  |  Central Storage
+-------------┬---------------+
              │ Reads Data & Metadata Files
              │ (via metadata_service & s3_service)
              ▼
+-----------------------------+
|   Flask API (server.py)     |  Serves Data via HTTP Endpoints
+-------------┬---------------+
              │ HTTP Requests / JSON & PNG Responses
              ▼
+-----------------------------+
|   Frontend (SolidJS App)    |  Consumes API
+-----------------------------+
```

**Explanation of Flow:**

1.  **Processing:** The `scripts/process_level*.py` files are run on a schedule. They use the `nexrad_fetcher` service to find and download recent raw data from public S3 buckets to temporary local storage. These scripts then use the `processing` modules (which utilize Py-ART/Matplotlib) to generate PNG plots and JSON metadata. The `s3_service` is used to upload these processed files to your project's S3 bucket. The `metadata_service` (also using `s3_service`) updates the JSON file lists, code counts, and update flags within the project S3 bucket. Finally, `s3_service` cleans up old processed files from your bucket.
2.  **Serving:** The `Flask API` (running via `server.py`) receives HTTP requests from the Frontend. It uses the `metadata_service` to get file lists/codes/flags (which reads JSON files from the Project S3 via `s3_service`) and uses the `s3_service` directly to retrieve specific plot (PNG) or metadata (JSON) files requested by the frontend, sending them back as HTTP responses.


## Features

* **Automated NEXRAD Data Processing:**
    * Fetches Level 2 (Reflectivity) and Level 3 (Hydrometeor, Precipitation) data.
    * Uses `Py-ART` for scientific radar data interpretation.
    * Generates transparent PNG plot overlays using `Matplotlib`.
    * Calculates and saves geographic bounding box JSON metadata for each plot.
    * Handles multi-sweep Level 2 data, generating plots/JSON per tilt angle.
    * Utilizes `asyncio` and `ProcessPoolExecutor` for concurrent downloading and processing.
* **REST API for Frontend:**
    * Serves processed PNG images and JSON metadata.
    * Provides lists of available files for different products/levels.
    * Provides lists of available Level 3 product codes and their file counts.
    * Handles CORS for frontend integration.
* **AWS S3 Integration:**
    * Leverages public S3 buckets for raw data input.
    * Uses a dedicated S3 bucket for storing processed output (plots, JSON, lists).
    * Includes utility for cleaning up old files from the S3 bucket.
* **Configuration:** Uses environment variables for AWS credentials and other settings.
* **Modular Structure:** Code is organized into services, processing modules, and API components for better maintainability and testability using a standard `src` layout.


## Technology Stack

* **Language:** Python (3.9+ recommended)
* **API Framework:** Flask
* **WSGI Server:** Waitress
* **AWS SDK:** Boto3
* **Scientific Computing:** Py-ART (likely `pyart-mch`), NumPy
* **Plotting:** Matplotlib
* **Asynchronous Programming:** asyncio, concurrent.futures
* **Environment Management:** python-dotenv
* **Cloud Storage:** AWS S3
* **Timezone Handling:** pytz
* **Packaging:** Setuptools (via `pyproject.toml`)


## API Endpoints (`src/nexrad_backend/api/routes.py`)

| Method    | Path                                   | Description                                                                                                | Success Response | Error Responses |
| :-------- | :------------------------------------- | :--------------------------------------------------------------------------------------------------------- | :--------------- | :-------------- |
| `GET`     | `/code/`                               | Retrieves the `codes/options.json` file containing Level 3 product code options and counts.                | 200 (JSON Body)  | 404, 500        |
| `GET`     | `/flag/`                               | Retrieves the `flags/update_flags.json` file.                                                              | 200 (JSON Body)  | 404, 500        |
| `POST`    | `/flag/`                               | Updates the `flags/update_flags.json` file with the provided JSON body. *(Note: No auth implemented)* | 200 (JSON Body)  | 400, 500        |
| `GET`     | `/list/<level>/<product>/`             | Retrieves the JSON file list for a specific level and product (e.g., `/list/2/reflectivity/`).             | 200 (JSON Body)  | 400, 404, 500   |
| `GET`     | `/list-all/`                           | Retrieves and combines JSON file lists for all primary products (reflectivity, hydrometeor, precipitation). | 200 (JSON Body)  | 500             |
| `GET`     | `/data/<level>/<path:file_key>/<ext>` | Retrieves a specific data file (e.g., `/data/2/..._idx0/png`). `ext` is `png` or `json`.                  | 200 (PNG/JSON)   | 400, 404, 500   |
| `OPTIONS` | `/*`                                   | Handles CORS preflight requests (managed automatically by Flask-CORS).                                     | 200              | -               |


## Data Processing Scripts

The data processing logic is initiated via scripts in the `/scripts` directory:

* **`scripts/process_level2.py`:** Orchestrates the workflow for Level 2 data:
    * Finds recent raw files on the public NOAA S3 bucket using the `nexrad_fetcher` service.
    * Filters out files already processed based on lists managed by the `metadata_service`.
    * Downloads new files concurrently using `nexrad_fetcher`.
    * Processes downloaded files concurrently using the `processing.level2` module, which generates/uploads individual sweep plots (PNG) and metadata (JSON) to the project S3 via the `s3_service`.
    * Updates the Level 2 file list and processing flag in the project S3 via `metadata_service`.
    * Cleans up old processed L2 files from the project S3 via `s3_service`.
* **`scripts/process_level3.py`:** Orchestrates the workflow for Level 3 data (per product type like 'hydrometeor', 'precipitation'):
    * Fetches product code configuration using `metadata_service`.
    * Finds recent raw files on the public Unidata S3 bucket for relevant codes using `nexrad_fetcher`.
    * Filters out files already processed (using normalized keys).
    * Downloads new files concurrently.
    * Processes downloaded files concurrently using the `processing.level3` module, which generates/uploads plots (PNG) and metadata (JSON) to the project S3.
    * Updates the Level 3 file list, product code counts (`options.json`), and processing flag for the specific product type via `metadata_service`.
    * Cleans up old processed L3 files from the project S3 via `s3_service`.

These scripts are designed to be run periodically (e.g., every 5-15 minutes) using a scheduler like `cron` or a cloud scheduling service to keep the data served by the API up-to-date.


## Setup & Configuration

1.  **Clone the repository:**
    ```bash
    git clone [https://github.com/your-username/nexrad-mapbox-backend.git](https://github.com/your-username/nexrad-mapbox-backend.git)
    cd nexrad-mapbox-backend
    ```

2.  **Create a Python Virtual Environment:** (Python 3.9+ recommended)
    ```bash
    # Using venv
    python -m venv venv
    source venv/bin/activate  # Linux/macOS
    # or Venv\Scripts\activate # Windows (cmd)
    # or Venv\Scripts\Activate.ps1 # Windows (PowerShell)

    # OR Using Conda (Recommended for easier handling of pygrib/eccodes on Windows)
    # conda create --name nexrad_env python=3.9
    # conda activate nexrad_env
    # conda install -c conda-forge eccodes pygrib pyart-mch matplotlib numpy pytz # Install key deps via conda
    ```

3.  **Install Dependencies:** The project uses `pyproject.toml`. Install the package itself in editable mode along with development dependencies (like Ruff linter/formatter):
    ```bash
    # Make sure you are in the project root (nexrad-mapbox-backend/)
    # Ensure pip is up-to-date: python -m pip install --upgrade pip

    # If using Venv:
    pip install -e .[dev]

    # If using Conda (after conda install step above):
    # Install remaining deps (Flask, Boto3, etc.) AND the local package
    pip install flask flask-cors boto3 python-dotenv waitress # Add any others not installed via conda
    pip install -e .[dev] # Installs local package and dev tools from pyproject.toml
    ```
    *(The `-e` flag installs your `nexrad_backend` package in editable mode, linking to your `src` directory. The `[dev]` installs optional dependencies listed under `project.optional-dependencies.dev` in `pyproject.toml`)*.

4.  **Set up Environment Variables:**
    * Create a `.env` file in the project root (`nexrad-mapbox-backend/.env`).
    * Add your AWS credentials and desired region. Ensure these credentials have S3 read/write/delete permissions for your target bucket (`PROJECT_S3_BUCKET` defined in config, defaults to `nexrad-mapbox`).
        ```dotenv
        # nexrad-mapbox-backend/.env

        AWS_ACCESS_KEY_ID=YOUR_AWS_ACCESS_KEY_ID
        AWS_SECRET_ACCESS_KEY=YOUR_AWS_SECRET_ACCESS_KEY
        AWS_REGION=us-west-1 # Or your preferred region

        # Optional: Override defaults from config.py if needed
        # PROJECT_S3_BUCKET=my-custom-bucket-name
        # RADAR_SITE_L2=KSEA
        # RADAR_SITE_L3=SEA
        # PROCESSING_WINDOW_MINUTES=120
        # API_PORT=5000
        ```
    * **Security:** Add `.env` to your `.gitignore` file to avoid committing credentials.

5.  **AWS S3 Bucket Setup:**
    * Ensure the target S3 bucket (e.g., `nexrad-mapbox`) exists in your specified AWS region.
    * The processing scripts create necessary prefixes (`plots_level2/`, `lists/`, etc.) if they don't exist.
    * Initial empty JSON files (`codes/options.json`, `flags/update_flags.json`) might be needed in S3 for the scripts/API to read successfully on their very first run, or ensure the `metadata_service` handles their absence gracefully (it currently returns empty dicts).


## Running the Application

1.  **Run the API Server:**
    * Activate your virtual environment (`source venv/bin/activate` or `conda activate nexrad_env`).
    * Run from the project root:
        ```bash
        python server.py
        ```
    * The API will be available at `http://<API_HOST>:<API_PORT>` (defaults to `http://0.0.0.0:4000`). Access via `http://localhost:4000` or `http://127.0.0.1:4000` from your local machine.

2.  **Run the Data Processing Scripts:**
    * Activate your virtual environment.
    * Run manually from the project root:
        ```bash
        python scripts/process_level2.py
        python scripts/process_level3.py
        ```
    * **Note:** These scripts should ideally be run periodically via a scheduler (`cron`, Task Scheduler, cloud service) rather than manually long-term.


## Deployment

* **API Server:** The API (`server.py` using Waitress) can be deployed to various PaaS/server environments (Render, Heroku, AWS EC2/ECS, etc.).
    * Ensure the deployment environment has Python and access to the necessary environment variables (AWS credentials, etc.).
    * The build process should install dependencies via `pip install .` (reading `pyproject.toml`) or `pip install -r requirements.txt` (if generated from `pyproject.toml` for pinning).
    * **Render.com Start Command Example:** `python server.py`
* **Processing Scripts:** The scripts (`scripts/process_level*.py`) need to be executed on a schedule. Suitable options include:
    * `cron` jobs on a Linux server.
    * Scheduled Tasks on Windows Server.
    * Cloud-based schedulers triggering container tasks or serverless functions (e.g., AWS EventBridge Scheduler + Lambda/Fargate, Google Cloud Scheduler + Cloud Run/Functions, Render Cron Jobs).


## License

This project is licensed under the MIT License - see the [LICENSE](https://github.com/ryanlevee/nexrad-mapbox-backend/blob/main/LICENSE) file for details.


## Contact

Ryan Levee - [GitHub](https://github.com/ryanlevee) | [LinkedIn](https://www.linkedin.com/in/ryanlevee/) | [Email](mailto:ryanlevee@gmail.com)
