## Instructions to work on the MkDocs migration.

1. Clone the repository.

    If you already have this repository, you can skip this step.
    ```bash
    git clone https://github.com/saezlab/cache-manager.git
    ```
2. Navigate into the repository directory:
    ```bash
    cd cache-manager
    ```
3. Check out the `docs/mkdocs-migration` branch:
    ```bash
    git checkout docs/mkdocs-migration
    ```
4. Install dependencies using Poetry:
    ```bash
    poetry install
    ```
5. Start the MkDocs development server:
    ```bash
    poetry run python -m mkdocs serve
    ```
6. Open the site in your browser: http://127.0.0.1:8000/cache-manager/