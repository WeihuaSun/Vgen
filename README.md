# Vgen Log Generator

Vgen is a log generator for simulating various transaction histories. It supports multiple workloads such as `BlindW-WH`, `BlindW-RH`, `BlindW-WR`, `TPC-C`, `C-Twitter`, `BlindW-Pred` and `COO`, and can be used to generate synthetic transaction logs for database experiments.

## Requirements

Before using Vgen, make sure you have the following prerequisites installed:
- **Python 3.x** (preferably Python 3.6 or later)
- **PostgreSQL** (for PostgreSQL-based workloads)
- **MySQL** (for MySQL-based workloads)
- **Required Python libraries**: `argparse`, `psycopg2`, `mysql-connector-python` (install via `pip`)

## Setup

Clone the repository to your local machine:

```bash
git clone https://github.com/WeihuaSun/Vgen.git
cd vgen
```

Configure database connection information in `config.py`.

## Usage

```bash
sh gen.sh
```
