# 📊 IFRS Taxonomy ETL
Please note: This is still in development and not yet production-ready.

**IFRS Taxonomy ETL** is an open-source Python project that provides an Extract–Transform–Load (ETL) process for converting the **IFRS Accounting Taxonomy 2025 Excel file** into structured, machine-readable formats.  
The goal is to make the taxonomy easier to use in analytics tools, databases, and hierarchical models — while respecting IFRS Foundation intellectual property.

---

## ✨ Features

- Converts the official IFRS Excel taxonomy into:
  - **Adjacency list** (`df_hierarchy`)
  - **Materialized path** (`df_materialized_path`)
  - **Nested JSON tree** (`taxonomy_tree`)
- Preserves taxonomy fidelity while enabling flexible downstream use
- Provides governance-ready outputs with clear documentation
- Designed for educational, analytical, and research purposes

---

## 📂 Project Structure

```
IFRS_taxonomy/
├── src/
│   └── ifrs_taxonomy/
│       ├── __init__.py
│       └── etl/
│           ├── __init__.py
│           ├── extract.py
│           ├── transform.py
│           └── load.py
│
├── config/
│   └── pipeline_config.yaml   # Central runtime configuration
│
├── notebooks/
│   ├── 01_quickstart.ipynb
│   └── 02_exploration.ipynb
│
├── data/
│   └── .gitignore
│
├── docs/
│   ├── output_data_specification.md
│   ├── api_contract.md
│   └── governance_notes.md
│
├── tests/
│   ├── __init__.py
│   ├── test_extract.py
│   ├── test_transform.py
│   └── test_load.py
│
├── requirements.txt
├── README.md
└── CHANGELOG.md

```

---

## ⚠️ Disclaimer

- This project is **not affiliated with, endorsed by, or associated with the IFRS Foundation**.  
- The **IFRS Accounting Taxonomy Excel file is not included** in this repository.  
- Users must **register with the IFRS Foundation** and download the official taxonomy file directly from [ifrs.org](https://www.ifrs.org/issued-standards/ifrs-taxonomy/ifrs-accounting-taxonomy-2025/).  
- The ETL code operates **only on the file once obtained from the official source**.  
- No IFRS content is redistributed, mirrored, or modified here.  
- This project is provided **for educational and analytical purposes only**. Users are responsible for ensuring compliance with IFRS Foundation licensing terms.  
- **IFRS® is a registered trademark of the IFRS Foundation.** All rights to the trademark and related content remain with the IFRS Foundation.

---

## 🚀 Getting Started

1. This project is developed and tested with Python 3.14.0.
2. Clone this repository:
   ```bash
   git clone https://github.com/your-username/IFRS_taxonomy.git
   cd IFRS_taxonomy
   ```
3. Install Python. This project is developed and tested with Python 3.14.0.
4. Create and activate a virtual environment using `venv`
5. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

6. Download the official IFRS Accounting Taxonomy Excel file from [ifrs.org](https://www.ifrs.org/issued-standards/ifrs-taxonomy/ifrs-accounting-taxonomy-2025/).

7. Run the ETL process as a script from the project root:
   ```bash
   python src/etl/ifrs_taxonomy_etl.py
   ```
8. Or, run the ETL process as a module (recommended) from the project root:
   ```bash
   python -m src.etl.ifrs_taxonomy_etl 
   ```

9. Outputs will be generated in the `data/` folder.

---

## 📜 License

This project is released under the MIT License.  
Please note that this license applies **only to the ETL code**. The IFRS Accounting Taxonomy itself remains the intellectual property of the IFRS Foundation.

---

## 🤝 Contributing

Contributions are welcome! Please open an issue or submit a pull request.  
All contributions should respect the IFRS Foundation’s IP boundaries and the disclaimer above.

---

## 📬 Contact

Maintained by **Priyadharshana**.  
For questions, ideas, or collaboration, please connect via LinkedIn or GitHub.
- LinkedIn: [priyadharshana](https://www.linkedin.com/in/priyadharshanaekanayake/)
- GitHub: [priyadharshana](https://github.com/priyadharshana)
