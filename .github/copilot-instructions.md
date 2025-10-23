# IFRS_taxonomy — GitHub Copilot Project Instructions

This repository implements an ETL workflow to convert IFRS Accounting Taxonomy files into
structured, machine‑readable formats. Use these instructions whenever proposing code, refactors,
tests, or notebooks to ensure consistency across the project.

## How to apply these rules
- Always read configuration before coding logic; never hardcode parameters or paths.
- Prefer explicit, readable code over clever or implicit constructs.
- Add or update pytest tests for all new or modified modules.
- Use logging for traceability. Do not use print.
- Keep business logic out of notebooks.


## 1 Configuration and paths
- Load all parameters, file paths, and business rules from YAML under `config/*_config.yaml`.
- Do not hardcode paths, constants, or thresholds in code.
- Use `pathlib.Path` for all filesystem work (no `os.path`).
- Pass configuration objects or specific values explicitly into functions (no hidden globals).

## 2 Data I/O and directories
- Save intermediate artifacts to `data/intermediate/`.
- Save final outputs to `data/output/`.
- Create parent directories when writing files (`Path(...).parent.mkdir(parents=True, exist_ok=True)`).
- Prefer deterministic, idempotent writes (separate temp files + atomic move when appropriate).

## 3 Logging and traceability
- Use the `logging` module; never use `print`.
- Create a module‑level logger: `logger = logging.getLogger(__name__)`.
- Log key steps, inputs, shapes/sizes, and decisions (at INFO); log anomalies and retries (at WARNING);
  only use DEBUG for detailed internals.

## 4 Testing (pytest)
- Write unit tests for all new or refactored modules.
- Favor small, fast, deterministic tests.
- Use `tmp_path` for filesystem tests and parametrize where it improves coverage.
- Keep fixtures minimal and focused.
- Test edge cases (empty inputs, missing columns, unexpected dtypes).

## 5 Notebooks policy
- Notebooks are for exploration, visualization, and interaction only.
- No business logic in notebooks. Move reusable logic into modules and import into notebooks.
- Keep notebooks reproducible by reading from config and using saved inputs.

## 6 Code style
- Follow PEP 8 across the codebase.
- Enforce max line length of 100 characters.
- Use f‑strings for string formatting.
- Prefer explicit imports and names.
- Name modules, files, and variables with `snake_case`. Constants in `UPPER_SNAKE_CASE`.

## 7 Type hinting
- Add type hints to all function signatures.
- Use built‑in generics (e.g., `list[int]`, `dict[str, float]`, `tuple[str, int]`).
- Use `| None` for optionals (e.g., `Path | None`).
- Import from `typing` only when needed (e.g., `Any`, `Callable`, `TypeVar`).

## 8 Library usage
- Use pandas for data manipulation.
- Prefer vectorized pandas/numpy operations; avoid `.iterrows()` unless strictly necessary.
- Use numpy only for lower‑level array ops that pandas cannot do efficiently.
- Use scikit‑learn for ML workflows where applicable.
- Prefer pandas nullable dtypes (e.g., `Int64`, `boolean`, `string`) over legacy numpy dtypes.

## 9 Visualization
- Prefer seaborn and matplotlib.
- With matplotlib, always use the OO API:
  - Explicitly create `Figure` and `Axes` (`fig, ax = plt.subplots(...)`).
  - Call methods on `Axes` for plotting.
- Do not rely on the stateful pyplot API for production plots.

## 10 ETL architecture guidance
- Keep ETL phases separated and composable:
  - Extract: reading raw inputs, decoding formats, basic normalization.
  - Transform: schema alignment, cleaning, enrichment, feature engineering.
  - Load: writing validated outputs to target stores/paths.
- Prefer pure functions for transformations (input -> output without side effects).
- Pass `config` and `Path` objects explicitly into ETL steps.
- Validate schemas and required columns at ETL boundaries; fail fast with clear errors.

## 11 Error handling and validation
- Validate inputs early (presence of files, required columns, expected dtypes).
- Raise informative exceptions; include context in error messages.
- Use logging to record the failure path and any recovery attempts.
- Avoid catching broad exceptions unless re‑raising with context.

## 12 Performance considerations
- Avoid row‑by‑row operations (`iterrows`, `itertuples`) unless necessary.
- Use vectorized operations, `map`, `where`, joins/merges, and groupby transforms.
- When reading large files, consider chunked reads if memory‑constrained (keep API consistent).
- Drop intermediates from memory once no longer needed; write to `data/intermediate/` if reused.
