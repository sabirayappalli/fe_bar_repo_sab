# FreshMart Stockout Prediction - Databricks prototype

## Outcome first

A lakehouse pipeline that predicts which store-SKU pairs will run out within their supplier lead time, explains why in plain language, and serves it to store managers.

* Model AUC **0.921** vs **0.848** for the legacy min/max rule (October 2026 hold-out, 146,802 in-stock store-SKU days).
* Precision on the top 5% riskiest rows: **82%** vs **63%** for the legacy rule, so each item a store manager reviews is about 30% more likely to be a real risk.
* Estimated lost sales from stockouts: **$2.83M in 6 months** across 40 stores (about $5.7M a year). Recovering 25-35% of that through earlier action is worth about **$1.4M-$2.0M a year** (assumption, see Value math).
* All 6,000 store-SKU pairs are scored on the latest snapshot (2026-10-31). The 200 highest-risk items get an LLM-written rationale. Both are served from Lakebase Postgres and queryable in plain English through a Genie Agent.

All data is synthetic. FreshMart is a fictional grocery chain.

## Business problem

FreshMart (40 stores, 150 SKUs, 10 suppliers) runs about 7% of store-SKU days out of stock (grocery benchmark ~8%). Replenishment uses a static min/max rule, `on_hand <= avg_demand x (lead_time + 3 days)`, which ignores lead-time variability, supplier reliability and promotions. It flags 51% of in-stock pairs, so managers ignore most alerts and still miss stockouts on promoted items and unreliable suppliers.

## Architecture (data journey)

```
UC Volume (CSV/JSON landing)
  -> Lakeflow Spark Declarative Pipeline (Auto Loader, serverless)
       bronze_*  (raw + source_file, ingest_ts)
       silver_*  (typed, expectations drop null sku / negative units, dedup with watermark)
       dim_store / dim_product / dim_supplier (PII masked)
       gold_store_sku_daily, gold_supplier_performance, gold_stockout_kpis_daily
  -> Unity Catalog governance: column masks on PII, comments, lineage, tags/grants (notebook 02)
  -> ML: point-in-time features -> LightGBM -> MLflow -> UC model registry (@champion)
       stockout_predictions (6,000 pairs) -> ai_query rationale -> replenishment_recommendations (200)
  -> Lakebase Postgres (synced tables) -> Databricks App (store managers, PO approvals)
  -> Genie Agent over gold + predictions (analysts, category managers)
```

| Layer | Asset | Evidence |
| --- | --- | --- |
| Raw | `/Volumes/serverless_stable_zlgeaw_catalog/stockout_raw/landing` | 184 daily POS CSVs (~903k rows with injected defects), 1.104M inventory snapshots, 132,089 receipts, 297 promotions |
| Pipeline | Lakeflow pipeline `ef370b68-6c02-4d8e-94b4-426403033591` (serverless, Photon) | Updates b2eb1b10, 6624c364, b68f2d0d completed |
| Bronze | `stockout_core.bronze_*` | pos 948,469; inventory 1,104,000; receipts 132,089 |
| Silver | `stockout_core.silver_*`, `dim_*` | pos 945,103 (dropped 552 null SKU, 921 negative units, 1,893 duplicates) |
| Gold | `gold_store_sku_daily` | 1,104,000 rows, unique on (date, store_id, sku_id), stockout rate 7.08% |
| Gold | `gold_supplier_performance`, `gold_stockout_kpis_daily` | 10 and 8,280 rows |
| Governance | `stockout_core.mask_pii` + column masks on dim_store | 3 masks in information_schema.column_masks; PII returns `***REDACTED***` |
| ML | `stockout_ml.stockout_risk_model` v1 @champion | AUC 0.921, PR-AUC 0.681 |
| Scoring | `stockout_ml.stockout_predictions` | 6,000 rows, 6,000 unique pairs, 0 case-pack or shelf-capacity violations |
| GenAI | `stockout_ml.replenishment_recommendations` | 200 rows, 0 empty rationales |
| Serving | Lakebase project `freshmart-stockout` | Postgres counts 6,000 and 200 match UC |
| Analytics | Genie Agent "FreshMart Stockout Analyst" | 5/5 benchmark questions correct |
| App | Databricks App `freshmart-replenishment` (Streamlit, source in `app/`) | Deployed; reads Lakebase synced tables; test approval written to `replenishment_app.po_approvals` (approval #1, APPROVE 18 units, S019 / SKU0090) |

## Repository layout

```
01_generate_synthetic_raw_data      synthetic stores, products, suppliers, POS, inventory, receipts, promotions
02_uc_governance                    PII mask function, tags, grants, evidence queries, lineage query
03_stockout_model_and_genai         features, time split, LightGBM, MLflow/UC registry, scoring, ai_query
pipeline/transformations/bronze     Auto Loader ingest (4 files)
pipeline/transformations/silver     validation, dedup, dimensions (4 files)
pipeline/transformations/gold       store_sku_daily, supplier_performance, stockout_kpis_daily
app/                                store-manager replenishment app (stage 6)
deck.md                             slide deck
```

## ML design

* **Label:** stockout in the next H days, where H = max(3, ceil(supplier lead time)). Only pairs in stock today are used, so the model predicts new stockouts instead of rediscovering existing ones.
* **Features (past data only):** 7-day and 28-day sales velocity, demand on in-stock days (sales are censored on stockout days), days of cover with and without on-order stock, cover minus lead time, promo-adjusted cover, recent stockout history, supplier lead-time mean/sd and fill rate, promotion flag and lift, day of week, perishability, category.
* **Split:** train May-Sep 2026, test October 2026. Training rows whose label window reaches into October are dropped (purge gap).
* **Order quantity:** demand over (lead time + 1 review day) + safety stock `z=1.65 x sqrt(L x sd_demand^2 + demand^2 x sd_LT^2)`, minus on-hand and on-order, rounded up to case pack and capped at shelf capacity.
* **Explanations:** top 3 positive LightGBM SHAP contributions per pair, stored as `key_drivers`.

| Metric (Oct 2026 test) | Model | Legacy rule |
| --- | --- | --- |
| AUC | 0.921 | 0.848 (0.744 as yes/no flag) |
| PR-AUC | 0.681 | 0.484 |
| Precision, top 5% | 0.822 | 0.634 |
| Recall at the legacy flag rate (51%) | 0.983 | 0.927 |

Top features by gain: cover minus lead time (56%), on-order units (8%), supplier lead time (7%), promo-adjusted cover (5%), promo lift (3%).

## Value math

* Estimated lost sales (trailing demand x stockout days, from `gold_stockout_kpis_daily`): $2,825,657 over 2026-05-01..10-31 on $27.5M net sales; October alone $589,907.
* Annualized: about $5.65M, or about $141k per store.
* Assumption: acting on model alerts (earlier orders, expediting open POs for the 134 of the top 200 items whose risk is timing rather than quantity) recovers 25-35% of lost sales: **$1.4M-$2.0M a year** for 40 stores. This is an assumption to validate with an A/B pilot, not a measured result.
* Labour: the legacy rule flags 51% of in-stock pairs. Working from the model's ranked list, managers review fewer items with higher hit rates (82% vs 63% at the top 5%).

## Decisions and trade-offs

* **No CREATE CATALOG permission:** used the existing catalog with three project schemas (`stockout_raw`, `stockout_core`, `stockout_ml`).
* **Pipeline default schema stuck at `default`:** all tables use fully qualified names.
* **PII mask in the pipeline schema, not ALTER TABLE:** pipeline-managed tables are re-declared on every update, so the mask is part of the table definition.
* **Time split, not random split:** avoids leakage from adjacent days of the same store-SKU.
* **In-stock-only training:** honest metric; out-of-stock pairs get risk 1.0 by rule.
* **Contracted supplier lead times as features, actual lead times only for order sizing:** `gold_supplier_performance` aggregates all POs including October, so using it as a feature would leak.
* **LightGBM over deep models:** tabular data, fast to train, SHAP explanations built in.
* **LLM only for the rationale, not for the number:** quantities come from the deterministic formula; the prompt is told to use the exact quantity and to recommend expediting when the order is 0.
* **Llama 3.3 70B through `ai_query`:** batch SQL, no endpoint to manage; 200 rows is cheap.
* **Lakebase snapshot sync:** predictions are fully overwritten each run, so snapshot is simpler than CDF-based triggered sync.
* **Synthetic data:** realistic rates (7.08% stockouts) with known ground truth (supplier V05 22% stockouts vs V07 0.2%), at the cost of real-world noise.

## AI tools and workflow

Built with Databricks Genie Code (the in-workspace AI assistant) across notebooks, the pipeline editor, Lakebase and the Genie page:

1. Specified the business problem, data model and acceptance checks for each stage.
2. Genie Code generated the code (synthetic data, pipeline files, features, model, prompts), ran it on serverless compute and checked the output: row counts, uniqueness, null checks, expectation drop counts, metric tables.
3. Each stage was verified before moving on, e.g. the POS negative-units rule was fixed after the event log showed 0 drops; an LLM rationale that contradicted a 0-unit order was caught and the prompt was tightened.
4. Genie Agent benchmarks (5/5) and Postgres row counts were used as independent checks.

AI is also part of the product: LightGBM + SHAP for risk and drivers, `ai_query` for manager-facing rationales, and Genie for natural-language analytics.

## Known gaps

* Table and column tags and grants are in notebook 02 but not yet applied (0 table tags at last check).
* Lineage is captured raw->bronze->silver->gold->features and predictions->recommendations->Lakebase, but not features->predictions (scoring goes through pandas).
* Predictions are a single snapshot (2026-10-31); a daily job would refresh pipeline, scoring, rationale and sync.

## How to reproduce

1. Run `01_generate_synthetic_raw_data`.
2. Run the Lakeflow pipeline.
3. Run `02_uc_governance` (tags and grants require a metastore admin or owner).
4. Run `03_stockout_model_and_genai`.
5. Create the Lakebase project and synced tables (see the Lakebase section of the deck).
6. Open the Genie Agent and ask the sample questions.
7. Deploy `app/` as a Databricks App with a Lakebase `postgres` resource (CAN_CONNECT_AND_CREATE), then in Postgres grant the app service principal `USAGE` on schema `stockout_ml` and `SELECT` on its tables. Re-run the grants after a full re-sync of the synced tables.

## Store-manager app

* URL: https://freshmart-replenishment-7474653766284409.aws.databricksapps.com
* Store picker, risk-tier and category filters, ranked risk list (risk %, days of cover, on-hand, on-order, recommended order, SHAP drivers).
* Item review with the AI rationale and Approve / Edit / Reject / Expedite. Edited quantities must be a multiple of the case pack.
* Each decision is written to `replenishment_app.po_approvals` in Lakebase with the signed-in user (from the app's forwarded identity) and timestamp. The schema is created and owned by the app service principal.
* Connects with short-lived OAuth credentials from `generate_database_credential`, refreshed every 40 minutes.
