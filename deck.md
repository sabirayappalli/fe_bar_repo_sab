---
title: FreshMart Stockout Prediction on Databricks
subtitle: Retail / CPG - Grocery replenishment prototype (synthetic data)
---

# 1. The outcome

* Model AUC **0.921** vs **0.848** for the legacy min/max rule (October 2026 hold-out)
* **82%** precision on the top 5% riskiest items vs **63%** legacy
* **$2.83M** estimated lost sales in 6 months; **$1.4M-$2.0M/year** recoverable (assumption: 25-35% recovery)
* Risk, order quantity and a plain-language reason for every store-SKU, served from Lakebase and Genie

---

# 2. Customer and challenge

**FreshMart** (fictional) - 40 grocery stores, 150 SKUs, 10 suppliers

* ~7% of store-SKU days are out of stock (benchmark ~8%)
* Legacy rule: reorder when `on_hand <= avg_demand x (lead_time + 3)`
  * Ignores lead-time variability, supplier reliability and promotions
  * Flags 51% of in-stock items, so alerts are ignored
* Worst pockets: promo days (Household 50% stockout rate on promo vs 21% off promo), unreliable suppliers (CleanHome 27% in October vs Golden Bakery 0.2%)

---

# 3. Architecture

```
UC Volume  ->  Lakeflow SDP (Auto Loader)  ->  bronze -> silver -> gold
                                                     |
                     Unity Catalog: masks, comments, lineage, tags, grants
                                                     |
              features -> LightGBM -> MLflow -> UC registry @champion
                                                     |
              stockout_predictions -> ai_query rationale -> recommendations
                          |                                   |
                 Lakebase Postgres (synced)              Genie Agent
                          |
               Databricks App (store managers, PO approvals)
```

---

# 4. Lakeflow pipeline

* Serverless Lakeflow Spark Declarative Pipeline, Python, Photon
* Auto Loader over CSV/JSON in a UC Volume, adds `source_file`, `ingest_ts`
* Expectations drop bad rows: 552 null SKU, 921 negative units; 1,893 duplicates removed with a 3-day watermark
* Gold: `gold_store_sku_daily` (1.104M rows, unique per day-store-SKU), supplier performance, region x category KPIs with lost-demand estimate

---

# 5. Unity Catalog governance

* `mask_pii` function + column masks on store manager name, email, phone (only `stockout_pii_readers` see values)
* Table and column comments on all 34 tables
* Lineage captured bronze -> silver -> gold -> features and predictions -> recommendations -> Lakebase
* Tags (domain, layer, contains_pii) and least-privilege grants scripted in notebook 02

---

# 6. ML: stockout risk model

* Label: stockout in the next max(3, lead time) days, for items in stock today
* Point-in-time features: velocity, censored-demand-aware cover, cover minus lead time, on-order, supplier lead time and fill rate, promo lift, stockout history
* Train May-Sep, test October, with a purge gap
* LightGBM -> MLflow -> `stockout_ml.stockout_risk_model@champion`

| Oct 2026 test | Model | Legacy |
| --- | --- | --- |
| AUC | 0.921 | 0.848 |
| PR-AUC | 0.681 | 0.484 |
| Precision top 5% | 0.822 | 0.634 |

---

# 7. From score to action

* Order quantity = demand over lead time + review day + safety stock (z = 1.65, demand and lead-time variability), rounded to case pack, capped at shelf capacity
* Key drivers from SHAP: e.g. "cover -5.5 days vs lead time; supplier lead time 7.0 days; low on-hand (4 units)"
* `ai_query` (Llama 3.3 70B) writes a 2-3 sentence rationale for the top 200
* Insight: 134 of the top 200 need **expediting**, not more units (shelf full or already on order)

---

# 8. Serving: Lakebase, Genie, App

* Lakebase Postgres project `freshmart-stockout`: synced tables for predictions (6,000) and recommendations (200), counts verified in Postgres
* Genie Agent "FreshMart Stockout Analyst": measures, joins, example SQL, PII guardrail; 5/5 benchmarks correct
* Databricks App `freshmart-replenishment` (live): store manager sees the ranked risk list and AI rationale, then approves, edits, rejects or expedites the PO; each decision is written back to Lakebase (`replenishment_app.po_approvals`, verified)

---

# 9. Business impact

* Lost sales today: $2.83M in 6 months (~$5.7M/year, ~$141k/store)
* Recover 25-35% -> **$1.4M-$2.0M/year** (to validate in a pilot)
* Fewer, better alerts: ranked list with 82% hit rate at the top instead of flags on 51% of items
* Faster answers for category and supply managers via Genie
* Supplier conversations backed by data: on-time and fill rates per supplier

---

# 10. How it was built: AI tools and workflow

* Databricks Genie Code (in-workspace AI assistant) for code generation, execution and verification across notebooks, pipeline editor, Lakebase and Genie
* Each stage had acceptance checks (row counts, uniqueness, drop counts, metrics) before moving on
* AI-caught issues: negative-unit rule ordering, LLM rationale contradicting a 0-unit order (prompt tightened)

---

# 11. Decisions and trade-offs

* Existing catalog with project schemas (no CREATE CATALOG)
* PII mask inside the pipeline schema so it survives refreshes
* Time split and in-stock-only label for honest metrics
* Contracted lead times as features; actual lead times only for order sizing (avoids leakage)
* LightGBM + SHAP over deep learning; LLM explains, the formula decides the quantity
* Lakebase snapshot sync because predictions are fully rewritten each run

---

# 12. Next steps

* Apply tags and grants; schedule pipeline -> score -> rationale -> sync as one daily job
* Pilot in 5 stores vs 5 control stores for 6 weeks; measure stockout rate and lost sales
* Add promo calendar look-ahead and supplier ETA feeds as features
