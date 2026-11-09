"""FreshMart Replenishment - store-manager app.

Reads model risk scores and LLM rationales from Lakebase synced tables
(stockout_ml.pg_stockout_predictions, stockout_ml.pg_replenishment_recommendations)
and writes PO decisions to replenishment_app.po_approvals (owned by the app service principal).
"""
import os
import time

import pandas as pd
import psycopg
import streamlit as st
from databricks.sdk import WorkspaceClient

DATA_SCHEMA = os.getenv("DATA_SCHEMA", "stockout_ml")
APP_SCHEMA = os.getenv("APP_SCHEMA", "replenishment_app")
PRED = f"{DATA_SCHEMA}.pg_stockout_predictions"
RECS = f"{DATA_SCHEMA}.pg_replenishment_recommendations"
APPROVALS = f"{APP_SCHEMA}.po_approvals"
TIER_ORDER = ["Out of stock", "High", "Medium", "Low", "Minimal"]

st.set_page_config(page_title="FreshMart Replenishment", layout="wide")


# ---------- Lakebase connection (fresh OAuth token per connection, tokens last ~1h) ----------
@st.cache_resource
def _workspace():
    return WorkspaceClient()


_token = {"value": None, "ts": 0.0}


def _password():
    if _token["value"] is None or time.time() - _token["ts"] > 40 * 60:
        cred = _workspace().postgres.generate_database_credential(endpoint=os.environ["LAKEBASE_ENDPOINT"])
        _token.update(value=cred.token, ts=time.time())
    return _token["value"]


def connect():
    return psycopg.connect(
        host=os.environ["PGHOST"],
        port=int(os.environ.get("PGPORT", 5432)),
        dbname=os.environ.get("PGDATABASE", "databricks_postgres"),
        user=os.environ["PGUSER"],
        password=_password(),
        sslmode=os.environ.get("PGSSLMODE", "require"),
        application_name=os.environ.get("PGAPPNAME", "freshmart-replenishment"),
        connect_timeout=15,
    )


def query_df(sql, params=None):
    with connect() as conn, conn.cursor() as cur:
        cur.execute(sql, params or ())
        cols = [d.name for d in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=cols)


@st.cache_resource
def ensure_app_schema():
    """The app service principal creates (and therefore owns) its write-back schema."""
    with connect() as conn, conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {APP_SCHEMA}")
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {APPROVALS} (
                approval_id BIGSERIAL PRIMARY KEY,
                snapshot_date DATE NOT NULL,
                store_id TEXT NOT NULL,
                sku_id TEXT NOT NULL,
                sku_name TEXT,
                supplier_id TEXT,
                risk_score DOUBLE PRECISION,
                risk_tier TEXT,
                recommended_order_units INTEGER,
                approved_order_units INTEGER NOT NULL,
                action TEXT NOT NULL CHECK (action IN ('APPROVE', 'EDIT', 'REJECT', 'EXPEDITE')),
                note TEXT,
                decided_by TEXT NOT NULL,
                decided_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )""")
        cur.execute(f"CREATE INDEX IF NOT EXISTS po_approvals_store_idx ON {APPROVALS} (store_id, decided_at DESC)")
        conn.commit()
    return True


def current_user():
    try:
        h = st.context.headers
        return h.get("X-Forwarded-Email") or h.get("X-Forwarded-Preferred-Username") or "unknown"
    except Exception:
        return "unknown"


@st.cache_data(ttl=300)
def load_stores():
    return query_df(f"SELECT DISTINCT store_id, store_name, region FROM {PRED} ORDER BY store_id")


@st.cache_data(ttl=300)
def load_store_items(store_id):
    return query_df(f"""
        SELECT p.snapshot_date, p.store_id, p.sku_id, p.sku_name, p.category, p.supplier_id, p.supplier_name,
               p.risk_score, p.risk_tier, p.days_of_cover, p.on_hand_units, p.on_order_units,
               p.avg_demand_28d, p.on_promo, p.horizon_days, p.case_pack, p.shelf_capacity,
               p.recommended_order_units, p.capped_by_shelf, p.legacy_flag, p.key_drivers, r.rationale
        FROM {PRED} p
        LEFT JOIN {RECS} r ON r.store_id = p.store_id AND r.sku_id = p.sku_id
        WHERE p.store_id = %s
        ORDER BY p.risk_score DESC, p.days_of_cover ASC""", (store_id,))


def save_decision(row, action, qty, note, user):
    with connect() as conn, conn.cursor() as cur:
        cur.execute(f"""
            INSERT INTO {APPROVALS} (snapshot_date, store_id, sku_id, sku_name, supplier_id, risk_score, risk_tier,
                                     recommended_order_units, approved_order_units, action, note, decided_by)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING approval_id""",
            (row.snapshot_date, row.store_id, row.sku_id, row.sku_name, row.supplier_id, float(row.risk_score),
             row.risk_tier, int(row.recommended_order_units), int(qty), action, note or None, user))
        new_id = cur.fetchone()[0]
        conn.commit()
    return new_id


def drivers_text(v):
    if isinstance(v, list):
        return "; ".join(map(str, v))
    return "" if v is None else str(v)


# ---------- UI ----------
st.title("FreshMart Replenishment")
st.caption("Stockout risk from the champion model, recommended orders and AI rationale. Synthetic data.")

try:
    ensure_app_schema()
    stores = load_stores()
except psycopg.errors.InsufficientPrivilege as e:
    st.error("The app service principal cannot read the synced tables yet. "
             f"Grant USAGE on schema {DATA_SCHEMA} and SELECT on its tables to role {os.environ.get('PGUSER')}.")
    st.code(str(e))
    st.stop()
except Exception as e:
    st.error("Could not connect to Lakebase.")
    st.code(f"{type(e).__name__}: {e}")
    st.stop()

user = current_user()
labels = {r.store_id: f"{r.store_name} ({r.region})" for r in stores.itertuples()}
with st.sidebar:
    store_id = st.selectbox("Store", list(labels), format_func=labels.get)
    tiers = st.multiselect("Risk tier", TIER_ORDER, default=["Out of stock", "High", "Medium"])
    items_all = load_store_items(store_id)
    cats = sorted(items_all["category"].dropna().unique())
    cat_sel = st.multiselect("Category", cats)
    st.caption(f"Signed in as {user}")

items = items_all[items_all["risk_tier"].isin(tiers)] if tiers else items_all
if cat_sel:
    items = items[items["category"].isin(cat_sel)]

snap = items_all["snapshot_date"].max()
c1, c2, c3, c4 = st.columns(4)
c1.metric("Snapshot", str(snap))
c2.metric("Out of stock", int((items_all["risk_tier"] == "Out of stock").sum()))
c3.metric("High risk", int((items_all["risk_tier"] == "High").sum()))
c4.metric("Units to order (shown)", int(items["recommended_order_units"].sum()))

tab_list, tab_hist = st.tabs(["Risk list", "My decisions"])

with tab_list:
    view = items.assign(
        risk_pct=(items["risk_score"] * 100).round(0),
        days_of_cover=items["days_of_cover"].round(1),
        drivers=items["key_drivers"].map(drivers_text),
    )[["sku_id", "sku_name", "category", "supplier_name", "risk_tier", "risk_pct", "days_of_cover",
       "on_hand_units", "on_order_units", "recommended_order_units", "drivers"]]
    st.dataframe(view, hide_index=True, width="stretch",
                 column_config={"risk_pct": st.column_config.ProgressColumn("Risk %", min_value=0, max_value=100, format="%d%%")})

    if items.empty:
        st.info("No items match the filters.")
    else:
        st.subheader("Review an item")
        opts = {r.sku_id: f"{r.sku_name} - {r.risk_tier} ({r.risk_score:.0%})" for r in items.itertuples()}
        sku = st.selectbox("Item", list(opts), format_func=opts.get)
        row = items[items["sku_id"] == sku].iloc[0]
        left, right = st.columns([3, 2])
        with left:
            st.markdown(f"**{row.sku_name}** from {row.supplier_name}. Risk of stockout in the next "
                        f"{int(row.horizon_days)} days: **{row.risk_score:.0%}** ({row.risk_tier}).")
            st.markdown(f"On hand {int(row.on_hand_units)}, on order {int(row.on_order_units)}, "
                        f"demand {row.avg_demand_28d:.1f}/day, {row.days_of_cover:.1f} days of cover"
                        + (", on promotion" if row.on_promo else "") + ".")
            st.markdown(f"Key drivers: {drivers_text(row.key_drivers)}")
            if isinstance(row.rationale, str) and row.rationale.strip():
                st.info(row.rationale)
            else:
                st.caption("No AI rationale (only generated for the 200 highest-risk in-stock items).")
            legacy = "would" if int(row.legacy_flag) == 1 else "would not"
            st.caption(f"The legacy min/max rule {legacy} reorder this item.")
        with right:
            rec = int(row.recommended_order_units)
            st.metric("Recommended order", f"{rec} units",
                      help=f"Case pack {int(row.case_pack)}, shelf capacity {int(row.shelf_capacity)}")
            if rec == 0:
                st.caption("No new units needed or the shelf is full. Consider expediting the open order.")
            with st.form(f"decide_{store_id}_{sku}", clear_on_submit=True):
                action = st.radio("Decision", ["APPROVE", "EDIT", "REJECT", "EXPEDITE"], horizontal=True)
                qty = st.number_input("Order units (used for EDIT)", min_value=0, step=max(int(row.case_pack), 1), value=rec)
                note = st.text_input("Note (optional)")
                if st.form_submit_button("Submit decision", type="primary"):
                    final_qty = {"APPROVE": rec, "EDIT": int(qty), "REJECT": 0, "EXPEDITE": 0}[action]
                    if action == "EDIT" and row.case_pack and final_qty % int(row.case_pack) != 0:
                        st.error(f"Quantity must be a multiple of the case pack ({int(row.case_pack)}).")
                    else:
                        new_id = save_decision(row, action, final_qty, note, user)
                        st.success(f"Saved decision #{new_id}: {action} {final_qty} units.")

with tab_hist:
    hist = query_df(f"""SELECT approval_id, decided_at, sku_id, sku_name, action, recommended_order_units,
                               approved_order_units, risk_tier, note, decided_by
                        FROM {APPROVALS} WHERE store_id = %s ORDER BY decided_at DESC LIMIT 200""", (store_id,))
    if hist.empty:
        st.info("No decisions yet for this store.")
    else:
        st.dataframe(hist, hide_index=True, width="stretch")
