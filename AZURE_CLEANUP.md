# AZURE CLEANUP

Everything created for RetailIntel, and the exact order to remove it.

**The single most important thing:** Databricks compute is the only resource
here that costs meaningful money, and it costs it *per minute while running*.
Storage, an idle Data Factory and an idle Databricks workspace cost effectively
nothing. If you do one thing, terminate the cluster.

---

## Resources created

| Resource | Name | Type | Costs money when idle? |
|---|---|---|---|
| Resource Group | `rg-retailintel` | Container | No |
| Storage (ADLS Gen2) | `stretailintel26c66f` | Standard LRS, HNS on | Pennies (~200 MB) |
| Data Factory | `adf-retailintel` | v2, no triggers | No |
| Databricks workspace | `dbw-retailintel` | trial SKU | No — **but its clusters do** |
| Service principal | `sp-retailintel-databricks` | App registration | No |
| Role assignments | 4 | RBAC | No |

**Subscription:** `Azure subscription 1` (Free Trial, spending limit **ON**).
The spending limit means you cannot be billed beyond your credit — resources
are disabled instead. That is your backstop, not a substitute for cleanup.

---

## Pre-cleanup checklist

Do **not** delete anything until every box is ticked. Deleting the resource
group is irreversible and takes the Gold tables with it.

- [ ] Screenshots captured and saved locally (see `AZURE_SCREENSHOTS.md`)
- [ ] Source data saved — `data/raw/source/` and `data/landing/` (regenerable
      from `src/data_generation/generate_data.py` with the fixed seed)
- [ ] Generated Delta lakehouse present locally — `lakehouse/`
- [ ] Notebooks saved — `notebooks/` (in the repo, not only in Databricks)
- [ ] Code saved — `src/`, `config/`, `tests/`, `scripts/`, `adf/`
- [ ] Power BI exports saved — `powerbi/data/*.csv`
- [ ] `RetailIntel.pbix` built and saved
- [ ] Benchmark results saved — `metrics/query_benchmark.json`
- [ ] Metrics saved — `metrics/*.json`
- [ ] Documentation complete — all 10 markdown files
- [ ] Azure run evidence recorded in `CONTEXT.md` (row counts, run ids)

Everything in the lakehouse can be rebuilt locally from the seeded generator, so
the irreplaceable artefacts are the **screenshots** — they are the only proof
this ran on Azure.

---

## Step 1 — Terminate compute (do this first, always)

This is the one that stops the money.

```bash
# List every cluster and its state
az extension add --name databricks 2>/dev/null
TOKEN=$(az account get-access-token --resource 2ff814a6-3304-4ab8-85cb-cd0e6f879c1d --query accessToken -o tsv)
URL=https://adb-7405607439445982.2.azuredatabricks.net

curl -s -X GET "$URL/api/2.1/clusters/list" -H "Authorization: Bearer $TOKEN" \
  | python3 -c "import sys,json;[print(c['cluster_id'], c['state'], c.get('cluster_name','')) for c in json.load(sys.stdin).get('clusters',[])]"
```

Terminate anything not already `TERMINATED`:

```bash
curl -s -X POST "$URL/api/2.1/clusters/delete" -H "Authorization: Bearer $TOKEN" \
  -d '{"cluster_id":"<cluster_id>"}'
```

> The pipeline uses **job clusters**, which terminate themselves when the run
> ends. This step is a safety check for any cluster started manually while
> exploring the workspace — which is easy to do and easy to forget.

**Verify:** every cluster reports `TERMINATED`.

---

## Step 2 — Confirm no ADF triggers are active

None were deployed, but confirm rather than assume — a live trigger is the
classic way to wake up to a bill.

```bash
az datafactory trigger list --resource-group rg-retailintel \
  --factory-name adf-retailintel -o table
```

**Expected:** empty. If anything is listed:

```bash
az datafactory trigger stop --resource-group rg-retailintel \
  --factory-name adf-retailintel --name <trigger-name>
az datafactory trigger delete --resource-group rg-retailintel \
  --factory-name adf-retailintel --name <trigger-name> --yes
```

---

## Step 3 — Decide: pause or delete

### Option A — Keep it for interviews, minimise cost

Leaves everything demonstrable and screenshot-able. With compute terminated and
no triggers, ongoing cost is **storage only** — roughly ₹5–20/month for ~200 MB.

Nothing more to do. Re-check clusters occasionally with Step 1.

### Option B — Delete everything

Only once screenshots are captured and the checklist above is fully ticked.

**This is irreversible.** It deletes the storage account, the lakehouse, the
Data Factory and the Databricks workspace.

```bash
az group delete --name rg-retailintel --yes --no-wait
```

Deleting a Databricks workspace also removes its **managed resource group**
(`databricks-rg-dbw-retailintel-*`) automatically. Verify:

```bash
az group list --query "[?starts_with(name,'rg-retailintel')||starts_with(name,'databricks-rg')].name" -o tsv
```

**Expected:** empty output once deletion completes (a few minutes).

---

## Step 4 — Remove the service principal

Not covered by the resource-group delete — app registrations live in Entra ID,
not in a resource group. Left behind, it is an orphaned identity with a
credential.

```bash
az ad sp delete --id $(az ad sp list --display-name sp-retailintel-databricks --query "[0].appId" -o tsv)
az ad app delete --id $(az ad app list --display-name sp-retailintel-databricks --query "[0].appId" -o tsv)
```

**Verify:**

```bash
az ad sp list --display-name sp-retailintel-databricks -o tsv
```

**Expected:** empty.

---

## Step 5 — Verify nothing is left

```bash
az resource list --query "[?resourceGroup=='rg-retailintel'].{name:name,type:type}" -o table
az group list -o table
```

---

## Step 6 — Check cost

```bash
az consumption usage list --start-date 2026-08-13 --end-date 2026-08-20 \
  --query "[].{name:instanceName, cost:pretaxCost, currency:currency}" -o table
```

Or in the Portal: **Cost Management + Billing → Cost analysis**, scoped to the
subscription.

> Usage data lags by **8–24 hours**, so a zero immediately after teardown means
> "not reported yet", not "free". Check again the next day.

**Expected total:** a few hundred rupees at most, dominated by the Databricks
job-cluster minutes. Everything else is rounding error.

---

## What survives cleanup

Deleting the Azure resources costs you nothing that matters, because the project
is reproducible end to end:

| Artefact | Where it lives | Rebuildable? |
|---|---|---|
| All source code | `src/`, `config/`, `scripts/` | — |
| Synthetic data | `data/` | **Yes** — seeded generator, byte-identical |
| Delta lakehouse | `lakehouse/` | **Yes** — `python -m src.pipeline --batch all --full-reload` |
| Measured metrics | `metrics/*.json` | **Yes** — rerun the quality and benchmark scripts |
| Power BI data | `powerbi/data/*.csv` | **Yes** — `python -m src.gold.export_powerbi` |
| Documentation | 10 markdown files | — |
| **Azure screenshots** | wherever you saved them | **NO — capture before deleting** |

The screenshots are the only irreplaceable artefact. Everything else regenerates
from a seed.
