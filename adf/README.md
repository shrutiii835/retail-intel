# Azure Data Factory — definitions and deployment

## What ADF does here, and what it deliberately does not

**ADF moves data and orchestrates. Databricks transforms.**

Keeping that boundary means every piece of business logic is Python that can be
unit-tested, code-reviewed and run on a laptop. The moment cleaning logic moves
into a Data Flow it becomes untestable GUI configuration.

| Activity | Type | Does |
|---|---|---|
| `SetSourceList` | SetVariable | Orders the five feeds — masters before transactions, so Silver's referential rules always have something to validate against |
| `CopyEachSourceToRaw` | ForEach → Copy | Lands each feed into `lakehouse/raw/<batch>/<source>/` |
| `BronzeIngest` | DatabricksNotebook | Watermark predicate → Bronze Delta |
| `SilverBuild` | DatabricksNotebook | Validate → quarantine → dedupe → MERGE |
| `GoldBuild` | DatabricksNotebook | Star schema → ROI mart → OPTIMIZE |

Each notebook activity depends on `Succeeded` of the previous one, so a failure
stops the chain rather than building Gold on top of a broken Silver.

---

## Where the watermark lives — and why not in ADF

The canonical ADF incremental pattern is *Lookup old watermark → Copy with a
`WHERE last_modified > @{watermark}` query → Stored procedure to update it*.
That pattern assumes a **queryable source** (SQL Server, Azure SQL). It is
included below for reference.

Here the source is **CSV files**, and a Copy activity cannot filter on file
*contents*. So the watermark predicate is applied in Spark, and watermark state
lives in the Delta control table `_control/watermarks`.

This is the better design for this pipeline regardless of source type, for one
concrete reason: the watermark can be advanced **in the same logical unit of
work as the data write**. If Silver's MERGE succeeds but an ADF "update
watermark" activity then fails, ADF's version of the watermark is now wrong and
the next run silently reprocesses or skips. Keeping the watermark next to the
data, advanced only after the write commits, removes that failure mode.

For a SQL source, the ADF-side pattern would be:

```json
{
  "name": "LookupWatermark",
  "type": "Lookup",
  "typeProperties": {
    "source": {
      "type": "AzureSqlSource",
      "sqlReaderQuery": "SELECT watermark_value FROM control.watermarks WHERE table_name = 'sales'"
    }
  }
},
{
  "name": "CopyIncremental",
  "type": "Copy",
  "typeProperties": {
    "source": {
      "type": "AzureSqlSource",
      "sqlReaderQuery": {
        "value": "SELECT * FROM dbo.sales WHERE last_modified_timestamp > '@{activity('LookupWatermark').output.firstRow.watermark_value}'",
        "type": "Expression"
      }
    }
  }
}
```

---

## Parameterisation

One dataset definition serves all five feeds across all batches, rather than 15
near-identical datasets:

```
ds_landing_csv(batch_id, source_name)  → landing/<batch_id>/<source_name>/*.csv
ds_raw_csv(batch_id, source_name)      → lakehouse/raw/<batch_id>/<source_name>/
```

Pipeline parameters: `batch_id` (which batch), `full_reload` (ignore watermarks).

---

## Security

- **No secrets in any of these files.** Both linked services authenticate with
  the Data Factory **system-assigned managed identity** — no account keys, no
  Databricks personal access token, nothing to rotate or leak.
- The ADF identity needs:
  - `Storage Blob Data Contributor` on the storage account
  - `Contributor` on the Databricks workspace (to submit jobs)
- `ls_databricks` uses a **job cluster**, not an interactive one: the cluster is
  created for the run and terminated when it finishes, so nothing is left
  billing afterwards.

---

## Cost

- **Single-node** job cluster (`Standard_DS3_v2`, 0 workers) — smallest practical.
- **No triggers are deployed.** Pipelines are run manually; a schedule would burn
  activity runs for a project that only needs to be demonstrated.
- Copy activity is a handful of small CSVs — well inside the free tier.

---

## Deployment

The JSON here is the ADF resource format, deployable either by importing in the
Studio UI or via `az datafactory`. Parameter values (storage account name,
workspace URL and resource id) are supplied at deployment time — that is why
they are parameters rather than hardcoded.

```bash
RG=rg-retailintel
DF=adf-retailintel

az datafactory linked-service create --resource-group $RG --factory-name $DF \
  --linked-service-name ls_adls_gen2 --properties @adf/linkedService/ls_adls_gen2.json

az datafactory linked-service create --resource-group $RG --factory-name $DF \
  --linked-service-name ls_databricks --properties @adf/linkedService/ls_databricks.json

az datafactory dataset create --resource-group $RG --factory-name $DF \
  --dataset-name ds_landing_csv --properties @adf/dataset/ds_landing_csv.json

az datafactory dataset create --resource-group $RG --factory-name $DF \
  --dataset-name ds_raw_csv --properties @adf/dataset/ds_raw_csv.json

az datafactory pipeline create --resource-group $RG --factory-name $DF \
  --name pl_retailintel_medallion --pipeline @adf/pipeline/pl_retailintel_medallion.json
```

Run it:

```bash
az datafactory pipeline create-run --resource-group $RG --factory-name $DF \
  --name pl_retailintel_medallion \
  --parameters '{"batch_id":"batch_01_initial","full_reload":true}'
```

> The notebook paths in the pipeline (`/Repos/RetailIntel/notebooks/...`) assume
> the repository is available in the Databricks workspace under
> `/Workspace/Repos/RetailIntel`, which is also the path the notebooks add to
> `sys.path` so they can import `src/`.
