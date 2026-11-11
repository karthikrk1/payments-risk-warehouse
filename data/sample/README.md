# PaySim sample (for CI)

`paysim_sample.csv` is a 4,904-row extract of the PaySim dataset, used by CI (`make ci-build`) to run
the full pipeline without downloading the 470 MB original.

## Source and licence

- **Dataset:** *Synthetic Financial Datasets For Fraud Detection* (PaySim), by E. A. Lopez-Rojas
  (Kaggle user `ealaxi`): <https://www.kaggle.com/datasets/ealaxi/paysim1>
- **Licence:** [CC BY-SA 4.0](https://creativecommons.org/licenses/by-sa/4.0/). This extract is
  shared under the same licence.
- **Citation requested by the author:** E. A. Lopez-Rojas, A. Elmir and S. Axelsson, "PaySim: A
  financial mobile money simulator for fraud detection", 28th European Modeling and Simulation
  Symposium (EMSS), Larnaca, Cyprus, 2016.

## Changes from the original

Rows are a subset, selected deterministically by `ingestion/make_sample.py` (`make sample`) and
copied verbatim, in their original order. No values were changed. The selection keeps:
every row flagged by PaySim's legacy rule, every zero-amount row, about 7% of fraudulent rows, every
transaction of about 0.03% of receiving accounts (so receivers repeat), and about 0.05% of the rest.

| Type | Rows | Fraud | Legacy-flagged |
|---|---:|---:|---:|
| CASH_IN | 1,065 | 0 | 0 |
| CASH_OUT | 2,007 | 279 | 0 |
| DEBIT | 21 | 0 | 0 |
| PAYMENT | 1,095 | 0 | 0 |
| TRANSFER | 716 | 311 | 16 |

Fraud is heavily over-represented (12% vs 0.13% in the full data) so tests and features have cases
to work on. **Don't draw conclusions from rates in this file;** results come from the full dataset.
