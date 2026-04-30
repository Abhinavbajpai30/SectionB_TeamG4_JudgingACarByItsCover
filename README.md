# Judging a Car by its Cover
### A Data-Driven Pricing Intelligence System for the Used Vehicle Market

**DVA Capstone 2 | Group 4, Section B | Newton School of Technology**
**Faculty Mentor: Satyaki Das | Submitted: 29 April 2026**

---

Every day, dealers walk into wholesale auctions and place bids on instinct. They lose margin on overpriced vehicles, miss opportunities on undervalued ones, and repeat the cycle the next week. This project asks a simple question: what if the data could tell you exactly what a car is worth before you bid?

We took 548,486 real wholesale auction transactions from the Manheim dataset, ran them through a full Python ETL pipeline, and turned raw auction records into a structured pricing intelligence system — with formal statistical validation and three Tableau dashboards built for the auction floor.

---

## What We Found

The market is more predictable than dealers think. MMR (the Manheim Market Report benchmark) correlates with final selling price at r = 0.97 — it is essentially the ground truth of used car pricing. Every 10,000 miles shaves off around $430. Moving a vehicle up one condition tier adds $2,000 to $3,500. Summer prices run nearly 60% above winter lows. And almost half the market sells above MMR — which means information is an edge.

Five recommendations follow from these findings, with a combined estimated annual impact of $914,000 for a mid-size dealer group. The cost of not acting is roughly $76,000 per month, sitting quietly in the data.

---

## The Project at a Glance

| What | Detail |
|------|--------|
| Dataset | Manheim Used Vehicle Sales, 2014-2015 (Kaggle) |
| Clean dataset | 548,486 transactions, 29 columns |
| Dashboards | 3 Tableau dashboards (Executive, Operational, Market Intelligence) |
| Estimated annual impact | ~$914,000 per dealer group |

---

## Repository Structure

```
DVACapstone2/
+-- data/               Raw and processed datasets
+-- notebooks/          Five Jupyter notebooks (extraction through export)
+-- scripts/            Standalone ETL pipeline script
+-- docs/               Data dictionary (29 fields documented)
+-- tableau/            Dashboard links and screenshots
+-- reports/            Final PDF report, PPTX deck, and presentation scripts
+-- DVA-oriented-Portfolio/   Team portfolio links
+-- DVA-oriented-Resume/      Team resume links
```

---

## Tech Stack

Python, pandas, NumPy, SciPy, Matplotlib, Seaborn, Tableau Public, ReportLab, Git.

---

## Team

| Name | Role |
|------|------|
| Abhinav Bajpai | Project Lead |
| Kushagra Bhardwaj | ETL Lead |
| Rishiwant Kumar Maurya | Analysis Lead |
| Pranav Singh | Visualization Lead |
| Samriddhi Shah | Strategy Lead |
| Drishti Jha | PPT and Quality Lead |

**GitHub:** [SectionB_TeamG4_JudgingACarByItsCover](https://github.com/Abhinavbajpai30/SectionB_TeamG4_JudgingACarByItsCover)
