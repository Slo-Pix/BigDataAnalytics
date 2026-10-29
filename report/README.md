# ExposureIQ project report

- [PDF report](ExposureIQ_Report.pdf)
- [Editable LaTeX source](ExposureIQ_Report.tex)
- [Supplied presentation](../showcase/Big%20Data%20Analytics.pdf)
- [Operator guide](../docs/EXPOSUREIQ_PILOT.md)

The report presents **ExposureIQ: Content Exposure Intelligence**, retaining *Distributed Analytics
of Global Torrent Activity* as the academic research foundation. It follows the supplied presentation's
Q1-Q6 ordering, includes the A7 streaming study, and distinguishes implemented company workflows
from historical findings and future deployment requirements.

## Rebuild

From the repository root, use the project's installed Python dependencies plus TeX Live
(`pdflatex`, `latexmk`, `geometry`, `booktabs`, `tabularx`, `hyperref`, `xurl`, `fancyhdr`, `lmodern`):

```bash
uv run python -m analysis.render_saved
latexmk -pdf -interaction=nonstopmode -halt-on-error -cd report/ExposureIQ_Report.tex
```

The chart renderer reads only tracked `analysis/results/*.csv` and reuses the project plotting
style. It does not start Spark/Docker, scrape trackers, read company data, change result CSVs,
or rerun the original research. The PDF embeds regenerated charts under `figures/`; these also
serve the research console. Intermediate TeX files are ignored by Git.

After editing, check for LaTeX errors, unresolved references and overfull boxes in the build output.
The stable portal route `/report.pdf` serves the new ExposureIQ PDF.

## Evidence and interpretation

Numbers come from saved CSVs and the supplied October 2026 presentation. Research outputs describe
the recorded collection, not a newly reproduced experiment. The presentation's approximate pageview
volume is resolved to the archived valid-row count of 1,153,632,847. The validation discussion preserves
the JSONL shortfall and timestamp-boundary caveat rather than turning 26 PASS labels into a claim
that every backup and timestamp check was exact.

The report does not imply enforcement capability, real-time discovery of unknown releases,
copyright ownership verification, unique audience measurement or production readiness.
Compatibility names such as `CWATCH_*` and `cwatch` remain intentionally unchanged.
