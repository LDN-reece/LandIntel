# Scottish Source Stack And Reference Bridge

## Purpose

This document defines the Scotland-only source stack and the canonical-site bridge that sits underneath the reasoning engine.

The core rule is simple:

- `public.sites` is the canonical internal site object
- no external dataset defines the site on its own
- every external code, geometry, schedule row, planning ref, or title clue must reconcile back to that object

## Why The Bridge Exists

Scottish datasets rarely share one stable key.

The same site may appear as:

- an HLA site ref
- an LDP allocation code
- a VDL audit code
- a planning reference in a PDF schedule
- one or more title numbers
- geometry-only overlap with no code at all

The bridge layer stops the product from becoming a pile of near-duplicate sites.

## Core Canonical Tables

- `public.sites`
- `public.site_reference_aliases`
- `public.site_geometry_versions`
- `public.site_reconciliation_matches`
- `public.site_reconciliation_review_queue`
- `analytics.v_canonical_sites`
- `analytics.v_site_reference_index`

## Matching Order

The reconciliation engine follows this order:

1. Direct site reference match
2. Alias table match
3. Planning reference match
4. Spatial overlap / proximity match
5. Fuzzy documentary match
6. Human review queue

Weak matches are not forced.

## What Gets Stored

`public.site_reference_aliases` preserves:

- raw reference value
- normalised reference value
- source dataset
- authority
- plan period
- site name hint
- geometry hash where useful
- source record id
- relation type
- status
- confidence
- notes

`public.site_geometry_versions` preserves:

- current or historic canonical geometry hash
- version label
- source dataset and table
- source record id
- relation type
- geometry and centroid

`public.site_reconciliation_matches` preserves:

- each attempted bridge event
- the matching path used
- the confidence
- the raw reference or site name used

`public.site_reconciliation_review_queue` preserves:

- low-confidence cases that should not be auto-forced
- failure reasons
- candidate matches for human review

## Current MVP Source Stack

### Mandatory core

- RoS cadastral parcels
- ScotLIS title workflow
- OS linked identifiers path
- OS Open Roads
- local authority boundaries
- planning applications
- local development plans
- settlement boundaries
- green belt
- HLA
- ELA
- building standards
- vacant and derelict land
- council asset registers
- SEPA flood maps
- BGS GeoIndex core layers
- contaminated land
- soils / peat / capability layers
- NatureScot / SiteLink / local nature designations
- ancient woodland
- HES designation layers
- core paths
- broadband proxy
- RoS open pricing statistics
- EPC linkage path
- Scottish Assessors valuation roll

### Mandatory reasoning modules

- site reference reconciliation engine
- boundary position engine
- previous-use inference engine
- current-building-use classification engine
- utility burden inference engine
- BGS reasoning engine
- confidence engine
- explanation engine
- source trace engine

## Boundary Engine Outputs

The boundary engine classifies:

- `fully_inside`
- `mostly_inside`
- `edge_straddling`
- `just_outside`
- `near_outside`

for:

- council boundary
- settlement boundary
- green belt

## Use Classification Outputs

Previous site use:

- virgin greenfield
- agricultural field
- farm steading / farmyard
- former residential
- existing residential
- commercial
- industrial
- storage / yard
- depot / transport
- office / civic
- education / community
- utilities / infrastructure
- vacant cleared site
- derelict / brownfield unknown
- mineral / extraction related
- mixed / unclear

Current building use:

- residential
- commercial retail
- office
- industrial
- workshop / factory / store
- agricultural building
- civic / public
- education
- healthcare
- utilities / infrastructure
- mixed-use
- vacant / disused
- unknown

## BGS Engine Outputs

The BGS layer is treated as reasoning, not decoration.

It currently produces:

- investigation intensity
- prior progression signal strength
- ground complexity signal
- hydrogeology caution
- extraction legacy caution

## What Still Needs More Depth

- true external row ingestion into the reconciliation engine, not just seeded site-linked rows
- geometry overlap candidate generation against incoming datasets
- PDF and schedule extraction for call-for-ideas and evidence-report appendices
- stronger UPRN / USRN / TOID enrichment once those feeds are wired
- authority-specific alias dictionaries for Scottish council quirks

## Non-Negotiable Principle

The bridge layer exists so the reasoning engine can say:

- what this site is
- what evidence is linked to it
- how the evidence was reconciled
- where the remaining identity uncertainty still sits

If that chain breaks, the reasoning engine stops being commercially trustworthy.
