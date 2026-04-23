# Scottish MVP Gap Analysis

## Where The MVP Is Already Strong

### 1. Canonical-site-first architecture

The product now has the right truth model.

- `public.sites` stays central
- datasets remain modular
- linked evidence is stored around the site rather than flattened into one master table

This is the foundation that most land tools get wrong.

### 2. Signals before opinions

The stack now runs in the right order:

- linked source rows
- normalised evidence
- atomic signals
- seven scores
- hard-fail gates
- bucket routing
- analyst-facing interpretations

That is a genuine reasoning architecture, not a dressed-up filter.

### 3. Evidence-first traceability

Signals, score contributions, interpretations, and site assessments all point back to evidence rows.

That is the trust layer senior land people will care about.

### 4. Portfolio routing instead of naive scoring

The six-bucket portfolio logic is the most commercially differentiated part of the MVP so far.

It forces the engine to answer:

- what kind of opportunity this is
- what is really stopping it
- whether the blocker is fixable, bounded, or fatal

That is much closer to how a strategic land team actually thinks.

### 5. The new bridge layer

The reference reconciliation layer is the other genuinely differentiated part.

It is the glue that lets HLA, LDP, planning, VDL, titles, and future schedule codes roll into one site without pretending Scotland uses one perfect join key.

## The Truly Revolutionary Parts

If this product becomes elite, it will be because of these parts:

### Canonical-site reconciliation

Most land systems stop at map layers.
This one can become a site identity engine.

That matters because commercial intelligence only compounds when the same site is recognised across:

- policy
- planning
- brownfield evidence
- technical history
- control clues
- buyer logic

### Portfolio bucket routing

The six-bucket model is not cosmetic.
It is the beginning of portfolio construction logic:

- long-term strategic
- medium-term emerging
- short-term re-entry
- messy but priceable
- infrastructure-frozen
- dead

That is much more investable than a red-amber-green map.

### BGS as reasoning, not overlay

Treating BGS mostly as:

- prior progression
- investigation intensity
- hydrogeology caution
- extraction legacy

is commercially sharper than treating boreholes and logs as generic “constraints”.

## Biggest Current Gaps

### 1. Real source adapters are still thin

The architecture is now ahead of the live ingest layer.

The next real leap is to replace more seeded rows with:

- planning adapters
- HLA adapters
- VDL adapters
- flood adapters
- valuation / market adapters

### 2. Reconciliation still needs live external row workflows

The bridge schema and matching logic exist, but the MVP still needs production ingestion jobs that:

- ingest uncoupled external rows
- compute geometry overlap candidates
- run documentary matching
- queue unresolved items for human review

### 3. Authority nuance is not yet deep enough

Scottish planning logic is not nationally uniform in practice.

The current ruleset is Scotland-first, but not yet council-smart enough for:

- Edinburgh edge cases
- West Lothian style housing-land interpretation
- Aberdeen / Aberdeenshire infrastructure nuance
- Fife brownfield and delivery nuance

### 4. Control logic is still early

Ownership fragmentation and legal blockers are present, but the control basis is not yet rich enough.

It still needs:

- title merge / split awareness
- ransom-strip style logic
- public ownership process nuance
- option / promotion suitability logic

### 5. Market logic is still too lightweight

Buyer depth exists, but it is still mostly a seeded profile layer plus comparables.

It needs stronger downstream commercial intelligence around:

- local delivery pattern
- PLC vs regional vs affordable fit
- settlement product fit
- value resilience versus abnormal costs

## What Needs Work Next

### Highest leverage engineering work

1. Real HLA + LDP + VDL ingestion with alias extraction
2. Geometry overlap candidate generation for incoming rows
3. Planning refusal theme extraction from real decision text
4. Manual reconciliation inbox for unresolved site identity cases
5. Manual override workflow for bucket and blocker

### Highest leverage reasoning work

1. Better fixability sub-rules by blocker type
2. Better buyer-specific routing
3. Better cost-to-control sub-scoring
4. Better infrastructure burden inference from plan-cycle evidence
5. Better short / medium / long horizon nuance inside each bucket

## What I Would Not Change

- Keep deterministic rules for the MVP
- Keep the six buckets
- Keep the seven scores
- Keep `public.sites` as the canonical site object
- Keep raw datasets out of Supabase where they do not need operational querying

## The Main Risk To Avoid

The main failure mode from here is not “too little AI”.

It is:

- drifting back into generic filter logic
- hiding reasoning in opaque heuristics
- letting source identity become messy
- skipping the hard work of site reconciliation

If we protect against that, this MVP can evolve into something genuinely unusual.
