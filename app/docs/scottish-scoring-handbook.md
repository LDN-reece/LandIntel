# Scottish Scoring Handbook

This handbook defines the first commercial scoring framework for the Scottish land reasoning MVP.

The scores do not claim certainty. They force consistency, make the routing explainable, and help the engine place a site into the right portfolio bucket for senior review.

## The Seven Scores

### `P` Planning Strength

Question: how credible is the planning route?

- `1`: no realistic policy route or repeated principle failure
- `2`: weak or highly speculative
- `3`: plausible with work, timing, or promotion
- `4`: strong support or positive planning context
- `5`: very strong adopted / effective basis

### `G` Ground / Constraints

Question: how clean or workable is the physical site?

- `1`: severe or open-ended technical risk
- `2`: meaningful technical issues
- `3`: workable but materially messy
- `4`: only light technical friction
- `5`: clean or low-constraint

### `I` Infrastructure

Question: how easy is access, servicing, drainage, roads, schools, and utilities?

- `1`: major infrastructure blocker
- `2`: heavy burden
- `3`: moderate friction
- `4`: light friction
- `5`: broadly straightforward

### `R` Prior Progress

Question: how much real work has already been done?

- `1`: little or none
- `2`: limited evidence of progression
- `3`: some meaningful work
- `4`: serious prior progression
- `5`: advanced prior progression

### `F` Fixability

Question: if there is a problem, does it look fixable?

- `1`: fatal or likely non-fixable
- `2`: doubtful / unclear
- `3`: maybe fixable
- `4`: likely fixable
- `5`: highly fixable or mostly a timing / commercial issue

### `K` Cost to Control

Question: how much cost, risk, and carry is needed before value inflection?

- `1`: very expensive / heavy carry
- `2`: expensive
- `3`: medium
- `4`: relatively efficient
- `5`: low cost / efficient

### `B` Buyer Depth

Question: how many credible buyers are likely to want the site later?

- `1`: almost no credible buyer depth
- `2`: narrow buyer pool
- `3`: workable buyer pool
- `4`: strong buyer demand
- `5`: broad buyer demand

## Confidence

Each score also carries a confidence label:

- `high`: multiple direct source families align
- `medium`: one strong source or partial corroboration exists
- `low`: data is sparse, conflicting, or largely inferred

If confidence is low for `P`, `G`, `I`, or `F`, the site is flagged for human review.

## Hard Fail Gates

Before routing, the engine checks four deterministic gates:

1. `planning_fatality`
2. `technical_fatality`
3. `exit_fatality`
4. `control_fatality`

If any gate triggers, the default route is `Bucket F` unless a human later overrides it.

## Bucket Routing

The MVP routes in this order:

1. Hard fail -> `F`
2. `R >= 4` and `F >= 3` -> `C`
3. `G >= 4` and `I >= 4` and `R <= 2` -> `A`
4. `I <= 2` and `P >= 3` -> `E`
5. `G <= 3` and `F >= 3` -> `D`
6. `P >= 3` -> `B`
7. Else -> `F`

## Human Review Triggers

The MVP sets `human_review_required = true` when:

- any hard fail gate triggers
- confidence is low for `P`, `G`, `I`, or `F`
- the blocker is not well-bounded
- the site still needs a decisive manual check on planning, ground, infrastructure, or control

## What This Is Not

- not a residual valuation
- not a black-box AI score
- not a replacement for a land director
- not an instruction to buy or reject land without human judgment

