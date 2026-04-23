# Scottish Reasoning Future Notes

This note captures the next extension points without complicating the current MVP.

## Weighted Sub-Scores

The current engine uses explicit additive contributions into the seven core scores.

Later we can introduce sub-scores such as:

- planning principle vs planning timing
- clean ground vs bounded abnormal cost
- on-site infrastructure vs off-site infrastructure burden
- prior DD depth vs prior consent depth
- control simplicity vs carry profile
- buyer breadth vs buyer quality

Those should stay transparent and inspectable.

## Buyer-Specific Routing

The MVP stores buyer-profile matches and a single buyer guess.

Later we can support:

- PLC routing
- regional-only routing
- affordable / partnership routing
- specialist brownfield routing
- retirement / age-restricted routing

That logic should sit on top of the same site evidence and scorecard, not replace it.

## Council-Specific Nuance

Scottish planning judgment varies materially by authority.

Future extensions should allow:

- authority-specific settlement logic
- authority-specific HLA interpretation nuance
- local contribution friction patterns
- known infrastructure bottleneck heuristics

This should be implemented as authority or region overlays, not hardcoded throughout the core engine.

## England Support

England should be added later as a separate jurisdictional layer.

That means:

- separate evidence normalisers where planning vocabulary differs
- separate score-contribution rules where policy logic differs
- separate authority nuance overlays
- shared canonical site and evidence architecture

Do not blend Scottish and English planning logic in the same first-pass ruleset.

