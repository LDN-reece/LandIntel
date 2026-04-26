alter table landintel.site_signals
    drop constraint if exists site_signals_confidence_check;

alter table landintel.site_signals
    add constraint site_signals_confidence_check
    check (
        confidence is null
        or (
            confidence::text ~ '^\s*([0-9]+(\.[0-9]+)?)\s*$'
            and confidence::text::numeric between 0 and 1
        )
        or btrim(confidence::text) in ('2', '3', '4', '5')
        or lower(btrim(confidence::text)) in ('low', 'medium', 'high')
    );

comment on constraint site_signals_confidence_check on landintel.site_signals
    is 'Type-safe Phase One confidence domain. Allows normalized 0..1 values for source expansion while preserving bounded legacy ordinal/text confidence values.';
