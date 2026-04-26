alter table landintel.site_signals
    drop constraint if exists site_signals_confidence_check;

alter table landintel.site_signals
    add constraint site_signals_confidence_check
    check (
        confidence is null
        or confidence between 0 and 1
        or confidence in (2, 3, 4, 5)
    );

comment on constraint site_signals_confidence_check on landintel.site_signals
    is 'Allows Phase One normalized confidence scores from live source expansion while preserving the bounded legacy ordinal confidence domain.';
