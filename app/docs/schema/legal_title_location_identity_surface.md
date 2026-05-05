# Legal Title And Location Identity Surface

## Purpose

`landintel_sourced.v_site_legal_title_location_identity` is the clean first-read surface for a sourced site.

It answers only:

- legal title number, if LandIntel already holds one;
- address, if LandIntel already holds one;
- local area / settlement name, if held;
- local authority;
- local council.

It deliberately avoids title workflow language. It does not infer ownership. It does not treat SCT or RoS parcel references as title numbers.

## What It Reads

The view reads existing objects only:

- `landintel.canonical_sites`
- `landintel.site_ldn_candidate_screen`
- `landintel.site_urgent_address_title_pack`
- `landintel.site_urgent_address_candidates`
- `public.site_title_validation`
- `public.site_title_resolution_candidates`
- `public.site_ros_parcel_link_candidates`
- `public.ros_cadastral_parcels`

It does not create a physical table and does not move data.

## Title Number Rule

The view only exposes values passing `public.is_scottish_title_number_candidate`.

That means:

- SCT references are excluded;
- rejected title rows are excluded;
- RoS parcel references are not title numbers;
- title workflow status is not used as ownership truth.

If no valid title-number-shaped value is held, the view returns:

`LEGAL TITLE NUMBER NOT HELD`

## External Focus Areas

Sites marked as external focus-area records are filtered from this view. They should not pollute the core LDN sourced-site identity surface.

## Caveat

A legal title number being present is not ownership confirmation.

Ownership remains a separate human/legal interpretation. This view is only the clean identity layer needed before deeper DD and sourcing review.
