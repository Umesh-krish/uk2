-- Consumption cannot exceed what the connection is physically rated to draw
-- over the window. A breach means the register was misread, the window
-- boundaries are wrong, or duplicates slipped past the deduplication.
--
-- A 25% margin is allowed: sanctioned load is a contractual limit, not a hard
-- physical cap, and brief overdraw is normal.

select
    meter_id,
    window_start_utc,
    consumption_kwh,
    sanctioned_load_kw,
    reading_count,
    duplicates_dropped

from {{ ref('fct_meter_consumption') }}
where consumption_kwh is not null
  and sanctioned_load_kw is not null
  and consumption_kwh >
      sanctioned_load_kw
      * (date_diff('minute', window_start_utc, window_end_utc) / 60.0)
      * 1.25
