# Screenshots: ThingsBoard Cloud evidence (14 Nov 2026)

Taken on ThingsBoard Cloud (EU, v4.4) during the demo run
(logs/run_20261114_095046.log) and the measurement run
(logs/run_20261114_104147.log).

| File | What it shows |
|---|---|
| dash_fleet_normal.png | Fleet view in normal operation: all markers green, no active alarms |
| dash_event.png | BDR-01 intrusion: marker red, High Turbidity (Warning) then Gross Contamination (Critical) | 
| dash_node_detail.png | BDR-01 detail page: turbidity gauge in the red band, turbidity/pH/TDS changing together |
| alarms_table.png | Alarms table with active and cleared alarms, severities and clear times |
| tb_backfill_before.png | BDR-01 chart during the 150 s outage: lines stop (gap) |
| tb_backfill_after.png | Same chart after replay: gap filled at the original timestamps |
| tb_alarm_rules.png | The six alarm rules (Alarm Rules 2.0) on the device profile |
| tb_rule_chain.png | Root rule chain with the WQI + ingest-age script node |
| SQLite/sqlite_schema.png | SQLite tables: readings (store-and-forward buffer), buffer_log (journal), meta |
| SQLite/sqlite_buffer_log.png | Journal of the 180 s outage: NET_LOST, rows buffered, NET_RESTORED, FLUSH batches, DRAINED |
| SQLite/sqlite_replayed_rows.png | Readings taken during the outage, sent ~3 min later with original timestamps |
| SQLitesqlite_readings_normal.png | Normal readings sent within ~1 s of acquisition |
| SQLite/sqlite_summary.png | All rows sent, 0 pending, 0 DATA_LOSS entries |