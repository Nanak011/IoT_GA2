
/* 
 * [A] Transformation > Script   name: "WQI + ingest age"
 * Server-side Water Quality Index, stored as derived keys
 * wqi / wqi_class.  ingest_age_ms = server receive time - acquisition ts,
 * which is the end-to-end latency for live rows and the buffering age
 * for replayed rows (metrics.py separates the two).
 * NOTE: one rule-engine message is created per timestamp, so batch-replayed
 * rows each get their own WQI computed at their ORIGINAL timestamp. */
var w  = {ph: 0.369, turbidity_ntu: 0.628, tds_ppm: 0.003};  // unit weights
var S  = {ph: 8.5,   turbidity_ntu: 5,     tds_ppm: 1000};   // NDWQS standard
var V0 = {ph: 7.0,   turbidity_ntu: 0,     tds_ppm: 0};      // ideal value

if (msg.ph !== undefined && msg.turbidity_ntu !== undefined && msg.tds_ppm !== undefined) {
    var num = 0, den = 0;
    for (var k in w) {
        var q = 100 * Math.abs(Number(msg[k]) - V0[k]) / (S[k] - V0[k]);   // sub-index
        num += w[k] * q;
        den += w[k];
    }
    var wqi = num / den;
    msg.wqi = Math.round(wqi * 10) / 10;
    msg.wqi_class = wqi <= 25 ? 'Excellent' : wqi <= 50 ? 'Good' :
                    wqi <= 75 ? 'Poor' : wqi <= 100 ? 'Very poor' : 'Unsuitable';
}
var acquired = parseInt(metadata.ts);
if (!isNaN(acquired)) {
    msg.ingest_age_ms = Date.now() - acquired;
}
return {msg: msg, metadata: metadata, msgType: msgType};


/*
 * [B] Enrichment > Originator attributes
 *     Server attributes: active_alarms   (adds metadata.ss_active_alarms)
 *
 * [C] Transformation > Script   name: "alarm state"
 * Keeps a per-device map {alarmType: severity} of ACTIVE alarms and the
 * highest severity, saved as server attributes.  The map widget colours
 * each node marker from alarm_severity. */
var active = {};
try { active = JSON.parse(metadata.ss_active_alarms || '{}'); } catch (e) { active = {}; }

var cleared = metadata.isClearedAlarm === 'true' ||
              (msg.status && String(msg.status).indexOf('CLEARED') === 0);
if (cleared) { delete active[msg.type]; } else { active[msg.type] = msg.severity; }

var rank = {CRITICAL: 4, MAJOR: 3, MINOR: 2, WARNING: 1, INDETERMINATE: 0};
var top = 'NONE', best = -1;
for (var t in active) {
    if (rank[active[t]] > best) { best = rank[active[t]]; top = active[t]; }
}
return {
    msg: {active_alarms: JSON.stringify(active), alarm_severity: top,
          alarm_count: Object.keys(active).length},
    metadata: metadata,
    msgType: 'POST_ATTRIBUTES_REQUEST'
};

