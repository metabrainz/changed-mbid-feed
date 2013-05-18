#!/usr/bin/env python

import sys

# TODO: add AR type changes

queries = {
  'artist':
  [
    """
    SELECT gid
      FROM %schema%.artist
      WHERE last_updated >= %s
        AND last_updated < %s
    """
    ,
    """
    SELECT gid
      FROM %schema%.artist_alias aa
      JOIN %schema%.artist a ON aa.artist = a.id
      WHERE aa.last_updated >= %s
        AND aa.last_updated < %s
    """
    ,
    """
    SELECT gid
      FROM %schema%.artist_annotation aa
      JOIN %schema%.artist a ON aa.artist = a.id
      JOIN %schema%.annotation an ON aa.annotation = an.id
      WHERE an.created >= %s
        AND an.created < %s
    """
    ,
    """
    SELECT gid
      FROM %schema%.artist_ipi ai
      JOIN %schema%.artist a ON ai.artist = a.id
      WHERE ai.created >= %s
        AND ai.created < %s
    """
    ,
    """
    SELECT gid
      FROM %schema%.artist_tag AT
      JOIN %schema%.artist a ON AT.artist = a.id
      WHERE AT.last_updated >= %s
        AND AT.last_updated < %s
    """
    ,
    """
    SELECT gid
      FROM %schema%.artist_credit ac
      JOIN %schema%.artist_credit_name acn ON acn.artist_credit = ac.id
      JOIN %schema%.artist a ON acn.artist = a.id
      WHERE ac.created >= %s
        AND ac.created < %s
    """
    ,
    """
    SELECT a.gid
      FROM artist_gid_redirect agr
      JOIN artist a ON agr.new_id = a.id
      WHERE created >= %s
        AND created < %s
    """
    ,
    """
    SELECT gid
      FROM artist_gid_redirect
      WHERE created >= %s
        AND created < %s
    """
    ,
    """
    SELECT gid
      FROM %schema%.l_artist_artist laa
      JOIN %schema%.artist a ON laa.entity1 = a.id
      WHERE laa.last_updated >= %s
        AND laa.last_updated < %s
    """
    ,
    """
    SELECT gid
      FROM %schema%.l_artist_artist laa
      JOIN %schema%.artist a ON laa.entity0 = a.id
      WHERE laa.last_updated >= %s
        AND laa.last_updated < %s
    """
    ,
    """
    SELECT gid
      FROM %schema%.l_artist_label lal
      JOIN %schema%.artist a ON lal.entity0 = a.id
      WHERE lal.last_updated >= %s
        AND lal.last_updated < %s
    """
    ,
    """
    SELECT gid
      FROM %schema%.l_artist_recording lar
      JOIN %schema%.artist a ON lar.entity0 = a.id
      WHERE lar.last_updated >= %s
        AND lar.last_updated < %s
    """
    ,
    """
    SELECT gid
      FROM %schema%.l_artist_release lar
      JOIN %schema%.artist a ON lar.entity0 = a.id
      WHERE lar.last_updated >= %s
        AND lar.last_updated < %s
    """
    ,
    """
    SELECT gid
      FROM %schema%.l_artist_release_group lar
      JOIN %schema%.artist a ON lar.entity0 = a.id
      WHERE lar.last_updated >= %s
        AND lar.last_updated < %s
    """
    ,
    """
    SELECT gid
      FROM %schema%.l_artist_url lau
      JOIN %schema%.artist a ON lau.entity0 = a.id
      WHERE lau.last_updated >= %s
        AND lau.last_updated < %s
    """
    ,
    """
    SELECT gid
      FROM %schema%.l_artist_work law
      JOIN %schema%.artist a ON law.entity0 = a.id
      WHERE law.last_updated >= %s
        AND law.last_updated < %s
    """
  ],
  'work':
  [
    """
    SELECT gid
      FROM %schema%.iswc i
      JOIN %schema%.work w ON i.work = w.id
      WHERE w.last_updated >= %s
        AND w.last_updated < %s
    """
    ,
    """
    SELECT gid
      FROM %schema%.l_artist_work law
      JOIN %schema%.work w ON law.entity1 = w.id
      WHERE law.last_updated >= %s
        AND law.last_updated < %s
    """
    ,
    """
    SELECT gid
      FROM %schema%.l_label_work llw
      JOIN %schema%.work w ON llw.entity1 = w.id
      WHERE llw.last_updated >= %s
        AND llw.last_updated < %s
    """
    ,
    """
    SELECT gid
      FROM %schema%.l_recording_work lrw
      JOIN %schema%.work w ON lrw.entity1 = w.id
      WHERE lrw.last_updated >= %s
        AND lrw.last_updated < %s
    """
    ,
    """
    SELECT gid
      FROM %schema%.l_release_work lrw
      JOIN %schema%.work w ON lrw.entity1 = w.id
      WHERE lrw.last_updated >= %s
        AND lrw.last_updated < %s
    """
    ,
    """
    SELECT gid
      FROM %schema%.l_release_group_work lrw
      JOIN %schema%.work w ON lrw.entity1 = w.id
      WHERE lrw.last_updated >= %s
        AND lrw.last_updated < %s
    """
    ,
    """
    SELECT gid
      FROM %schema%.l_url_work luw
      JOIN %schema%.work w ON luw.entity1 = w.id
      WHERE luw.last_updated >= %s
        AND luw.last_updated < %s
    """
    ,
    """
    SELECT gid
      FROM %schema%.l_work_work lww
      JOIN %schema%.work w ON lww.entity0 = w.id
      WHERE lww.last_updated >= %s
        AND lww.last_updated < %s
    """
    ,
    """
    SELECT gid
      FROM %schema%.l_work_work lww
      JOIN %schema%.work w ON lww.entity1 = w.id
      WHERE lww.last_updated >= %s
        AND lww.last_updated < %s
    """
    ,
    """
    SELECT gid
      FROM %schema%.work
      WHERE last_updated >= %s
        AND last_updated < %s
    """
    ,
    """
    SELECT gid
      FROM %schema%.work_alias wa
      JOIN %schema%.work w ON wa.work = w.id
      WHERE wa.last_updated >= %s
        AND wa.last_updated < %s
    """
    ,
    """
    SELECT gid
      FROM %schema%.work_annotation wa
      JOIN %schema%.work w ON wa.work = w.id
      JOIN %schema%.annotation an ON wa.annotation = an.id
      WHERE an.created >= %s
        AND an.created < %s
    """
    ,
    """
    SELECT w.gid
      FROM work_gid_redirect wgr
      JOIN
      WORK w ON wgr.new_id = w.id
      WHERE created >= %s
        AND created < %s
    """
    ,
    """
    SELECT gid
      FROM work_gid_redirect
      WHERE created >= %s
        AND created < %s
    """
    ,
    """
    SELECT gid
      FROM %schema%.work_tag wt
      JOIN %schema%.work w ON wt.work = w.id
      WHERE wt.last_updated >= %s
        AND wt.last_updated < %s
    """
  ],  
  'label':
  [
    """
    SELECT gid
      FROM %schema%.l_artist_label lal
      JOIN %schema%.label l ON lal.entity1 = l.id
      WHERE lal.last_updated >= %s
        AND lal.last_updated < %s
    """
    ,
    """
    SELECT gid
      FROM %schema%.l_label_label lll
      JOIN %schema%.label l ON lll.entity1 = l.id
      WHERE lll.last_updated >= %s
        AND lll.last_updated < %s
    """
    ,
    """
    SELECT gid
      FROM %schema%.l_label_label lll
      JOIN %schema%.label l ON lll.entity0 = l.id
      WHERE lll.last_updated >= %s
        AND lll.last_updated < %s
    """
    ,
    """
    SELECT gid
      FROM %schema%.l_label_recording llr
      JOIN %schema%.label l ON llr.entity0 = l.id
      WHERE llr.last_updated >= %s
        AND llr.last_updated < %s
    """
    ,
    """
    SELECT gid
      FROM %schema%.l_label_release llr
      JOIN %schema%.label l ON llr.entity0 = l.id
      WHERE llr.last_updated >= %s
        AND llr.last_updated < %s
    """
    ,
    """
    SELECT gid
      FROM %schema%.l_label_release_group llr
      JOIN %schema%.label l ON llr.entity0 = l.id
      WHERE llr.last_updated >= %s
        AND llr.last_updated < %s
    """
    ,
    """
    SELECT gid
      FROM %schema%.l_label_url llu
      JOIN %schema%.label l ON llu.entity0 = l.id
      WHERE llu.last_updated >= %s
        AND llu.last_updated < %s
    """
    ,
    """
    SELECT gid
      FROM %schema%.l_label_work llw
      JOIN %schema%.label l ON llw.entity0 = l.id
      WHERE llw.last_updated >= %s
        AND llw.last_updated < %s
    """
    ,
    """
    SELECT gid
      FROM %schema%.label
      WHERE last_updated >= %s
        AND last_updated < %s
    """
    ,
    """
    SELECT gid
      FROM %schema%.label_alias la
      JOIN %schema%.label l ON la.label = l.id
      WHERE la.last_updated >= %s
        AND la.last_updated < %s
    """
    ,
    """
    SELECT gid
      FROM %schema%.label_annotation la
      JOIN %schema%.label l ON la.label = l.id
      JOIN %schema%.annotation an ON la.annotation = an.id
      WHERE an.created >= %s
        AND an.created < %s
    """
    ,
    """
    SELECT gid
      FROM %schema%.label_ipi li
      JOIN %schema%.label l ON li.label = l.id
      WHERE li.created >= %s
        AND li.created < %s
    """
    ,
    """
    SELECT gid
      FROM %schema%.label_tag lt
      JOIN %schema%.label l ON lt.label = l.id
      WHERE lt.last_updated >= %s
        AND lt.last_updated < %s
    """
    ,
    """
    SELECT l.gid
      FROM label_gid_redirect lgr
      JOIN label l ON lgr.new_id = l.id
      WHERE created >= %s
        AND created < %s
    """
    ,
    """
    SELECT gid
      FROM label_gid_redirect
      WHERE created >= %s
        AND created < %s
    """
    ,
    """
    SELECT gid
      FROM %schema%.release_label rl
      JOIN %schema%.label l ON rl.label = l.id
      WHERE rl.last_updated >= %s
        AND rl.last_updated < %s
    """
  ],  
  'recording':
  [
    """
    SELECT gid
      FROM %schema%.isrc i
      JOIN %schema%.recording r ON i.recording = r.id
      WHERE i.created >= %s
        AND i.created < %s
    """
    ,
    """
    SELECT gid
      FROM %schema%.l_artist_recording lar
      JOIN %schema%.recording r ON lar.entity1 = r.id
      WHERE lar.last_updated >= %s
        AND lar.last_updated < %s
    """
    ,
    """
    SELECT gid
      FROM %schema%.l_label_recording llr
      JOIN %schema%.label l ON llr.entity1 = l.id
      WHERE llr.last_updated >= %s
        AND llr.last_updated < %s
    """
    ,
    """
    SELECT gid
      FROM %schema%.l_recording_recording lrr
      JOIN %schema%.recording r ON lrr.entity1 = r.id
      WHERE lrr.last_updated >= %s
        AND lrr.last_updated < %s
    """
    ,
    """
    SELECT gid
      FROM %schema%.l_recording_recording lrr
      JOIN %schema%.recording r ON lrr.entity0 = r.id
      WHERE lrr.last_updated >= %s
        AND lrr.last_updated < %s
    """
    ,
    """
    SELECT gid
      FROM %schema%.l_recording_release lrr
      JOIN %schema%.recording r ON lrr.entity0 = r.id
      WHERE lrr.last_updated >= %s
        AND lrr.last_updated < %s
    """
    ,
    """
    SELECT gid
      FROM %schema%.l_recording_release_group lrr
      JOIN %schema%.recording r ON lrr.entity0 = r.id
      WHERE lrr.last_updated >= %s
        AND lrr.last_updated < %s
    """
    ,
    """
    SELECT gid
      FROM %schema%.l_recording_url lru
      JOIN %schema%.recording r ON lru.entity0 = r.id
      WHERE lru.last_updated >= %s
        AND lru.last_updated < %s
    """
    ,
    """
    SELECT gid
      FROM %schema%.l_recording_work lrw
      JOIN %schema%.recording r ON lrw.entity0 = r.id
      WHERE lrw.last_updated >= %s
        AND lrw.last_updated < %s
    """
    ,
    """
    SELECT gid
      FROM %schema%.recording
      WHERE last_updated >= %s
        AND last_updated < %s
    """
    ,
    """
    SELECT gid
      FROM %schema%.recording_annotation ra
      JOIN %schema%.recording r ON ra.recording = r.id
      JOIN %schema%.annotation an ON ra.annotation = an.id
      WHERE an.created >= %s
        AND an.created < %s
    """
    ,
    """
    SELECT r.gid
      FROM recording_gid_redirect rgr
      JOIN recording r ON rgr.new_id = r.id
      WHERE created >= %s
        AND created < %s
    """
    ,
    """
    SELECT gid
      FROM recording_gid_redirect
      WHERE created >= %s
        AND created < %s
    """
    ,
    """
    SELECT gid
      FROM %schema%.recording_tag rt
      JOIN %schema%.recording r ON rt.recording = r.id
      WHERE rt.last_updated >= %s
        AND rt.last_updated < %s
    """
    ,
    """
    SELECT r.gid
      FROM %schema%.track t
      JOIN %schema%.recording r ON t.recording = r.id
      WHERE t.last_updated >= %s
        AND t.last_updated < %s
    """
  ],
  'release':
  [
    """
    SELECT gid
      FROM %schema%.l_artist_release lar
      JOIN %schema%.release r ON lar.entity1 = r.id
      WHERE lar.last_updated >= %s
        AND lar.last_updated < %s
    """
    ,
    """
    SELECT gid
      FROM %schema%.l_label_release lr
      JOIN %schema%.release r ON lr.entity1 = r.id
      WHERE lr.last_updated >= %s
        AND lr.last_updated < %s
    """
    ,
    """
    SELECT gid
      FROM %schema%.l_recording_release lrr
      JOIN %schema%.recording r ON lrr.entity1 = r.id
      WHERE lrr.last_updated >= %s
        AND lrr.last_updated < %s
    """
    ,
    """
    SELECT gid
      FROM %schema%.l_release_release lrr
      JOIN %schema%.release r ON lrr.entity0 = r.id
      WHERE lrr.last_updated >= %s
        AND lrr.last_updated < %s
    """
    ,
    """
    SELECT gid
      FROM %schema%.l_release_release lrr
      JOIN %schema%.release r ON lrr.entity1 = r.id
      WHERE lrr.last_updated >= %s
        AND lrr.last_updated < %s
    """
    ,
    """
    SELECT gid
      FROM %schema%.l_release_url lru
      JOIN %schema%.release r ON lru.entity0 = r.id
      WHERE lru.last_updated >= %s
        AND lru.last_updated < %s
    """
    ,
    """
    SELECT gid
      FROM %schema%.l_release_work lrw
      JOIN %schema%.release r ON lrw.entity0 = r.id
      WHERE lrw.last_updated >= %s
        AND lrw.last_updated < %s
    """
    ,
    """
    SELECT gid
      FROM %schema%.medium m
      JOIN %schema%.release r ON m.release = r.id
      WHERE m.last_updated >= %s
        AND m.last_updated < %s
    """
    ,
    """
    SELECT gid
      FROM %schema%.release
      WHERE last_updated >= %s
        AND last_updated < %s
    """
    ,
    """
    SELECT gid
      FROM %schema%.artist_credit ac
      JOIN %schema%.artist_credit_name acn ON acn.artist_credit = ac.id
      JOIN %schema%.release r ON r.artist_credit = ac.id
      WHERE r.last_updated >= %s
        AND r.last_updated < %s
    """
    ,
    """
    SELECT r.gid
      FROM %schema%.release r
      JOIN %schema%.release_group rg ON r.release_group = rg.id
      WHERE r.last_updated >= %s
        AND r.last_updated < %s
    """
    ,
    """
    SELECT gid
      FROM %schema%.release_annotation ra
      JOIN %schema%.release r ON ra.release = r.id
      JOIN %schema%.annotation an ON ra.annotation = an.id
      WHERE an.created >= %s
        AND an.created < %s
    """
    ,
    """
    SELECT r.gid
      FROM release_gid_redirect rgr
      JOIN release r ON rgr.new_id = r.id
      WHERE created >= %s
        AND created < %s
    """
    ,
    """
    SELECT gid
      FROM release_gid_redirect
      WHERE created >= %s
        AND created < %s
    """
    ,
    """
    SELECT gid
      FROM %schema%.release_label rl
      JOIN %schema%.release r ON rl.release = r.id
      WHERE rl.last_updated >= %s
        AND rl.last_updated < %s
    """
    ,
    """
    SELECT gid
      FROM %schema%.release_tag rt
      JOIN %schema%.release r ON rt.release = r.id
      WHERE rt.last_updated >= %s
        AND rt.last_updated < %s
    """
    ,
    """
    SELECT gid
      FROM %schema%.medium_cdtoc mc
      JOIN %schema%.medium m ON mc.medium = m.id
      JOIN %schema%.release r ON m.release = r.id
      WHERE mc.last_updated >= %s
        AND mc.last_updated < %s
    """
    ,
    """
    SELECT gid
      FROM %schema%.medium_cdtoc mc
      JOIN %schema%.cdtoc c ON c.id = mc.cdtoc
      JOIN %schema%.medium m ON mc.medium = m.id
      JOIN %schema%.release r ON m.release = r.id
      WHERE c.created >= %s
        AND c.created < %s
    """
    ,
    """
    SELECT r.gid
      FROM %schema%.track t
      JOIN %schema%.medium m ON m.id = t.medium
      JOIN %schema%.release r ON m.release = r.id
      WHERE t.last_updated >= %s
        AND t.last_updated < %s
    """
    ,
    """
    SELECT r.gid
      FROM %schema%.recording rec
      JOIN %schema%.track t ON t.recording = rec.id
      JOIN %schema%.medium m ON m.id = t.medium
      JOIN %schema%.release r ON m.release = r.id
      WHERE rec.last_updated >= %s
        AND rec.last_updated < %s
    """
  ],  
  'release_group':
  [
    """
    SELECT gid
      FROM %schema%.l_artist_release_group lar
      JOIN %schema%.release_group r ON lar.entity1 = r.id
      WHERE lar.last_updated >= %s
        AND lar.last_updated < %s
    """
    ,
    """
    SELECT gid
      FROM %schema%.l_label_release_group llr
      JOIN %schema%.label l ON llr.entity1 = l.id
      WHERE llr.last_updated >= %s
        AND llr.last_updated < %s
    """
    ,
    """
    SELECT gid
      FROM %schema%.l_recording_release_group lrr
      JOIN %schema%.release_group r ON lrr.entity1 = r.id
      WHERE lrr.last_updated >= %s
        AND lrr.last_updated < %s
    """
    ,
    """
    SELECT gid
      FROM %schema%.l_release_release_group lrr
      JOIN %schema%.release_group r ON lrr.entity1 = r.id
      WHERE lrr.last_updated >= %s
        AND lrr.last_updated < %s
    """
    ,
    """
    SELECT gid
      FROM %schema%.l_release_group_release_group lrr
      JOIN %schema%.release_group r ON lrr.entity0 = r.id
      WHERE lrr.last_updated >= %s
        AND lrr.last_updated < %s
    """
    ,
    """
    SELECT gid
      FROM %schema%.l_release_group_release_group lrr
      JOIN %schema%.release_group r ON lrr.entity1 = r.id
      WHERE lrr.last_updated >= %s
        AND lrr.last_updated < %s
    """
    ,
    """
    SELECT gid
      FROM %schema%.l_release_group_url lru
      JOIN %schema%.release_group r ON lru.entity0 = r.id
      WHERE lru.last_updated >= %s
        AND lru.last_updated < %s
    """
    ,
    """
    SELECT gid
      FROM %schema%.l_release_group_work lrw
      JOIN %schema%.release_group r ON lrw.entity0 = r.id
      WHERE lrw.last_updated >= %s
        AND lrw.last_updated < %s
    """
    ,
    """
    SELECT rg.gid
      FROM %schema%.release r
      JOIN %schema%.release_group rg ON r.release_group = rg.id
      WHERE r.last_updated >= %s
        AND r.last_updated < %s
    """
    ,
    """
    SELECT gid
      FROM %schema%.release_group
      WHERE last_updated >= %s
        AND last_updated < %s
    """
    ,
    """
    SELECT gid
      FROM %schema%.release_group_annotation ra
      JOIN %schema%.release_group r ON ra.release_group = r.id
      JOIN %schema%.annotation an ON ra.annotation = an.id
      WHERE an.created >= %s
        AND an.created < %s
    """
    ,
    """
    SELECT r.gid
      FROM release_group_gid_redirect rgr
      JOIN release_group r ON rgr.new_id = r.id
      WHERE created >= %s
        AND created < %s
    """
    ,
    """
    SELECT gid
      FROM release_group_gid_redirect
      WHERE created >= %s
        AND created < %s
    """
    ,
    """
    SELECT gid
      FROM %schema%.release_group_tag rt
      JOIN %schema%.release_group r ON rt.release_group = r.id
      WHERE rt.last_updated >= %s
        AND rt.last_updated < %s
    """
    ,
    """
    SELECT gid
      FROM %schema%.l_artist_release_group lar
      JOIN %schema%.release_group r ON lar.entity1 = r.id
      WHERE lar.last_updated >= %s
        AND lar.last_updated < %s
    """
  ]
}
