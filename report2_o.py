SELECT
  s.*,
  h.header_version,
  h.header_version_status
FROM pdipp.prvrostercnf_conformed_file_stats AS s
JOIN pdipp.prvroster_header_tracking_pht AS h
  ON h.file_name = s.file_name
 AND h.header_version = 1
 AND h.header_version_status ILIKE 'new';
