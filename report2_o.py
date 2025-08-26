def add_new_roster_formats_simple(
    df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Count new roster formats per business owner based on version status and number.
    Only counts records where header_version_status starts with "NEW" (case-insensitive)
    and header_version_number equals 1.
    
    Args:
        df: DataFrame containing roster information
    
    Returns:
        DataFrame with added column for new roster format counts
    """
    # Create a copy of the DataFrame
    out = df.copy()
    
    # Helper function to identify business owner column
    def _business_owner_col(df: pd.DataFrame) -> str:
        """
        Identify the business owner column.
        Looks for common variations of business owner column names.
        """
        possible_names = ["business_owner", "business_owner_name", "owner", "business_owner_col"]
        
        for col_name in possible_names:
            if col_name in df.columns:
                return col_name
        
        # Return the most common name if none found
        return "business_owner"
    
    # Get business owner column name
    owner_col = _business_owner_col(out)
    
    # Check if required columns exist
    required_cols = [owner_col, "header_version_status", "header_version_number"]
    if not all(col in out.columns for col in required_cols):
        out["# New Roster Formats"] = 0
        return out
    
    # Filter for version 1 and statuses that start with "NEW" (case-insensitive)
    new_status_condition = (
        out["header_version_status"].str.upper().str.startswith("NEW")
    )
    version_condition = out["header_version_number"] == 1
    
    # Apply both conditions
    is_new = new_status_condition & version_condition
    
    # Count by business owner and transform to match original DataFrame length
    counts = is_new.groupby(out[owner_col]).transform("sum")
    
    # Add the count column
    out["# New Roster Formats"] = counts.astype(int)
    
    return out
***********************************************
import pandas as pd
from typing import Tuple, Optional

def add_new_roster_formats_simple(
    df: pd.DataFrame,
    owner_col: str = "business_owner",
    status_col: str = "header_version_status",
    version_col: str = "header_version_number",
    include: Tuple[str, ...] = ("NEW_FILE", "NEW_VERSION", "ACTIVE"),
) -> pd.DataFrame:
    """
    Add a '# New Roster Formats' column that counts, per business owner,
    rows where:
        - header_version_number == 1
        - header_version_status indicates a 'new' status
          (matches `include` after replacing '_' with ' ' and case-insensitive),
          or begins with 'NEW' (fallback).

    The count is broadcast to all rows for that owner.

    If any required column is missing, the column is added with 0s and returned.
    """
    out = df.copy()

    # tolerant, case-insensitive column resolution
    lc_map = {c.lower(): c for c in out.columns}
    def _resolve(name: str) -> Optional[str]:
        return lc_map.get(name.lower())

    owner_col_res   = _resolve(owner_col)
    status_col_res  = _resolve(status_col)
    version_col_res = _resolve(version_col)

    required = [owner_col_res, status_col_res, version_col_res]
    if any(c is None for c in required):
        out["# New Roster Formats"] = 0
        return out

    # normalize status list
    include_norm = {s.replace("_", " ").strip().upper() for s in include}

    # boolean mask for "new" rows
    status_series = out[status_col_res].astype(str).str.upper().str.strip()
    version_is_1  = out[version_col_res].fillna(0).astype(int).eq(1)
    status_is_new = status_series.isin(include_norm) | status_series.str.startswith("NEW")

    is_new = version_is_1 & status_is_new

    # count per owner and broadcast back
    counts_by_owner = (
        is_new.groupby(out[owner_col_res]).sum().astype(int)
    )
    out["# New Roster Formats"] = (
        out[owner_col_res].map(counts_by_owner).fillna(0).astype(int)
    )

    return out

**********************************************
SELECT
  s.*,
  h.header_version,
  h.header_version_status
FROM pdipp.prvrostercnf_conformed_file_stats AS s
JOIN pdipp.prvroster_header_tracking_pht AS h
  ON h.file_name = s.file_name
 AND h.header_version = 1
 AND h.header_version_status ILIKE 'new';
