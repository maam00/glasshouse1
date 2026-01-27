"""
Glass House CSV Importer
=========================
Import Parcl Labs CSV exports as fallback when API credits are limited.

Supports:
- Sales CSV (from "Home Sales by Day" download)
- Listings CSV (from "For Sale Listings" download)
"""

import os
from pathlib import Path
from datetime import datetime
from typing import Optional, Tuple
import logging

import pandas as pd

logger = logging.getLogger(__name__)


class CSVImporter:
    """Import and normalize Parcl Labs CSV exports."""

    # Default import directory
    DEFAULT_IMPORT_DIR = Path(__file__).parent.parent.parent / "data" / "imports"

    # Column name mappings (Parcl CSV -> internal names)
    # These will be refined once we see actual CSV exports
    SALES_COLUMN_MAP = {
        # Property ID
        "Property ID": "property_id",

        # Sale price
        "Sale Price": "sale_price",
        "sale_price": "sale_price",

        # Purchase price/date
        "Purchase Price": "purchase_price",
        "purchase_price": "purchase_price",
        "Purchase Date": "purchase_date",

        # Days held
        "Days Held": "days_held",
        "days_held": "days_held",

        # Realized net / profit
        "Realized Net": "realized_net",
        "realized_net": "realized_net",

        # Sale date
        "Sale Date": "sale_date",
        "sale_date": "sale_date",

        # Quarter/Year
        "Quarter": "quarter",
        "Year": "year",

        # Buyer (usually empty for Opendoor sales)
        "Buyer Entity": "buyer_entity",

        # Location (may not be present in sales)
        "State": "state",
        "City": "city",
        "Address": "address",
    }

    LISTINGS_COLUMN_MAP = {
        # Property ID
        "Property ID": "property_id",

        # Location
        "Address": "address",
        "City": "city",
        "State": "state",
        "ZIP Code": "zip_code",

        # Property details
        "Property Type": "property_type",
        "Square Feet": "sqft",
        "Bedrooms": "bedrooms",
        "Bathrooms": "bathrooms",
        "Year Built": "year_built",

        # Purchase info
        "Original Purchase Date": "purchase_date",
        "Original Purchase Price": "purchase_price",

        # Listing info
        "Initial Listing Date": "initial_list_date",
        "Initial Listing Price": "initial_list_price",
        "Latest Listing Date": "latest_list_date",
        "Latest Listing Price": "list_price",

        # Key metrics
        "Days on Market": "days_on_market",
        "Price Cuts": "price_cuts",
        "Unrealized Net": "unrealized_net",
        "Unrealized Net %": "unrealized_net_pct",
    }

    def __init__(self, import_dir: Path = None):
        self.import_dir = import_dir or self.DEFAULT_IMPORT_DIR
        self.import_dir.mkdir(parents=True, exist_ok=True)

    def _clean_currency(self, val):
        """Convert currency string like '$288,000' to float."""
        if pd.isna(val) or val == "" or val == "$0":
            return 0.0
        if isinstance(val, (int, float)):
            return float(val)
        # Remove $ and commas
        cleaned = str(val).replace("$", "").replace(",", "").strip()
        if cleaned == "" or cleaned == "0":
            return 0.0
        try:
            return float(cleaned)
        except ValueError:
            return 0.0

    def _clean_date(self, val):
        """Convert date string like 'Jan 22, 2026' to datetime."""
        if pd.isna(val) or val == "":
            return None
        if isinstance(val, datetime):
            return val
        try:
            # Try common formats
            for fmt in ["%b %d, %Y", "%Y-%m-%d", "%m/%d/%Y", "%m-%d-%Y"]:
                try:
                    return datetime.strptime(str(val).strip(), fmt)
                except ValueError:
                    continue
            return pd.to_datetime(val)
        except Exception:
            return None

    def _clean_numeric(self, val):
        """Convert to numeric, handling blanks."""
        if pd.isna(val) or val == "":
            return 0
        try:
            return int(float(val))
        except (ValueError, TypeError):
            return 0

    def _clean_dataframe(self, df: pd.DataFrame, csv_type: str) -> pd.DataFrame:
        """Clean and convert data types for a DataFrame."""
        df = df.copy()

        # Currency columns
        currency_cols = [
            "sale_price", "purchase_price", "realized_net",
            "list_price", "initial_list_price", "unrealized_net"
        ]
        for col in currency_cols:
            if col in df.columns:
                df[col] = df[col].apply(self._clean_currency)

        # Date columns
        date_cols = [
            "sale_date", "purchase_date",
            "initial_list_date", "latest_list_date"
        ]
        for col in date_cols:
            if col in df.columns:
                df[col] = df[col].apply(self._clean_date)

        # Numeric columns
        numeric_cols = ["days_held", "days_on_market", "price_cuts", "sqft", "bedrooms", "year_built"]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = df[col].apply(self._clean_numeric)

        return df

    def _normalize_columns(self, df: pd.DataFrame, column_map: dict) -> pd.DataFrame:
        """Normalize column names using mapping."""
        rename_dict = {}
        for col in df.columns:
            # Try exact match first
            if col in column_map:
                rename_dict[col] = column_map[col]
            # Try case-insensitive match
            else:
                col_lower = col.lower().strip()
                for map_key, map_val in column_map.items():
                    if map_key.lower() == col_lower:
                        rename_dict[col] = map_val
                        break

        if rename_dict:
            df = df.rename(columns=rename_dict)

        return df

    def _detect_csv_type(self, df: pd.DataFrame) -> str:
        """Detect if CSV is sales or listings based on columns."""
        cols_lower = [c.lower() for c in df.columns]

        # Sales indicators
        sales_indicators = ["sale price", "sold price", "realized net", "days held", "homes sold"]
        # Listings indicators
        listings_indicators = ["list price", "days on market", "dom", "listing price"]

        sales_score = sum(1 for ind in sales_indicators if any(ind in c for c in cols_lower))
        listings_score = sum(1 for ind in listings_indicators if any(ind in c for c in cols_lower))

        if sales_score > listings_score:
            return "sales"
        elif listings_score > sales_score:
            return "listings"
        else:
            return "unknown"

    def import_sales_csv(self, filepath: str = None) -> pd.DataFrame:
        """
        Import sales CSV from Parcl Labs.

        Args:
            filepath: Path to CSV. If None, looks for most recent in import_dir.

        Returns:
            Normalized DataFrame ready for MetricsCalculator.
        """
        if filepath is None:
            filepath = self._find_latest_csv("sales")
            if filepath is None:
                logger.warning("No sales CSV found in import directory")
                return pd.DataFrame()

        filepath = Path(filepath)
        if not filepath.exists():
            logger.error(f"Sales CSV not found: {filepath}")
            return pd.DataFrame()

        logger.info(f"Importing sales CSV: {filepath}")

        try:
            df = pd.read_csv(filepath)
            logger.info(f"  Raw columns: {list(df.columns)}")
            logger.info(f"  Rows: {len(df)}")

            # Normalize columns
            df = self._normalize_columns(df, self.SALES_COLUMN_MAP)
            logger.info(f"  Normalized columns: {list(df.columns)}")

            # Clean data types (currency, dates, etc.)
            df = self._clean_dataframe(df, "sales")

            # Handle aggregated daily data (expand if needed)
            df = self._expand_aggregated_sales(df)

            logger.info(f"  Cleaned. Sample realized_net: {df['realized_net'].head(3).tolist() if 'realized_net' in df.columns else 'N/A'}")

            return df

        except Exception as e:
            logger.error(f"Error importing sales CSV: {e}")
            import traceback
            traceback.print_exc()
            return pd.DataFrame()

    def import_listings_csv(self, filepath: str = None) -> pd.DataFrame:
        """
        Import listings CSV from Parcl Labs.

        Args:
            filepath: Path to CSV. If None, looks for most recent in import_dir.

        Returns:
            Normalized DataFrame ready for MetricsCalculator.
        """
        if filepath is None:
            filepath = self._find_latest_csv("listings")
            if filepath is None:
                logger.warning("No listings CSV found in import directory")
                return pd.DataFrame()

        filepath = Path(filepath)
        if not filepath.exists():
            logger.error(f"Listings CSV not found: {filepath}")
            return pd.DataFrame()

        logger.info(f"Importing listings CSV: {filepath}")

        try:
            df = pd.read_csv(filepath)
            logger.info(f"  Raw columns: {list(df.columns)}")
            logger.info(f"  Rows: {len(df)}")

            # Normalize columns
            df = self._normalize_columns(df, self.LISTINGS_COLUMN_MAP)
            logger.info(f"  Normalized columns: {list(df.columns)}")

            # Clean data types (currency, dates, etc.)
            df = self._clean_dataframe(df, "listings")

            logger.info(f"  Cleaned. Sample days_on_market: {df['days_on_market'].head(3).tolist() if 'days_on_market' in df.columns else 'N/A'}")

            return df

        except Exception as e:
            logger.error(f"Error importing listings CSV: {e}")
            import traceback
            traceback.print_exc()
            return pd.DataFrame()

    def import_auto(self, filepath: str) -> Tuple[Optional[pd.DataFrame], str]:
        """
        Auto-detect CSV type and import appropriately.

        Returns:
            Tuple of (DataFrame, type) where type is "sales" or "listings".
        """
        filepath = Path(filepath)
        if not filepath.exists():
            logger.error(f"CSV not found: {filepath}")
            return None, "unknown"

        df = pd.read_csv(filepath)
        csv_type = self._detect_csv_type(df)

        if csv_type == "sales":
            return self.import_sales_csv(filepath), "sales"
        elif csv_type == "listings":
            return self.import_listings_csv(filepath), "listings"
        else:
            logger.warning(f"Could not detect CSV type for: {filepath}")
            return df, "unknown"

    def _find_latest_csv(self, csv_type: str) -> Optional[Path]:
        """Find most recent CSV of given type in import directory."""
        patterns = {
            "sales": ["*sales*.csv", "*sold*.csv", "*home_sales*.csv"],
            "listings": ["*listing*.csv", "*inventory*.csv", "*for_sale*.csv"],
        }

        candidates = []
        for pattern in patterns.get(csv_type, ["*.csv"]):
            candidates.extend(self.import_dir.glob(pattern))

        if not candidates:
            # Try any CSV and detect type
            all_csvs = list(self.import_dir.glob("*.csv"))
            for csv_file in sorted(all_csvs, key=lambda x: x.stat().st_mtime, reverse=True):
                df = pd.read_csv(csv_file)
                if self._detect_csv_type(df) == csv_type:
                    return csv_file

        if candidates:
            # Return most recently modified
            return max(candidates, key=lambda x: x.stat().st_mtime)

        return None

    def _expand_aggregated_sales(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Expand aggregated daily sales data into individual rows.

        The Parcl "Home Sales by Day" table shows:
        Date | Homes Sold | Avg Price | Total Revenue

        We need to expand this for cohort analysis if we don't have
        individual property data.
        """
        # Check if this is aggregated data
        if "homes_sold" in df.columns and "sale_date" in df.columns:
            # This is aggregated - we can't do cohort analysis without individual records
            # But we can still track daily totals
            logger.warning("Sales CSV appears to be aggregated daily data")
            logger.warning("Cohort analysis requires individual property records")

            # Keep as-is for now - the metrics calculator will handle it
            pass

        return df

    def list_available_csvs(self) -> dict:
        """List all CSVs in import directory with detected types."""
        result = {"sales": [], "listings": [], "unknown": []}

        for csv_file in self.import_dir.glob("*.csv"):
            try:
                df = pd.read_csv(csv_file, nrows=5)  # Just read header + few rows
                csv_type = self._detect_csv_type(df)
                result[csv_type].append({
                    "path": str(csv_file),
                    "name": csv_file.name,
                    "modified": datetime.fromtimestamp(csv_file.stat().st_mtime).isoformat(),
                    "rows": len(pd.read_csv(csv_file)),
                })
            except Exception as e:
                result["unknown"].append({
                    "path": str(csv_file),
                    "name": csv_file.name,
                    "error": str(e),
                })

        return result


def import_from_clipboard() -> Tuple[Optional[pd.DataFrame], str]:
    """
    Import data directly from clipboard (for pasting tables).

    Returns:
        Tuple of (DataFrame, detected_type).
    """
    try:
        df = pd.read_clipboard()
        importer = CSVImporter()
        csv_type = importer._detect_csv_type(df)

        if csv_type == "sales":
            df = importer._normalize_columns(df, importer.SALES_COLUMN_MAP)
        elif csv_type == "listings":
            df = importer._normalize_columns(df, importer.LISTINGS_COLUMN_MAP)

        return df, csv_type

    except Exception as e:
        logger.error(f"Error reading from clipboard: {e}")
        return None, "error"
