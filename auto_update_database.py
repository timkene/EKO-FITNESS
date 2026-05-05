#!/usr/bin/env python3
"""
Auto-Update Database Script
Automatically refreshes the AI DRIVEN DATA database with latest data from source systems
"""
# Load .env so MOTHERDUCK_TOKEN is set when running from CLI (no need to export manually)
try:
    from dotenv import load_dotenv
    from pathlib import Path
    _env = Path(__file__).resolve().parent / ".env"
    if _env.exists():
        load_dotenv(_env)
except ImportError:
    pass

import duckdb
import pandas as pd
import os
import sys
import logging
from datetime import datetime, timedelta
from dlt_sources import (
    total_pa_procedures, claims, all_providers, all_group,
    all_active_member, benefitcode, benefitcode_procedure, group_plan,
    pa_issue_request, proceduredata, e_account_group, debit_note, group_contract, fin_gl,
    # Newly added resources to mirror into DuckDB
    tariff, group_coverage, member_plans, planbenefitcode_limit, plans,
    group_invoice, premium1_schedule, fin_accsetup, providers_tariff, member_provider,
    diagnosis, member, member_coverage,
    # EACCOUNT connection helper
    create_eacount_connection
)
from create_debit_note_accrued import create_debit_note_accrued_table, validate_table
from create_client_cash_received import create_client_cash_received_year, create_client_cash_received_combined, validate_client_cash_received_tables
from create_salary_and_palliative import create_salary_and_palliative_year, create_salary_and_palliative_combined, validate_salary_and_palliative_tables
from create_expense_and_commission import create_expense_and_commission_year, create_expense_and_commission_combined, validate_expense_and_commission_tables
from mongodb import (
    fetch_collections_as_dataframes,
    get_requests_with_populated_procedures,
    get_duckdb_connection,
    create_nhia_schema,
    push_dataframe_to_duckdb
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('database_update.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class DatabaseUpdater:
    """Handles automatic database updates for both local DuckDB and MotherDuck"""
    
    # Schema name (same for both local and MotherDuck)
    SCHEMA_NAME = 'AI DRIVEN DATA'
    
    # MotherDuck configuration (set MOTHERDUCK_TOKEN or MOTHERDUCK_PAT in env)
    MOTHERDUCK_TOKEN = os.environ.get('MOTHERDUCK_TOKEN') or os.environ.get('MOTHERDUCK_PAT') or ''
    MOTHERDUCK_DB = 'ai_driven_data'
    
    def __init__(self, db_path='ai_driven_data.duckdb', update_motherduck=True, motherduck_only=False):
        self.db_path = db_path
        self.conn = None  # Local DuckDB connection
        self.md_conn = None  # MotherDuck connection
        self.update_motherduck = update_motherduck
        self.motherduck_only = motherduck_only  # If True, skip local updates entirely
        self.update_stats = {
            'start_time': None,
            'end_time': None,
            'tables_updated': 0,
            'total_rows_updated': 0,
            'local_errors': [],
            'motherduck_errors': [],
            'errors': []
        }
    
    def connect(self):
        """Connect to local DuckDB and/or MotherDuck"""
        # Connect to local DuckDB (only if not motherduck_only mode)
        if not self.motherduck_only:
            try:
                self.conn = duckdb.connect(self.db_path)
                logger.info(f"✅ Connected to local database: {self.db_path}")
                # Initialize metadata tracking
                self._init_metadata_table(self.conn)
            except Exception as e:
                logger.error(f"❌ Failed to connect to local database: {e}")
                return False
        
        # Connect to MotherDuck if enabled
        if self.update_motherduck:
            if not self.MOTHERDUCK_TOKEN or not self.MOTHERDUCK_TOKEN.strip():
                logger.warning(
                    "⚠️ MOTHERDUCK_TOKEN (or MOTHERDUCK_PAT) not set. "
                    "Add it to a .env file in the project root, or run: export MOTHERDUCK_TOKEN=your_token"
                )
            try:
                self.md_conn = duckdb.connect(f'md:?motherduck_token={self.MOTHERDUCK_TOKEN}')
                self.md_conn.execute(f"CREATE DATABASE IF NOT EXISTS {self.MOTHERDUCK_DB}")
                self.md_conn.execute(f"USE {self.MOTHERDUCK_DB}")
                logger.info(f"✅ Connected to MotherDuck database: {self.MOTHERDUCK_DB}")
                # Initialize metadata tracking
                self._init_metadata_table(self.md_conn)
            except Exception as e:
                logger.error(f"❌ Failed to connect to MotherDuck: {e}")
                if self.motherduck_only:
                    return False  # If motherduck_only, we must have MotherDuck connection
                logger.warning("⚠️ Continuing with local database only")
                self.update_motherduck = False
                self.md_conn = None
        
        return True
    
    def _init_metadata_table(self, conn):
        """Initialize metadata table for tracking last update timestamps"""
        try:
            schema = self.SCHEMA_NAME
            conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
            conn.execute(f'''
                CREATE TABLE IF NOT EXISTS "{schema}"."_update_metadata" (
                    table_name VARCHAR PRIMARY KEY,
                    last_update_timestamp TIMESTAMP,
                    last_update_date DATE,
                    row_count BIGINT,
                    last_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
        except Exception as e:
            logger.warning(f"⚠️ Could not initialize metadata table: {e}")
    
    def _get_last_update_timestamp(self, conn, table_name, date_column='datesubmitted'):
        """Get the last update timestamp for a table"""
        try:
            schema = self.SCHEMA_NAME
            result = conn.execute(f'''
                SELECT last_update_timestamp, last_update_date
                FROM "{schema}"."_update_metadata"
                WHERE table_name = ?
            ''', [table_name]).fetchone()
            
            if result and result[0]:
                return result[0], result[1]
            
            # If no metadata, try to get max date from the table itself
            try:
                max_date = conn.execute(f'''
                    SELECT MAX({date_column}) as max_date
                    FROM "{schema}"."{table_name}"
                ''').fetchone()[0]
                if max_date:
                    return max_date, max_date.date() if hasattr(max_date, 'date') else max_date
            except:
                pass
            
            return None, None
        except Exception as e:
            logger.debug(f"Could not get last update timestamp for {table_name}: {e}")
            return None, None
    
    def _prepare_full_pa_data_reload(self):
        """
        Drop local PA DATA and clear its smart-update metadata so the next run
        fetches the full 2-year window (since_date=None). Needed after query
        changes (e.g. join fix) because _get_last_update_timestamp falls back to
        MAX(requestdate) when metadata is missing, which would otherwise keep
        incremental mode and leave old duplicate rows.
        """
        if not self.conn or self.motherduck_only:
            return
        schema = self.SCHEMA_NAME
        try:
            self.conn.execute(
                f'DELETE FROM "{schema}"."_update_metadata" WHERE table_name = ?',
                ['PA DATA'],
            )
            logger.info('🗑️ Cleared _update_metadata for PA DATA (full reload)')
        except Exception as e:
            logger.warning(f'⚠️ Could not clear PA DATA metadata (table may not exist yet): {e}')
        try:
            self.conn.execute(f'DROP TABLE IF EXISTS "{schema}"."PA DATA"')
            logger.info('🗑️ Dropped local "PA DATA" table for full reload')
        except Exception as e:
            logger.warning(f'⚠️ Could not drop PA DATA: {e}')

    def _update_metadata(self, conn, table_name, timestamp, row_count):
        """Update metadata table with last update information"""
        try:
            schema = self.SCHEMA_NAME
            # Convert timestamp to appropriate format
            if hasattr(timestamp, 'date'):
                last_date = timestamp.date()
            elif isinstance(timestamp, str):
                last_date = pd.to_datetime(timestamp).date()
            else:
                last_date = timestamp
            
            # Use REPLACE or DELETE + INSERT for DuckDB compatibility
            conn.execute(f'''
                DELETE FROM "{schema}"."_update_metadata" WHERE table_name = ?
            ''', [table_name])
            
            conn.execute(f'''
                INSERT INTO "{schema}"."_update_metadata" 
                    (table_name, last_update_timestamp, last_update_date, row_count, last_updated_at)
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
            ''', [table_name, timestamp, last_date, row_count])
        except Exception as e:
            logger.warning(f"⚠️ Could not update metadata for {table_name}: {e}")
    
    def disconnect(self):
        """Disconnect from both databases"""
        if self.conn:
            self.conn.close()
            logger.info("✅ Disconnected from local database")
        
        if self.md_conn:
            self.md_conn.close()
            logger.info("✅ Disconnected from MotherDuck")

    def get_gl_year_info(self) -> dict:
        """
        Inspect EACCOUNT to understand GL year state dynamically.
        
        Logic:
        - Get current year from datetime
        - Check what year the live FIN_GL contains (MAX(YEAR(GLDate)))
        - Check if that year's archive exists (FIN_GL{year} table)
        - If archive exists, live FIN_GL is for next year
        - If archive doesn't exist, live FIN_GL is for that year
        
        Returns:
            dict with:
            - live_gl_year: The year that live FIN_GL currently contains
            - has_archive: Whether the live_gl_year has been archived
            - all_archived_years: List of all archived years found
        """
        info = {
            "live_gl_year": None,
            "has_archive": False,
            "all_archived_years": [],
        }
        try:
            conn = create_eacount_connection()
            cursor = conn.cursor()

            # Determine current live GL year from dbo.FIN_GL
            try:
                cursor.execute("SELECT MAX(YEAR(GLDate)) FROM dbo.FIN_GL")
                row = cursor.fetchone()
                if row and row[0]:
                    max_year_in_gl = int(row[0])
                    
                    # Check if the previous year (max_year - 1) has been archived
                    # If FIN_GL{max_year-1} exists, then that year is closed and max_year is current
                    prev_year = max_year_in_gl - 1
                    cursor.execute(
                        """
                        SELECT COUNT(*)
                        FROM INFORMATION_SCHEMA.TABLES
                        WHERE TABLE_TYPE = 'BASE TABLE'
                          AND UPPER(TABLE_NAME) = ?
                        """,
                        (f'FIN_GL{prev_year}',)
                    )
                    prev_year_archived = cursor.fetchone()[0] > 0
                    
                    if prev_year_archived:
                        # Previous year is archived, so max_year is the current live year
                        info["live_gl_year"] = max_year_in_gl
                        info["has_archive"] = True  # Previous year has archive
                        logger.info(f"📊 Year {prev_year} is archived. Live FIN_GL contains year {info['live_gl_year']}")
                    else:
                        # Previous year is not archived, so max_year is still the current live year
                        info["live_gl_year"] = max_year_in_gl
                        info["has_archive"] = False
                        logger.info(f"📊 Year {info['live_gl_year']} is current. Live FIN_GL contains year {info['live_gl_year']}")
            except Exception as e:
                logger.warning(f"⚠️ Could not determine current GL year from dbo.FIN_GL: {e}")

            # Get all archived years for reference
            try:
                cursor.execute(
                    """
                    SELECT TABLE_NAME
                    FROM INFORMATION_SCHEMA.TABLES
                    WHERE TABLE_TYPE = 'BASE TABLE'
                      AND UPPER(TABLE_NAME) LIKE 'FIN_GL%'
                      AND UPPER(TABLE_NAME) != 'FIN_GL'
                    ORDER BY TABLE_NAME
                    """
                )
                archived_tables = [row[0] for row in cursor.fetchall()]
                # Extract years from table names (e.g., "FIN_GL2023" -> 2023)
                for table_name in archived_tables:
                    # Try to extract year from table name
                    import re
                    match = re.search(r'(\d{4})', table_name)
                    if match:
                        year = int(match.group(1))
                        if year not in info["all_archived_years"]:
                            info["all_archived_years"].append(year)
                info["all_archived_years"].sort()
            except Exception as e:
                logger.debug(f"Could not list archived years: {e}")

            conn.close()
        except Exception as e:
            logger.warning(f"⚠️ Could not inspect EACCOUNT GL year info: {e}")

        logger.info(
            f"📊 EACCOUNT GL year info: live_gl_year={info['live_gl_year']}, "
            f"has_archive={info['has_archive']}, archived_years={info['all_archived_years']}"
        )
        return info
    
    def _update_table_in_db(self, conn, table_name, new_data, schema_sql, db_type="local"):
        """Helper method to update a table in a specific database"""
        try:
            schema = self.SCHEMA_NAME
            # Ensure schema exists
            conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
            
            # Ensure table schema exists if provided
            if schema_sql:
                try:
                    conn.execute(schema_sql)
                except Exception:
                    pass  # Schema might already exist
            
            # Determine if table exists and current count
            table_exists = True
            try:
                current_count = conn.execute(
                    f'SELECT COUNT(*) FROM "{schema}"."{table_name}"'
                ).fetchone()[0]
            except Exception:
                current_count = 0
                table_exists = False

            if table_exists:
                try:
                    # Clear existing data and insert
                    conn.execute(f'DELETE FROM "{schema}"."{table_name}"')
                    conn.register('new_data', new_data)
                    conn.execute(
                        f'INSERT INTO "{schema}"."{table_name}" SELECT * FROM new_data'
                    )
                except Exception:
                    # Fallback: hard recreate to match source schema
                    conn.execute(f'DROP TABLE IF EXISTS "{schema}"."{table_name}"')
                    conn.register('new_data', new_data)
                    conn.execute(
                        f'CREATE TABLE "{schema}"."{table_name}" AS SELECT * FROM new_data'
                    )
            else:
                # First creation from incoming data
                conn.register('new_data', new_data)
                conn.execute(
                    f'CREATE TABLE "{schema}"."{table_name}" AS SELECT * FROM new_data'
                )
            
            new_count = len(new_data)
            logger.info(f"  ✅ [{db_type.upper()}] {table_name}: {current_count:,} → {new_count:,} rows (+{new_count - current_count:,})")
            return True, new_count
            
        except Exception as e:
            error_msg = f"  ❌ [{db_type.upper()}] Failed to update {table_name}: {e}"
            logger.error(error_msg)
            return False, 0
    
    def update_table(self, table_name, data_loader, schema_sql, sample_creator=None, enable_smart_update=True, date_column=None):
        """
        Update a specific table with new data in both local and MotherDuck.
        
        Args:
            table_name: Name of the table to update
            data_loader: Function that yields DataFrame (can accept since_date parameter)
            schema_sql: SQL schema definition
            sample_creator: Optional function to create sample data
            enable_smart_update: If True, use incremental updates for large tables
            date_column: Column name to use for incremental updates (e.g., 'datesubmitted', 'requestdate')
        """
        try:
            logger.info(f"🔄 Updating {table_name}...")
            
            # Determine if we should use smart incremental updates
            # Tables that support smart updates
            smart_update_tables = {
                'PA DATA': ('requestdate', total_pa_procedures),
                'CLAIMS DATA': ('datesubmitted', claims)
            }
            
            # Use smart update if:
            # 1. Smart updates are enabled
            # 2. Table supports smart updates
            # 3. Not in motherduck_only mode (smart updates work on local DB)
            # 4. We have a local connection
            use_smart_update = (enable_smart_update and 
                              table_name in smart_update_tables and 
                              not self.motherduck_only and 
                              self.conn is not None)
            
            if use_smart_update:
                date_col, loader_func = smart_update_tables[table_name]
                return self._smart_update_table(table_name, loader_func, date_col, schema_sql)
            
            # Standard full update
            # Get new data
            new_data = list(data_loader())[0]
            
            if new_data.empty:
                logger.warning(f"⚠️ No data found for {table_name}")
                if sample_creator:
                    # Empty fetch usually means MediCloud/EACCOUNT was unreachable. Do not
                    # overwrite an already-populated table with toy sample rows.
                    if self.conn and not self.motherduck_only:
                        try:
                            schema = self.SCHEMA_NAME
                            cnt = self.conn.execute(
                                f'SELECT COUNT(*) FROM "{schema}"."{table_name}"'
                            ).fetchone()[0]
                            if cnt and cnt > 0:
                                logger.warning(
                                    f"⚠️ Keeping existing {table_name} ({cnt:,} rows); "
                                    "refusing sample fallback on empty fetch."
                                )
                                return False
                        except Exception:
                            pass
                    logger.info(f"📝 Creating sample data for {table_name}")
                    new_data = sample_creator()
                else:
                    return False
            
            # Column name mapping for CLAIMS DATA - ensure procedurecode is renamed to code
            # and select only the columns that match the schema
            if table_name == 'CLAIMS DATA':
                if 'procedurecode' in new_data.columns:
                    new_data = new_data.rename(columns={'procedurecode': 'code'})
                    logger.debug("✅ Renamed procedurecode to code for CLAIMS DATA")
                
                # Map nhislegacynumber to enrollee_id if needed
                if 'nhislegacynumber' in new_data.columns and 'enrollee_id' not in new_data.columns:
                    new_data = new_data.rename(columns={'nhislegacynumber': 'enrollee_id'})
                    logger.debug("✅ Renamed nhislegacynumber to enrollee_id for CLAIMS DATA")
                
                # Select only the columns that match the schema to avoid issues
                schema_columns = [
                    'enrollee_id', 'providerid', 'groupid', 'nhisgroupid', 'nhisproviderid', 
                    'panumber', 'encounterdatefrom', 'encounterdateto', 'datesubmitted', 
                    'chargeamount', 'approvedamount', 'code', 'deniedamount', 'diagnosiscode', 
                    'claimnumber', 'memberid', 'dependantnumber', 'isinpatient', 'discount', 
                    'datereceived', 'claimstatusid', 'adjusterid', 'unitfactor', 'isapproved', 
                    'isfinal', 'ispaid', 'amountpaid', 'datepaid', 'paymentbatchno', 'dateadded', 
                    'claimid', 'nhisdependantnumber'
                ]
                available_columns = [col for col in schema_columns if col in new_data.columns]
                if available_columns:
                    new_data = new_data[available_columns]
                    logger.debug(f"✅ Selected {len(available_columns)} schema columns for CLAIMS DATA")
                    logger.debug(f"   Columns: {', '.join(available_columns[:10])}{'...' if len(available_columns) > 10 else ''}")
            
            # Update local database (skip if motherduck_only mode)
            local_success = True
            local_count = 0
            if not self.motherduck_only and self.conn:
                local_success, local_count = self._update_table_in_db(
                    self.conn, table_name, new_data, schema_sql, "local"
                )
                
                if not local_success:
                    self.update_stats['local_errors'].append(f"{table_name}: Local update failed")
                    self.update_stats['errors'].append(f"{table_name}: Local update failed")
            
            # Update MotherDuck if enabled
            md_success = True
            md_count = 0
            if self.update_motherduck and self.md_conn:
                md_success, md_count = self._update_table_in_db(
                    self.md_conn, table_name, new_data, schema_sql, "motherduck"
                )
                
                if not md_success:
                    self.update_stats['motherduck_errors'].append(f"{table_name}: MotherDuck update failed")
                    self.update_stats['errors'].append(f"{table_name}: MotherDuck update failed")
            
            # Track statistics
            if local_success and not self.motherduck_only:
                self.update_stats['tables_updated'] += 1
                self.update_stats['total_rows_updated'] += local_count
            elif md_success:
                self.update_stats['tables_updated'] += 1
                self.update_stats['total_rows_updated'] += md_count
            
            return local_success or md_success
            
        except Exception as e:
            error_msg = f"❌ Failed to update {table_name}: {e}"
            logger.error(error_msg)
            self.update_stats['errors'].append(f"{table_name}: {e}")
            return False
    
    def _smart_update_table(self, table_name, data_loader, date_column, schema_sql):
        """
        Smart incremental update: only fetch and merge new records.
        
        Args:
            table_name: Name of the table
            data_loader: Function that accepts since_date parameter
            date_column: Column name for date filtering (e.g., 'datesubmitted', 'requestdate')
            schema_sql: SQL schema definition
        """
        try:
            schema = self.SCHEMA_NAME
            
            # Check if table exists and get last update timestamp
            last_timestamp, last_date = self._get_last_update_timestamp(self.conn, table_name, date_column)
            
            if last_timestamp:
                # Incremental update: fetch only new records
                logger.info(f"📊 Smart update: Fetching records since {last_date}")
                # Subtract 1 day to catch any records that might have been missed
                since_date = last_date - timedelta(days=1) if hasattr(last_date, '__sub__') else last_date
                new_data = list(data_loader(since_date=since_date))[0]
                mode = "incremental"
            else:
                # First run or no metadata: do full 2-year fetch
                logger.info(f"📊 Smart update: First run - fetching 2-year window")
                new_data = list(data_loader(since_date=None))[0]
                mode = "full (first run)"
            
            if new_data.empty:
                logger.info(f"✅ No new data for {table_name}")
                return True
            
            logger.info(f"📥 Fetched {len(new_data):,} new rows ({mode})")
            
            # Column name mapping and filtering for CLAIMS DATA
            if table_name == 'CLAIMS DATA':
                if 'procedurecode' in new_data.columns:
                    new_data = new_data.rename(columns={'procedurecode': 'code'})
                
                if 'nhislegacynumber' in new_data.columns and 'enrollee_id' not in new_data.columns:
                    new_data = new_data.rename(columns={'nhislegacynumber': 'enrollee_id'})
                
                # Select only the columns that match the schema
                schema_columns = [
                    'enrollee_id', 'providerid', 'groupid', 'nhisgroupid', 'nhisproviderid', 
                    'panumber', 'encounterdatefrom', 'encounterdateto', 'datesubmitted', 
                    'chargeamount', 'approvedamount', 'code', 'deniedamount', 'diagnosiscode', 
                    'claimnumber', 'memberid', 'dependantnumber', 'isinpatient', 'discount', 
                    'datereceived', 'claimstatusid', 'adjusterid', 'unitfactor', 'isapproved', 
                    'isfinal', 'ispaid', 'amountpaid', 'datepaid', 'paymentbatchno', 'dateadded', 
                    'claimid', 'nhisdependantnumber'
                ]
                available_columns = [col for col in schema_columns if col in new_data.columns]
                if available_columns:
                    new_data = new_data[available_columns]
                    logger.debug(f"✅ Filtered to {len(available_columns)} schema columns for CLAIMS DATA")
            
            # Ensure schema exists
            self.conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
            if schema_sql:
                try:
                    self.conn.execute(schema_sql)
                except:
                    pass
            
            # Check if table exists
            table_exists = True
            try:
                current_count = self.conn.execute(f'SELECT COUNT(*) FROM "{schema}"."{table_name}"').fetchone()[0]
            except:
                current_count = 0
                table_exists = False

            if not table_exists:
                # First time: create table with all data
                logger.info(f"📝 Creating {table_name} for the first time...")
                self.conn.register('new_data', new_data)
                self.conn.execute(f'CREATE TABLE "{schema}"."{table_name}" AS SELECT * FROM new_data')
                self.conn.unregister('new_data')
                new_count = len(new_data)
                final_count = new_count
                logger.info(f"  ✅ [{mode.upper()}] {table_name}: {new_count:,} rows (created)")
            else:
                # Incremental update: merge new data
                logger.info(f"🔄 Merging {len(new_data):,} new rows into existing table...")
                
                # Determine primary key for deduplication
                # For PA DATA: use panumber + code + requestdate
                # For CLAIMS DATA: use claimid or claimnumber
                if table_name == 'PA DATA':
                    # Use panumber + code + requestdate as unique key
                    if 'panumber' in new_data.columns and 'code' in new_data.columns and 'requestdate' in new_data.columns:
                        # Create temp table with new data
                        self.conn.register('new_data', new_data)
                        
                        # Delete existing records that match new data (upsert)
                        self.conn.execute(f'''
                            DELETE FROM "{schema}"."{table_name}" t
                            WHERE EXISTS (
                                SELECT 1 FROM new_data n
                                WHERE CAST(t.panumber AS VARCHAR) = CAST(n.panumber AS VARCHAR)
                                AND CAST(t.code AS VARCHAR) = CAST(n.code AS VARCHAR)
                                AND t.requestdate = n.requestdate
                            )
                        ''')
                        
                        # Insert new data
                        self.conn.execute(f'INSERT INTO "{schema}"."{table_name}" SELECT * FROM new_data')
                        self.conn.unregister('new_data')
                    else:
                        # Fallback: replace all data
                        logger.warning(f"⚠️ Missing key columns, using full replace")
                        self.conn.execute(f'DELETE FROM "{schema}"."{table_name}"')
                        self.conn.register('new_data', new_data)
                        self.conn.execute(f'INSERT INTO "{schema}"."{table_name}" SELECT * FROM new_data')
                        self.conn.unregister('new_data')
                
                elif table_name == 'CLAIMS DATA':
                    # Use claimid or claimnumber as unique key
                    if 'claimid' in new_data.columns:
                        # Create temp table with new data
                        self.conn.register('new_data', new_data)
                        
                        # Delete existing records that match new data (upsert)
                        self.conn.execute(f'''
                            DELETE FROM "{schema}"."{table_name}" t
                            WHERE EXISTS (
                                SELECT 1 FROM new_data n
                                WHERE CAST(t.claimid AS VARCHAR) = CAST(n.claimid AS VARCHAR)
                            )
                        ''')
                        
                        # Insert new data
                        self.conn.execute(f'INSERT INTO "{schema}"."{table_name}" SELECT * FROM new_data')
                        self.conn.unregister('new_data')
                    elif 'claimnumber' in new_data.columns:
                        self.conn.register('new_data', new_data)
                        self.conn.execute(f'''
                            DELETE FROM "{schema}"."{table_name}" t
                            WHERE EXISTS (
                                SELECT 1 FROM new_data n
                                WHERE CAST(t.claimnumber AS VARCHAR) = CAST(n.claimnumber AS VARCHAR)
                            )
                        ''')
                        self.conn.execute(f'INSERT INTO "{schema}"."{table_name}" SELECT * FROM new_data')
                        self.conn.unregister('new_data')
                    else:
                        # Fallback: replace all data
                        logger.warning(f"⚠️ Missing key columns, using full replace")
                        self.conn.execute(f'DELETE FROM "{schema}"."{table_name}"')
                        self.conn.register('new_data', new_data)
                        self.conn.execute(f'INSERT INTO "{schema}"."{table_name}" SELECT * FROM new_data')
                        self.conn.unregister('new_data')
                else:
                    # Unknown table: use full replace
                    logger.warning(f"⚠️ Unknown table structure, using full replace")
                    self.conn.execute(f'DELETE FROM "{schema}"."{table_name}"')
                    self.conn.register('new_data', new_data)
                    self.conn.execute(f'INSERT INTO "{schema}"."{table_name}" SELECT * FROM new_data')
                    self.conn.unregister('new_data')
                
                # Get final count
                final_count = self.conn.execute(f'SELECT COUNT(*) FROM "{schema}"."{table_name}"').fetchone()[0]
                new_count = len(new_data)
                logger.info(f"  ✅ [{mode.upper()}] {table_name}: {current_count:,} → {final_count:,} rows (+{new_count:,} new)")
            
            # Update metadata with latest timestamp
            if date_column in new_data.columns and not new_data.empty:
                max_timestamp = pd.to_datetime(new_data[date_column]).max()
                # Get current row count for metadata
                current_row_count = self.conn.execute(f'SELECT COUNT(*) FROM "{schema}"."{table_name}"').fetchone()[0]
                self._update_metadata(self.conn, table_name, max_timestamp, current_row_count)
            
            # Update MotherDuck if enabled (always full sync from local DB)
            if self.update_motherduck and self.md_conn:
                logger.info(f"☁️  Syncing {table_name} to MotherDuck (full sync from local)...")
                try:
                    # Sync full table from local DB to MotherDuck
                    self.md_conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
                    # Ensure any previous local_db attachment is cleared
                    try:
                        self.md_conn.execute("DETACH local_db")
                    except Exception:
                        pass
                    self.md_conn.execute(f"ATTACH '{self.db_path}' AS local_db (READ_ONLY)")
                    self.md_conn.execute(f'DROP TABLE IF EXISTS "{schema}"."{table_name}"')
                    self.md_conn.execute(f'CREATE TABLE "{schema}"."{table_name}" AS SELECT * FROM local_db."{schema}"."{table_name}"')
                    self.md_conn.execute("DETACH local_db")
                    
                    md_count = self.md_conn.execute(f'SELECT COUNT(*) FROM "{schema}"."{table_name}"').fetchone()[0]
                    logger.info(f"  ✅ [MOTHERDUCK] {table_name}: {md_count:,} rows (synced from local)")
                except Exception as e:
                    logger.error(f"  ❌ [MOTHERDUCK] Failed to sync {table_name}: {e}")
                    self.update_stats['motherduck_errors'].append(f"{table_name}: {e}")
            
            # Track statistics
            self.update_stats['tables_updated'] += 1
            self.update_stats['total_rows_updated'] += new_count
            
            return True
            
        except Exception as e:
            error_msg = f"❌ Smart update failed for {table_name}: {e}"
            logger.error(error_msg)
            logger.exception(e)  # Log full traceback
            self.update_stats['errors'].append(f"{table_name}: {e}")
            # Fallback to full update
            logger.info(f"🔄 Falling back to full update for {table_name}...")
            return self.update_table(table_name, data_loader, schema_sql, enable_smart_update=False)
    
    def create_sample_pa_data(self):
        """Create sample PA data if needed"""
        import numpy as np
        from datetime import datetime, timedelta
        
        sample_data = {
            'panumber': [f'PA{str(i).zfill(6)}' for i in range(1, 21)],
            'groupname': ['ACME Corp', 'Tech Solutions Ltd', 'Healthcare Plus', 'Global Industries', 'MediCare Group'] * 4,
            'divisionname': ['Lagos', 'Abuja', 'Kano', 'Lagos', 'Abuja'] * 4,
            'plancode': ['PLAN001', 'PLAN002', 'PLAN003', 'PLAN001', 'PLAN002'] * 4,
            'IID': [f'IID{str(i).zfill(4)}' for i in range(1, 21)],
            'providerid': [f'PROV{str(i).zfill(3)}' for i in range(1, 21)],
            'requestdate': [datetime.now() - timedelta(days=i*2) for i in range(20)],
            'pastatus': ['APPROVED', 'PENDING', 'REJECTED', 'APPROVED', 'PENDING'] * 4,
            'code': [f'PROC{str(i).zfill(3)}' for i in range(1, 21)],
            'userid': [f'USER{str(i).zfill(3)}' for i in range(1, 21)],
            'totaltariff': [np.random.uniform(1000, 50000) for _ in range(20)],
            'benefitcode': [f'BEN{str(i).zfill(3)}' for i in range(1, 21)],
            'dependantnumber': [f'DEP{str(i).zfill(3)}' for i in range(1, 21)],
            'requested': [np.random.uniform(500, 25000) for _ in range(20)],
            'granted': [np.random.uniform(400, 20000) for _ in range(20)]
        }
        return pd.DataFrame(sample_data)
    
    def create_sample_claims_data(self):
        """Create sample claims data if needed"""
        import numpy as np
        from datetime import datetime, timedelta
        
        sample_data = {
            'enrollee_id': [f'CL/IIS/378/01A', f'CL/IGCL/739660/2024-A', f'CL/ARIK/482/2017~B', f'CL/TECH/123/2024', f'CL/HEALTH/456/2023'],
            'providerid': ['977', '977', '977', '123', '456'],
            'groupid': ['1292', '1453', '1328', '2001', '2002'],
            'panumber': [0, 0, 0, 100001, 100002],
            'encounterdatefrom': [(datetime.now() - timedelta(days=i*30)).date() for i in range(5)],
            'datesubmitted': [(datetime.now() - timedelta(days=i*7)).date() for i in range(5)],
            'chargeamount': [2000.0, 900.0, 1350.0, 27.0, 2000.0],
            'approvedamount': [2000.0, 900.0, 1350.0, 486.0, 2000.0],
            'code': ['CONS021', 'DIAG435', 'DRG1081', 'DRG2641', 'CONS021'],
            'deniedamount': [0.0, 0.0, 0.0, 0.0, 0.0]
        }
        return pd.DataFrame(sample_data)
    
    def create_sample_providers_data(self):
        """Create sample providers data if needed"""
        from datetime import datetime, timedelta
        
        sample_data = {
            'providerid': ['850', '851', '852', '853', '854'],
            'providername': ['Bolasad Specialist Hospital', 'Bidems Victory Hosp & Diag Centre', 'Ayo Clinic', 'Channels Clinic & Hosp. Nig. Ltd', 'City International Clinics Taraba'],
            'dateadded': [datetime.now() - timedelta(days=i*30) for i in range(5)],
            'isvisible': [False, True, True, True, False],
            'lganame': ['Ikeja', 'Ikorodu', 'KOSOFE', 'Obia/Akpor', 'Jalingo'],
            'statename': ['Lagos', 'Lagos', 'Lagos', 'Rivers', 'Taraba'],
            'bands': ['Band D', 'Band D', 'Band D', 'Band D', 'Band D']
        }
        return pd.DataFrame(sample_data)
    
    def create_sample_groups_data(self):
        """Create sample groups data if needed"""
        from datetime import datetime, timedelta
        
        sample_data = {
            'groupid': [800, 801, 802, 803, 804],
            'groupname': ['CRYSTAL FINANCE COMPANY LIMITED', 'AG HOMES', 'FUNSHO LOGISTICS LIMITED', 'Spice Digital Nigeria Limited', 'Proximity Communications Limited'],
            'lganame': ['Ikeja', 'Ikeja', 'Lekki', 'Victoria Island', 'Ikeja'],
            'statename': ['Lagos', 'Lagos', 'Lagos', 'Lagos', 'Lagos'],
            'dateadded': [datetime.now() - timedelta(days=i*30) for i in range(5)]
        }
        return pd.DataFrame(sample_data)
    
    def create_sample_members_data(self):
        """Create sample members data if needed"""
        from datetime import datetime, timedelta
        import random
        
        sample_data = {
            'memberid': ['M001', 'M002', 'M003', 'M004', 'M005'],
            'groupid': [800, 801, 802, 803, 804],
            'enrollee_id': ['CL/MEM/001', 'CL/MEM/002', 'CL/MEM/003', 'CL/MEM/004', 'CL/MEM/005'],
            'planid': ['PLAN001', 'PLAN002', 'PLAN001', 'PLAN003', 'PLAN002'],
            'iscurrent': [True, True, True, True, True],
            'isterminated': [False, False, False, False, False],
            'dob': [(datetime.now() - timedelta(days=random.randint(18*365, 65*365))) for _ in range(5)],
            'genderid': [1, 2, 1, 2, 1],  # 1=male, 2=female
            'email1': ['member1@email.com', 'member2@email.com', 'member3@email.com', 'member4@email.com', 'member5@email.com'],
            'email2': [None, None, None, None, None],
            'email3': [None, None, None, None, None],
            'email4': [None, None, None, None, None],
            'phone1': ['08012345678', '08012345679', '08012345680', '08012345681', '08012345682'],
            'phone2': [None, None, None, None, None],
            'phone3': [None, None, None, None, None],
            'phone4': [None, None, None, None, None],
            'address1': ['123 Main St', '456 Oak Ave', '789 Pine Rd', '321 Elm St', '654 Maple Dr'],
            'address2': [None, None, None, None, None],
            'registrationdate': [(datetime.now() - timedelta(days=random.randint(30, 365))) for _ in range(5)],
            'effectivedate': [(datetime.now() - timedelta(days=random.randint(1, 30))) for _ in range(5)],
            'terminationdate': [(datetime.now() + timedelta(days=random.randint(30, 365))) for _ in range(5)]
        }
        return pd.DataFrame(sample_data)
    
    def create_sample_benefitcode_data(self):
        """Create sample benefitcode data if needed"""
        sample_data = {
            'benefitcodeid': [1, 3, 4, 5, 7],
            'benefitcodedesc': ['ADMISSION AND FEEDING', 'ICU ADMISSION', 'PERSONAL MEDICAL DEVICES', 'PHOTOTHERAPY', 'PSYCHIATRIC TREATMENT']
        }
        return pd.DataFrame(sample_data)
    
    def create_sample_benefitcode_procedure_data(self):
        """Create sample benefitcode_procedure data if needed"""
        sample_data = {
            'benefitcodeid': [1, 1, 3, 3, 4],
            'procedurecode': ['DIAG010', 'DIAG019', 'DIAG228', 'DIAG278', 'DIAG307']
        }
        return pd.DataFrame(sample_data)
    
    def create_sample_group_plan_data(self):
        """Create sample group_plan data if needed"""
        from datetime import datetime, timedelta
        
        sample_data = {
            'groupid': [800, 801, 802, 803, 804],
            'planlimit': [1200000.0, 1800000.0, 1300000.0, 1200000.0, 1300000.0],
            'countofindividual': [4, 10, 1, 20, 2],
            'countoffamily': [0, 0, 0, 0, 0],
            'individualprice': [65000.0, 79200.0, 48000.0, 48000.0, 60000.0],
            'familyprice': [0.0, 0.0, 0.0, 0.0, 0.0],
            'maxnumdependant': [5, 5, 5, 5, 5]
        }
        return pd.DataFrame(sample_data)
    
    def build_fin_gl_raw_from_excel(self, year):
        """Build FIN_GL raw table from Excel file"""
        try:
            excel_file = 'GL 2023 and 2024.xlsx'
            if not os.path.exists(excel_file):
                logger.warning(f"Excel file not found: {excel_file}")
                return pd.DataFrame()
            
            # Sheet 0 is 2023, Sheet 1 is 2024
            sheet_idx = 0 if year == 2023 else 1
            xls = pd.ExcelFile(excel_file)
            df = xls.parse(xls.sheet_names[sheet_idx])
            
            # Map columns
            cols = {c.lower().strip(): c for c in df.columns}
            def pick(*names):
                for n in names:
                    if n in cols: return cols[n]
                return None
            
            out = pd.DataFrame()
            out['glid'] = pd.to_numeric(df.get(pick('glid')), errors='coerce') if pick('glid') else pd.Series(range(1, len(df)+1))
            out['acctype'] = df.get(pick('acctype'))
            out['glcode'] = df.get(pick('acccode','glcode','code')).astype(str)
            out['gldesc'] = df.get(pick('gldesc','accdesc','description','desc')).astype(str)
            out['gldate'] = pd.to_datetime(df.get(pick('gldate','date')), errors='coerce')
            out['glamount'] = pd.to_numeric(df.get(pick('glamount','amount')), errors='coerce')
            out['refno'] = df.get(pick('refno'))
            # CODE is the company ID column, not acccode!
            out['code'] = pd.to_numeric(df.get(pick('code')), errors='coerce')
            out['code'] = out['code'].astype('Int64').astype(str).replace('<NA>', None)
            out['acccode'] = out['glcode']  # acccode is the account code
            
            # Tag acctype for CASH and CURRENT ASSETS
            CASH = ('1618002','1618003','1618007','1619029','1618046','1618047','1618049','1618051','1619031','1618053','1618055')
            ASSETS = ('1312001','1312002')
            out.loc[out['acccode'].isin(CASH), 'acctype'] = 'CASH'
            out.loc[out['acccode'].isin(ASSETS), 'acctype'] = 'CURRENT ASSETS'
            
            out = out.sort_values(['gldate','glcode','gldesc'], na_position='last').reset_index(drop=True)
            return out
            
        except Exception as e:
            logger.error(f"Failed to build FIN_GL from Excel for {year}: {e}")
            return pd.DataFrame()
    
    def build_fin_gl_raw_from_eaccount(self, year: int):
        """
        Build FIN_GL_{year}_RAW from EACCOUNT database (generic, future-proof).
        
        Args:
            year: The year to fetch from live FIN_GL table
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            from dlt_sources import create_eacount_connection
            import pyodbc
            
            logger.info(f"📥 Connecting to EACCOUNT to fetch {year} data...")
            conn = create_eacount_connection()
            logger.info("✅ Connected to EACCOUNT!")
            
            # Fetch data for the specified year
            query = f"""
                SELECT *
                FROM dbo.FIN_GL
                WHERE YEAR(GLDate) = {year}
                ORDER BY GLDate, AccCode
            """
            logger.info(f"📥 Fetching {year} data from EACCOUNT...")
            df = pd.read_sql(query, conn)
            conn.close()
            logger.info(f"✅ Successfully loaded {len(df):,} rows from EACCOUNT")
            
            if df.empty:
                logger.warning(f"⚠️ No data found for {year}")
                return False
            
            # Build FIN_GL_{year}_RAW
            logger.info(f"🔨 Building FIN_GL_{year}_RAW...")
            schema = self.SCHEMA_NAME
            
            # Map columns from EACCOUNT to our format
            out = pd.DataFrame()
            
            # GLID
            out['glid'] = pd.to_numeric(df['GLID'], errors='coerce')
            
            # Acctype
            out['acctype'] = df.get('acctype')
            
            # GLCode / AccCode
            out['glcode'] = df['AccCode'].astype(str)
            
            # GLDesc
            out['gldesc'] = df.get('GLDesc')
            
            # GLDate
            out['gldate'] = pd.to_datetime(df['GLDate'], errors='coerce')
            
            # GLAmount
            out['glamount'] = pd.to_numeric(df['GLAmount'], errors='coerce')
            
            # RefNo
            out['refno'] = df.get('RefNo')
            
            # CODE - company ID (this is the key column!)
            out['code'] = pd.to_numeric(df['code'], errors='coerce')
            out['code'] = out['code'].astype('Int64').astype(str).replace('<NA>', None)
            
            # AccCode
            out['acccode'] = out['glcode']
            
            # Tag acctype for CASH and CURRENT ASSETS
            CASH = ('1618002','1618003','1618007','1619029','1618046','1618047','1618049','1618051','1619031','1618053','1618055')
            ASSETS = ('1312001','1312002')
            out.loc[out['acccode'].isin(CASH), 'acctype'] = 'CASH'
            out.loc[out['acccode'].isin(ASSETS), 'acctype'] = 'CURRENT ASSETS'
            
            out = out.sort_values(['gldate','glcode','gldesc'], na_position='last').reset_index(drop=True)
            
            # Save to DuckDB
            if not self.motherduck_only and self.conn:
                table_name = f"FIN_GL_{year}_RAW"
                self.conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
                self.conn.execute(f'DROP TABLE IF EXISTS "{schema}"."{table_name}"')
                self.conn.register('df_src', out)
                self.conn.execute(f'CREATE TABLE "{schema}"."{table_name}" AS SELECT * FROM df_src')
                cnt = self.conn.execute(f'SELECT COUNT(*) FROM "{schema}"."{table_name}"').fetchone()[0]
                logger.info(f"✅ {table_name} created with {cnt:,} rows")
                
                code_count = self.conn.execute(f'SELECT COUNT(*) FROM "{schema}"."{table_name}" WHERE code IS NOT NULL').fetchone()[0]
                logger.info(f"📊 Rows with code (company ID): {code_count:,}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Error building FIN_GL_{year}_RAW: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False
    
    def update_fin_gl_tables(self):
        """Update FIN_GL raw tables in local and/or MotherDuck"""
        try:
            logger.info("🔄 Updating FIN_GL raw tables...")
            
            # Inspect EACCOUNT GL state dynamically to determine current live year
            gl_info = self.get_gl_year_info()
            live_gl_year = gl_info.get("live_gl_year")
            has_archive = gl_info.get("has_archive", False)
            archived_years = gl_info.get("all_archived_years", [])
            
            if not live_gl_year:
                logger.warning("⚠️ Could not determine live GL year from EACCOUNT. Skipping live GL update.")
            else:
                logger.info(f"📊 Live FIN_GL contains year {live_gl_year} data")
            
            # Update historical years from Excel (2023, 2024 - these are static)
            for year in [2023, 2024]:
                table_name = f"FIN_GL_{year}_RAW"
                logger.info(f"📊 Updating {table_name} from Excel...")
                
                data = self.build_fin_gl_raw_from_excel(year)
                
                if data.empty:
                    logger.warning(f"No data found for {table_name}")
                    continue
                
                schema = self.SCHEMA_NAME
                
                # Update local (if not motherduck_only)
                if not self.motherduck_only and self.conn:
                    self.conn.execute(f'DROP TABLE IF EXISTS "{schema}"."{table_name}"')
                    self.conn.register('df_src', data)
                    self.conn.execute(f'CREATE TABLE "{schema}"."{table_name}" AS SELECT * FROM df_src')
                    row_count = len(data)
                    logger.info(f"  ✅ [LOCAL] {table_name}: {row_count:,} rows")
                
                # Update MotherDuck
                if self.update_motherduck and self.md_conn:
                    try:
                        self.md_conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
                        self.md_conn.execute(f'DROP TABLE IF EXISTS "{schema}"."{table_name}"')
                        self.md_conn.register('df_src', data)
                        self.md_conn.execute(f'CREATE TABLE "{schema}"."{table_name}" AS SELECT * FROM df_src')
                        row_count = len(data)
                        logger.info(f"  ✅ [MOTHERDUCK] {table_name}: {row_count:,} rows")
                    except Exception as e:
                        logger.error(f"  ❌ [MOTHERDUCK] Failed to update {table_name}: {e}")
                        self.update_stats['motherduck_errors'].append(f"{table_name}: {e}")
            
            # Update current live year from EACCOUNT (dynamic - works for any year)
            if not self.motherduck_only and live_gl_year:
                # live_gl_year is the year that live FIN_GL currently contains
                # If the previous year (live_gl_year - 1) is archived, that confirms live_gl_year is current
                # If the previous year is NOT archived, then live_gl_year is still current (it's the first year)
                # Either way, we update FIN_GL_{live_gl_year}_RAW from live FIN_GL
                table_name = f"FIN_GL_{live_gl_year}_RAW"
                logger.info(f"📊 Updating {table_name} from EACCOUNT (year {live_gl_year} is current live year)...")
                success = self.build_fin_gl_raw_from_eaccount(live_gl_year)
                if not success:
                    logger.warning(f"⚠️ {table_name} update failed - may need manual retry")
                else:
                    # Sync to MotherDuck if it was successfully updated locally
                    if self.update_motherduck and self.md_conn:
                        try:
                            # Attach local DB and copy the table
                            schema = self.SCHEMA_NAME
                            self.md_conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
                            try:
                                self.md_conn.execute("DETACH local_db")
                            except Exception:
                                pass
                            self.md_conn.execute(f"ATTACH '{self.db_path}' AS local_db (READ_ONLY)")
                            self.md_conn.execute(f'DROP TABLE IF EXISTS "{schema}"."{table_name}"')
                            self.md_conn.execute(f'CREATE TABLE "{schema}"."{table_name}" AS SELECT * FROM local_db."{schema}"."{table_name}"')
                            self.md_conn.execute("DETACH local_db")
                            
                            count = self.md_conn.execute(f'SELECT COUNT(*) FROM "{schema}"."{table_name}"').fetchone()[0]
                            logger.info(f"  ✅ [MOTHERDUCK] {table_name}: {count:,} rows")
                        except Exception as e:
                            logger.error(f"  ❌ [MOTHERDUCK] Failed to sync {table_name}: {e}")
                            self.update_stats['motherduck_errors'].append(f"{table_name}: {e}")
            else:
                # In motherduck_only mode, sync from local if it exists (for current live year)
                if self.update_motherduck and self.md_conn and live_gl_year:
                    # live_gl_year is the current live year - sync it from local
                    table_name = f"FIN_GL_{live_gl_year}_RAW"
                    try:
                        schema = self.SCHEMA_NAME
                        self.md_conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
                        try:
                            self.md_conn.execute("DETACH local_db")
                        except Exception:
                            pass
                        self.md_conn.execute(f"ATTACH '{self.db_path}' AS local_db (READ_ONLY)")
                        self.md_conn.execute(f'DROP TABLE IF EXISTS "{schema}"."{table_name}"')
                        self.md_conn.execute(f'CREATE TABLE "{schema}"."{table_name}" AS SELECT * FROM local_db."{schema}"."{table_name}"')
                        self.md_conn.execute("DETACH local_db")
                        
                        count = self.md_conn.execute(f'SELECT COUNT(*) FROM "{schema}"."{table_name}"').fetchone()[0]
                        logger.info(f"  ✅ [MOTHERDUCK] {table_name}: {count:,} rows (synced from local)")
                    except Exception as e:
                        logger.warning(f"  ⚠️ [MOTHERDUCK] Could not sync {table_name} from local: {e}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to update FIN_GL tables: {e}")
            self.update_stats['errors'].append(f"FIN_GL tables: {e}")
            return False
    
    def update_derived_tables(self):
        """Update all derived tables (DEBIT_NOTE_ACCRUED, CLIENT_CASH_RECEIVED, etc.) in local and/or MotherDuck"""
        try:
            logger.info("🔄 Updating derived tables...")
            
            # Update DEBIT_NOTE_ACCRUED using our dedicated script (only if not motherduck_only)
            if not self.motherduck_only and self.conn:
                logger.info("📊 Updating DEBIT_NOTE_ACCRUED...")
                summary = create_debit_note_accrued_table(self.conn)
                logger.info(f"  ✅ [LOCAL] DEBIT_NOTE_ACCRUED: {summary['total_rows']:,} rows, ₦{summary['total_amount']:,.0f}")
                
                # Validate the table was created correctly
                if validate_table(self.conn):
                    logger.info("  ✅ [LOCAL] DEBIT_NOTE_ACCRUED validation passed")
                else:
                    logger.error("  ❌ [LOCAL] DEBIT_NOTE_ACCRUED validation failed")
                    raise Exception("DEBIT_NOTE_ACCRUED table validation failed")
            
            # Sync to MotherDuck (from local if available, or create directly if motherduck_only)
            if self.update_motherduck and self.md_conn:
                try:
                    schema = self.SCHEMA_NAME
                    self.md_conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
                    if not self.motherduck_only and self.conn:
                        # Sync from local
                        try:
                            self.md_conn.execute("DETACH local_db")
                        except Exception:
                            pass
                        self.md_conn.execute(f"ATTACH '{self.db_path}' AS local_db (READ_ONLY)")
                        self.md_conn.execute(f'DROP TABLE IF EXISTS "{schema}"."DEBIT_NOTE_ACCRUED"')
                        self.md_conn.execute(f'CREATE TABLE "{schema}"."DEBIT_NOTE_ACCRUED" AS SELECT * FROM local_db."{schema}"."DEBIT_NOTE_ACCRUED"')
                        self.md_conn.execute("DETACH local_db")
                        count = self.md_conn.execute(f'SELECT COUNT(*) FROM "{schema}"."DEBIT_NOTE_ACCRUED"').fetchone()[0]
                        logger.info(f"  ✅ [MOTHERDUCK] DEBIT_NOTE_ACCRUED: {count:,} rows")
                    else:
                        # In motherduck_only mode, try to sync from local if it exists
                        try:
                            try:
                                self.md_conn.execute("DETACH local_db")
                            except Exception:
                                pass
                            self.md_conn.execute(f"ATTACH '{self.db_path}' AS local_db (READ_ONLY)")
                            self.md_conn.execute(f'DROP TABLE IF EXISTS "{schema}"."DEBIT_NOTE_ACCRUED"')
                            self.md_conn.execute(f'CREATE TABLE "{schema}"."DEBIT_NOTE_ACCRUED" AS SELECT * FROM local_db."{schema}"."DEBIT_NOTE_ACCRUED"')
                            self.md_conn.execute("DETACH local_db")
                            count = self.md_conn.execute(f'SELECT COUNT(*) FROM "{schema}"."DEBIT_NOTE_ACCRUED"').fetchone()[0]
                            logger.info(f"  ✅ [MOTHERDUCK] DEBIT_NOTE_ACCRUED: {count:,} rows (synced from local)")
                        except Exception as e2:
                            logger.warning(f"  ⚠️ [MOTHERDUCK] Could not sync DEBIT_NOTE_ACCRUED from local: {e2}")
                except Exception as e:
                    logger.error(f"  ❌ [MOTHERDUCK] Failed to sync DEBIT_NOTE_ACCRUED: {e}")
                    self.update_stats['motherduck_errors'].append(f"DEBIT_NOTE_ACCRUED: {e}")
            
            # Update CLIENT_CASH_RECEIVED tables
            if not self.motherduck_only and self.conn:
                logger.info("📊 Updating CLIENT_CASH_RECEIVED tables...")
                for year in [2023, 2024, 2025, 2026]:
                    summary = create_client_cash_received_year(self.conn, year)
                    logger.info(f"  ✅ [LOCAL] CLIENT_CASH_RECEIVED_{year}: {summary['total_rows']:,} rows, ₦{summary['total_amount']:,.0f}")
                    
                    # Sync to MotherDuck
                    if self.update_motherduck and self.md_conn:
                        try:
                            schema = self.SCHEMA_NAME
                            try:
                                self.md_conn.execute("DETACH local_db")
                            except Exception:
                                pass
                            self.md_conn.execute(f"ATTACH '{self.db_path}' AS local_db (READ_ONLY)")
                            self.md_conn.execute(f'DROP TABLE IF EXISTS "{schema}"."CLIENT_CASH_RECEIVED_{year}"')
                            self.md_conn.execute(f'CREATE TABLE "{schema}"."CLIENT_CASH_RECEIVED_{year}" AS SELECT * FROM local_db."{schema}"."CLIENT_CASH_RECEIVED_{year}"')
                            self.md_conn.execute("DETACH local_db")
                            logger.info(f"  ✅ [MOTHERDUCK] CLIENT_CASH_RECEIVED_{year}: {summary['total_rows']:,} rows")
                        except Exception as e:
                            logger.error(f"  ❌ [MOTHERDUCK] Failed to sync CLIENT_CASH_RECEIVED_{year}: {e}")
                            self.update_stats['motherduck_errors'].append(f"CLIENT_CASH_RECEIVED_{year}: {e}")
            
            # Create combined CLIENT_CASH_RECEIVED table
            combined_summary = create_client_cash_received_combined(self.conn)
            logger.info(f"  ✅ [LOCAL] CLIENT_CASH_RECEIVED combined: {combined_summary['total_rows']:,} rows, ₦{combined_summary['total_amount']:,.0f}")
                
            # Sync combined to MotherDuck
            if self.update_motherduck and self.md_conn:
                try:
                    schema = self.SCHEMA_NAME
                    try:
                        self.md_conn.execute("DETACH local_db")
                    except Exception:
                        pass
                    self.md_conn.execute(f"ATTACH '{self.db_path}' AS local_db (READ_ONLY)")
                    self.md_conn.execute(f'DROP TABLE IF EXISTS "{schema}"."CLIENT_CASH_RECEIVED"')
                    self.md_conn.execute(f'CREATE TABLE "{schema}"."CLIENT_CASH_RECEIVED" AS SELECT * FROM local_db."{schema}"."CLIENT_CASH_RECEIVED"')
                    self.md_conn.execute("DETACH local_db")
                    logger.info(f"  ✅ [MOTHERDUCK] CLIENT_CASH_RECEIVED combined: {combined_summary['total_rows']:,} rows")
                except Exception as e:
                    logger.error(f"  ❌ [MOTHERDUCK] Failed to sync CLIENT_CASH_RECEIVED: {e}")
                    self.update_stats['motherduck_errors'].append(f"CLIENT_CASH_RECEIVED: {e}")
            
            # Validate CLIENT_CASH_RECEIVED tables
            if validate_client_cash_received_tables(self.conn):
                logger.info("  ✅ [LOCAL] CLIENT_CASH_RECEIVED tables validation passed")
            else:
                logger.error("  ❌ [LOCAL] CLIENT_CASH_RECEIVED tables validation failed")
                raise Exception("CLIENT_CASH_RECEIVED tables validation failed")

            # In motherduck_only mode with MotherDuck enabled, sync from local if available
            if self.motherduck_only and self.update_motherduck and self.md_conn:
                logger.info("📊 Syncing CLIENT_CASH_RECEIVED tables from local...")
                try:
                    schema = self.SCHEMA_NAME
                    self.md_conn.execute(f"ATTACH '{self.db_path}' AS local_db (READ_ONLY)")
                    for year in [2023, 2024, 2025, 2026]:
                        self.md_conn.execute(f'DROP TABLE IF EXISTS "{schema}"."CLIENT_CASH_RECEIVED_{year}"')
                        self.md_conn.execute(f'CREATE TABLE "{schema}"."CLIENT_CASH_RECEIVED_{year}" AS SELECT * FROM local_db."{schema}"."CLIENT_CASH_RECEIVED_{year}"')
                        count = self.md_conn.execute(f'SELECT COUNT(*) FROM "{schema}"."CLIENT_CASH_RECEIVED_{year}"').fetchone()[0]
                        logger.info(f"  ✅ [MOTHERDUCK] CLIENT_CASH_RECEIVED_{year}: {count:,} rows (synced from local)")
                    self.md_conn.execute(f'DROP TABLE IF EXISTS "{schema}"."CLIENT_CASH_RECEIVED"')
                    self.md_conn.execute(f'CREATE TABLE "{schema}"."CLIENT_CASH_RECEIVED" AS SELECT * FROM local_db."{schema}"."CLIENT_CASH_RECEIVED"')
                    self.md_conn.execute("DETACH local_db")
                    count = self.md_conn.execute(f'SELECT COUNT(*) FROM "{schema}"."CLIENT_CASH_RECEIVED"').fetchone()[0]
                    logger.info(f"  ✅ [MOTHERDUCK] CLIENT_CASH_RECEIVED combined: {count:,} rows (synced from local)")
                except Exception as e:
                    logger.warning(f"  ⚠️ [MOTHERDUCK] Could not sync CLIENT_CASH_RECEIVED from local: {e}")
            
            # Update SALARY_AND_PALLIATIVE tables
            if not self.motherduck_only and self.conn:
                logger.info("📊 Updating SALARY_AND_PALLIATIVE tables...")
                for year in [2023, 2024, 2025, 2026]:
                    summary = create_salary_and_palliative_year(self.conn, year)
                    logger.info(f"  ✅ [LOCAL] SALARY_AND_PALLIATIVE_{year}: {summary['total_rows']:,} rows, ₦{summary['total_amount']:,.0f}")
                    
                    # Sync to MotherDuck
                    if self.update_motherduck and self.md_conn:
                        try:
                            schema = self.SCHEMA_NAME
                            try:
                                self.md_conn.execute("DETACH local_db")
                            except Exception:
                                pass
                            self.md_conn.execute(f"ATTACH '{self.db_path}' AS local_db (READ_ONLY)")
                            self.md_conn.execute(f'DROP TABLE IF EXISTS "{schema}"."SALARY_AND_PALLIATIVE_{year}"')
                            self.md_conn.execute(f'CREATE TABLE "{schema}"."SALARY_AND_PALLIATIVE_{year}" AS SELECT * FROM local_db."{schema}"."SALARY_AND_PALLIATIVE_{year}"')
                            self.md_conn.execute("DETACH local_db")
                            logger.info(f"  ✅ [MOTHERDUCK] SALARY_AND_PALLIATIVE_{year}: {summary['total_rows']:,} rows")
                        except Exception as e:
                            logger.error(f"  ❌ [MOTHERDUCK] Failed to sync SALARY_AND_PALLIATIVE_{year}: {e}")
                            self.update_stats['motherduck_errors'].append(f"SALARY_AND_PALLIATIVE_{year}: {e}")
            
            # Create combined SALARY_AND_PALLIATIVE table
            combined_summary = create_salary_and_palliative_combined(self.conn)
            logger.info(f"  ✅ [LOCAL] SALARY_AND_PALLIATIVE combined: {combined_summary['total_rows']:,} rows, ₦{combined_summary['total_amount']:,.0f}")
                
            # Sync combined to MotherDuck
            if self.update_motherduck and self.md_conn:
                try:
                    schema = self.SCHEMA_NAME
                    try:
                        self.md_conn.execute("DETACH local_db")
                    except Exception:
                        pass
                    self.md_conn.execute(f"ATTACH '{self.db_path}' AS local_db (READ_ONLY)")
                    self.md_conn.execute(f'DROP TABLE IF EXISTS "{schema}"."SALARY_AND_PALLIATIVE"')
                    self.md_conn.execute(f'CREATE TABLE "{schema}"."SALARY_AND_PALLIATIVE" AS SELECT * FROM local_db."{schema}"."SALARY_AND_PALLIATIVE"')
                    self.md_conn.execute("DETACH local_db")
                    logger.info(f"  ✅ [MOTHERDUCK] SALARY_AND_PALLIATIVE combined: {combined_summary['total_rows']:,} rows")
                except Exception as e:
                        logger.error(f"  ❌ [MOTHERDUCK] Failed to sync SALARY_AND_PALLIATIVE: {e}")
                        self.update_stats['motherduck_errors'].append(f"SALARY_AND_PALLIATIVE: {e}")
            
            # Validate SALARY_AND_PALLIATIVE tables
            if validate_salary_and_palliative_tables(self.conn):
                logger.info("  ✅ [LOCAL] SALARY_AND_PALLIATIVE tables validation passed")
            else:
                logger.error("  ❌ [LOCAL] SALARY_AND_PALLIATIVE tables validation failed")
                raise Exception("SALARY_AND_PALLIATIVE tables validation failed")

            # In motherduck_only mode, sync SALARY_AND_PALLIATIVE tables from local if available
            if self.motherduck_only and self.update_motherduck and self.md_conn:
                logger.info("📊 Syncing SALARY_AND_PALLIATIVE tables from local...")
                try:
                    schema = self.SCHEMA_NAME
                    try:
                        self.md_conn.execute("DETACH local_db")
                    except Exception:
                        pass
                    self.md_conn.execute(f"ATTACH '{self.db_path}' AS local_db (READ_ONLY)")
                    for year in [2023, 2024, 2025, 2026]:
                        self.md_conn.execute(f'DROP TABLE IF EXISTS "{schema}"."SALARY_AND_PALLIATIVE_{year}"')
                        self.md_conn.execute(f'CREATE TABLE "{schema}"."SALARY_AND_PALLIATIVE_{year}" AS SELECT * FROM local_db."{schema}"."SALARY_AND_PALLIATIVE_{year}"')
                        count = self.md_conn.execute(f'SELECT COUNT(*) FROM "{schema}"."SALARY_AND_PALLIATIVE_{year}"').fetchone()[0]
                        logger.info(f"  ✅ [MOTHERDUCK] SALARY_AND_PALLIATIVE_{year}: {count:,} rows (synced from local)")
                    self.md_conn.execute(f'DROP TABLE IF EXISTS "{schema}"."SALARY_AND_PALLIATIVE"')
                    self.md_conn.execute(f'CREATE TABLE "{schema}"."SALARY_AND_PALLIATIVE" AS SELECT * FROM local_db."{schema}"."SALARY_AND_PALLIATIVE"')
                    self.md_conn.execute("DETACH local_db")
                    count = self.md_conn.execute(f'SELECT COUNT(*) FROM "{schema}"."SALARY_AND_PALLIATIVE"').fetchone()[0]
                    logger.info(f"  ✅ [MOTHERDUCK] SALARY_AND_PALLIATIVE combined: {count:,} rows (synced from local)")
                except Exception as e:
                    logger.warning(f"  ⚠️ [MOTHERDUCK] Could not sync SALARY_AND_PALLIATIVE from local: {e}")
            
            # Update EXPENSE_AND_COMMISSION tables
            if not self.motherduck_only and self.conn:
                logger.info("📊 Updating EXPENSE_AND_COMMISSION tables...")
                for year in [2023, 2024, 2025, 2026]:
                    summary = create_expense_and_commission_year(self.conn, year)
                    logger.info(f"  ✅ [LOCAL] EXPENSE_AND_COMMISSION_{year}: {summary['total_rows']:,} rows, ₦{summary['total_amount']:,.0f}")
                    
                    # Sync to MotherDuck
                    if self.update_motherduck and self.md_conn:
                        try:
                            schema = self.SCHEMA_NAME
                            try:
                                self.md_conn.execute("DETACH local_db")
                            except Exception:
                                pass
                            self.md_conn.execute(f"ATTACH '{self.db_path}' AS local_db (READ_ONLY)")
                            self.md_conn.execute(f'DROP TABLE IF EXISTS "{schema}"."EXPENSE_AND_COMMISSION_{year}"')
                            self.md_conn.execute(f'CREATE TABLE "{schema}"."EXPENSE_AND_COMMISSION_{year}" AS SELECT * FROM local_db."{schema}"."EXPENSE_AND_COMMISSION_{year}"')
                            self.md_conn.execute("DETACH local_db")
                            logger.info(f"  ✅ [MOTHERDUCK] EXPENSE_AND_COMMISSION_{year}: {summary['total_rows']:,} rows")
                        except Exception as e:
                            logger.error(f"  ❌ [MOTHERDUCK] Failed to sync EXPENSE_AND_COMMISSION_{year}: {e}")
                            self.update_stats['motherduck_errors'].append(f"EXPENSE_AND_COMMISSION_{year}: {e}")
            
            # Create combined EXPENSE_AND_COMMISSION table
            combined_summary = create_expense_and_commission_combined(self.conn)
            logger.info(f"  ✅ [LOCAL] EXPENSE_AND_COMMISSION combined: {combined_summary['total_rows']:,} rows, ₦{combined_summary['total_amount']:,.0f}")
                
            # Sync combined to MotherDuck
            if self.update_motherduck and self.md_conn:
                try:
                    schema = self.SCHEMA_NAME
                    try:
                        self.md_conn.execute("DETACH local_db")
                    except Exception:
                        pass
                    self.md_conn.execute(f"ATTACH '{self.db_path}' AS local_db (READ_ONLY)")
                    self.md_conn.execute(f'DROP TABLE IF EXISTS "{schema}"."EXPENSE_AND_COMMISSION"')
                    self.md_conn.execute(f'CREATE TABLE "{schema}"."EXPENSE_AND_COMMISSION" AS SELECT * FROM local_db."{schema}"."EXPENSE_AND_COMMISSION"')
                    self.md_conn.execute("DETACH local_db")
                    logger.info(f"  ✅ [MOTHERDUCK] EXPENSE_AND_COMMISSION combined: {combined_summary['total_rows']:,} rows")
                except Exception as e:
                    logger.error(f"  ❌ [MOTHERDUCK] Failed to sync EXPENSE_AND_COMMISSION: {e}")
                    self.update_stats['motherduck_errors'].append(f"EXPENSE_AND_COMMISSION: {e}")
            
            # Validate EXPENSE_AND_COMMISSION tables
            if validate_expense_and_commission_tables(self.conn):
                logger.info("  ✅ [LOCAL] EXPENSE_AND_COMMISSION tables validation passed")
            else:
                logger.error("  ❌ [LOCAL] EXPENSE_AND_COMMISSION tables validation failed")
                raise Exception("EXPENSE_AND_COMMISSION tables validation failed")

            # In motherduck_only mode, sync EXPENSE_AND_COMMISSION tables from local if available
            if self.motherduck_only and self.update_motherduck and self.md_conn:
                logger.info("📊 Syncing EXPENSE_AND_COMMISSION tables from local...")
                try:
                    schema = self.SCHEMA_NAME
                    try:
                        self.md_conn.execute("DETACH local_db")
                    except Exception:
                        pass
                    self.md_conn.execute(f"ATTACH '{self.db_path}' AS local_db (READ_ONLY)")
                    for year in [2023, 2024, 2025, 2026]:
                        self.md_conn.execute(f'DROP TABLE IF EXISTS "{schema}"."EXPENSE_AND_COMMISSION_{year}"')
                        self.md_conn.execute(f'CREATE TABLE "{schema}"."EXPENSE_AND_COMMISSION_{year}" AS SELECT * FROM local_db."{schema}"."EXPENSE_AND_COMMISSION_{year}"')
                        count = self.md_conn.execute(f'SELECT COUNT(*) FROM "{schema}"."EXPENSE_AND_COMMISSION_{year}"').fetchone()[0]
                        logger.info(f"  ✅ [MOTHERDUCK] EXPENSE_AND_COMMISSION_{year}: {count:,} rows (synced from local)")
                    self.md_conn.execute(f'DROP TABLE IF EXISTS "{schema}"."EXPENSE_AND_COMMISSION"')
                    self.md_conn.execute(f'CREATE TABLE "{schema}"."EXPENSE_AND_COMMISSION" AS SELECT * FROM local_db."{schema}"."EXPENSE_AND_COMMISSION"')
                    self.md_conn.execute("DETACH local_db")
                    count = self.md_conn.execute(f'SELECT COUNT(*) FROM "{schema}"."EXPENSE_AND_COMMISSION"').fetchone()[0]
                    logger.info(f"  ✅ [MOTHERDUCK] EXPENSE_AND_COMMISSION combined: {count:,} rows (synced from local)")
                except Exception as e:
                    logger.warning(f"  ⚠️ [MOTHERDUCK] Could not sync EXPENSE_AND_COMMISSION from local: {e}")
            
            # Client dashboard summary (for API to read from; avoids 502 on slow hosts)
            try:
                from api.routes.clients import populate_client_dashboard_summary_table
                logger.info("📊 Populating CLIENT_DASHBOARD_SUMMARY...")
                if self.motherduck_only and self.update_motherduck and self.md_conn:
                    # Render cron etc.: populate writes directly to MotherDuck via get_db_connection()
                    success, rows, err = populate_client_dashboard_summary_table()
                    if success:
                        logger.info(f"  ✅ [MOTHERDUCK] CLIENT_DASHBOARD_SUMMARY: {rows:,} rows")
                    else:
                        logger.warning(f"  ⚠️ CLIENT_DASHBOARD_SUMMARY populate failed: {err}")
                elif not self.motherduck_only and self.conn:
                    success, rows, err = populate_client_dashboard_summary_table()
                    if success:
                        logger.info(f"  ✅ [LOCAL] CLIENT_DASHBOARD_SUMMARY: {rows:,} rows")
                        if self.update_motherduck and self.md_conn:
                            try:
                                schema = self.SCHEMA_NAME
                                try:
                                    self.md_conn.execute("DETACH local_db")
                                except Exception:
                                    pass
                                self.md_conn.execute(f"ATTACH '{self.db_path}' AS local_db (READ_ONLY)")
                                self.md_conn.execute(f'DROP TABLE IF EXISTS "{schema}"."CLIENT_DASHBOARD_SUMMARY"')
                                self.md_conn.execute(f'CREATE TABLE "{schema}"."CLIENT_DASHBOARD_SUMMARY" AS SELECT * FROM local_db."{schema}"."CLIENT_DASHBOARD_SUMMARY"')
                                self.md_conn.execute("DETACH local_db")
                                count = self.md_conn.execute(f'SELECT COUNT(*) FROM "{schema}"."CLIENT_DASHBOARD_SUMMARY"').fetchone()[0]
                                logger.info(f"  ✅ [MOTHERDUCK] CLIENT_DASHBOARD_SUMMARY: {count:,} rows")
                            except Exception as e:
                                logger.error(f"  ❌ [MOTHERDUCK] Failed to sync CLIENT_DASHBOARD_SUMMARY: {e}")
                                self.update_stats['motherduck_errors'].append(f"CLIENT_DASHBOARD_SUMMARY: {e}")
                    else:
                        logger.warning(f"  ⚠️ CLIENT_DASHBOARD_SUMMARY populate failed: {err}")
                else:
                    logger.debug("  ⏭️ CLIENT_DASHBOARD_SUMMARY skipped (no conn)")
            except Exception as e:
                logger.warning(f"  ⚠️ CLIENT_DASHBOARD_SUMMARY skipped: {e}")
            
            logger.info("✅ All derived tables updated successfully!")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to update derived tables: {e}")
            self.update_stats['errors'].append(f"Derived tables: {e}")
            return False
    
    def cleanup_old_data(self, keep_years=2):
        """
        Remove data older than specified years from large tables.
        Keeps only the most recent N years of data.
        
        Args:
            keep_years: Number of years to keep (default: 2)
        """
        try:
            current_year = datetime.now().year
            cutoff_date = f'{current_year - keep_years + 1}-01-01'
            
            logger.info(f"🧹 Cleaning up data older than {keep_years} years (keeping from {cutoff_date} onwards)...")
            
            if not self.motherduck_only and self.conn:
                schema = self.SCHEMA_NAME
                
                # Clean PA DATA
                logger.info(f"🧹 Cleaning PA DATA (removing data before {cutoff_date})...")
                before_count = self.conn.execute(f'SELECT COUNT(*) FROM "{schema}"."PA DATA"').fetchone()[0]
                self.conn.execute(f'''
                    DELETE FROM "{schema}"."PA DATA"
                    WHERE requestdate < DATE '{cutoff_date}'
                ''')
                after_count = self.conn.execute(f'SELECT COUNT(*) FROM "{schema}"."PA DATA"').fetchone()[0]
                removed = before_count - after_count
                logger.info(f"  ✅ PA DATA: Removed {removed:,} old rows ({before_count:,} → {after_count:,})")
                
                # Clean CLAIMS DATA
                logger.info(f"🧹 Cleaning CLAIMS DATA (removing data before {cutoff_date})...")
                before_count = self.conn.execute(f'SELECT COUNT(*) FROM "{schema}"."CLAIMS DATA"').fetchone()[0]
                self.conn.execute(f'''
                    DELETE FROM "{schema}"."CLAIMS DATA"
                    WHERE datesubmitted < DATE '{cutoff_date}'
                ''')
                after_count = self.conn.execute(f'SELECT COUNT(*) FROM "{schema}"."CLAIMS DATA"').fetchone()[0]
                removed = before_count - after_count
                logger.info(f"  ✅ CLAIMS DATA: Removed {removed:,} old rows ({before_count:,} → {after_count:,})")
                
                # Update metadata
                pa_count = self.conn.execute(f'SELECT COUNT(*) FROM "{schema}"."PA DATA"').fetchone()[0]
                claims_count = self.conn.execute(f'SELECT COUNT(*) FROM "{schema}"."CLAIMS DATA"').fetchone()[0]
                
                if pa_count > 0:
                    max_pa_date = self.conn.execute(f'SELECT MAX(requestdate) FROM "{schema}"."PA DATA"').fetchone()[0]
                    if max_pa_date:
                        self._update_metadata(self.conn, 'PA DATA', max_pa_date, pa_count)
                
                if claims_count > 0:
                    max_claims_date = self.conn.execute(f'SELECT MAX(datesubmitted) FROM "{schema}"."CLAIMS DATA"').fetchone()[0]
                    if max_claims_date:
                        self._update_metadata(self.conn, 'CLAIMS DATA', max_claims_date, claims_count)
                
                # Sync cleaned data to MotherDuck
                if self.update_motherduck and self.md_conn:
                    logger.info("☁️  Syncing cleaned data to MotherDuck...")
                    try:
                        self.md_conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
                        self.md_conn.execute(f"ATTACH '{self.db_path}' AS local_db (READ_ONLY)")
                        
                        # Sync PA DATA
                        self.md_conn.execute(f'DROP TABLE IF EXISTS "{schema}"."PA DATA"')
                        self.md_conn.execute(f'CREATE TABLE "{schema}"."PA DATA" AS SELECT * FROM local_db."{schema}"."PA DATA"')
                        pa_count = self.md_conn.execute(f'SELECT COUNT(*) FROM "{schema}"."PA DATA"').fetchone()[0]
                        logger.info(f"  ✅ [MOTHERDUCK] PA DATA: {pa_count:,} rows")
                        
                        # Sync CLAIMS DATA
                        self.md_conn.execute(f'DROP TABLE IF EXISTS "{schema}"."CLAIMS DATA"')
                        self.md_conn.execute(f'CREATE TABLE "{schema}"."CLAIMS DATA" AS SELECT * FROM local_db."{schema}"."CLAIMS DATA"')
                        claims_count = self.md_conn.execute(f'SELECT COUNT(*) FROM "{schema}"."CLAIMS DATA"').fetchone()[0]
                        logger.info(f"  ✅ [MOTHERDUCK] CLAIMS DATA: {claims_count:,} rows")
                        
                        self.md_conn.execute("DETACH local_db")
                    except Exception as e:
                        logger.error(f"  ❌ Failed to sync cleaned data to MotherDuck: {e}")
                
                logger.info("✅ Cleanup completed successfully!")
                return True
            else:
                logger.warning("⚠️ Cleanup skipped (motherduck_only mode or no local connection)")
                return False
                
        except Exception as e:
            logger.error(f"❌ Cleanup failed: {e}")
            return False
    
    def update_nhia_schema(self):
        """
        Update NHIA schema from MongoDB for both local DuckDB and MotherDuck
        Fetches nhisprocedures, nhiaenrollees, medicaldiagnoses, and flattened requests
        """
        try:
            logger.info("🔄 Updating NHIA schema from MongoDB...")
            
            # Step 1: Fetch collections from MongoDB (excluding requests)
            logger.info("📊 Fetching collections from MongoDB...")
            collections_to_fetch = ['nhisprocedures', 'nhiaenrollees', 'medicaldiagnoses']
            dfs = fetch_collections_as_dataframes(collections=collections_to_fetch)
            
            # Step 2: Get flattened requests
            logger.info("📊 Fetching and flattening requests...")
            requests_flattened = get_requests_with_populated_procedures(limit=None)  # Get all requests
            dfs['requests'] = requests_flattened
            
            # Step 3: Update local DuckDB NHIA schema (if not motherduck_only)
            if not self.motherduck_only and self.conn:
                logger.info("💾 Updating local DuckDB NHIA schema...")
                create_nhia_schema(self.conn)
                
                # Define table mapping (collection name -> table name)
                table_mapping = {
                    'nhisprocedures': 'nhisprocedures',
                    'nhiaenrollees': 'nhiaenrollees',
                    'medicaldiagnoses': 'medicaldiagnoses',
                    'requests': 'requests'  # This is the flattened requests table
                }
                
                for collection_name, table_name in table_mapping.items():
                    if collection_name in dfs:
                        df = dfs[collection_name]
                        logger.info(f"  📤 Pushing {collection_name} -> {table_name} to local DuckDB...")
                        push_dataframe_to_duckdb(df, table_name, self.conn, schema='NHIA', replace=True)
                        self.update_stats['tables_updated'] += 1
                        self.update_stats['total_rows_updated'] += len(df)
                    else:
                        logger.warning(f"  ⚠️ {collection_name} not found in fetched data")
            
            # Step 4: Update MotherDuck NHIA schema (if enabled)
            if self.update_motherduck and self.md_conn:
                logger.info("☁️  Updating MotherDuck NHIA schema...")
                
                # Create NHIA schema in MotherDuck if it doesn't exist
                self.md_conn.execute('CREATE SCHEMA IF NOT EXISTS "NHIA"')
                
                # Attach local database for efficient copying (if local was updated)
                if not self.motherduck_only and self.conn:
                    # Sync from local DuckDB (more efficient)
                    logger.info("  📤 Syncing NHIA tables from local to MotherDuck...")
                    try:
                        self.md_conn.execute("DETACH local_db")
                    except Exception:
                        pass
                    self.md_conn.execute(f"ATTACH '{self.db_path}' AS local_db (READ_ONLY)")
                    
                    table_mapping = {
                        'nhisprocedures': 'nhisprocedures',
                        'nhiaenrollees': 'nhiaenrollees',
                        'medicaldiagnoses': 'medicaldiagnoses',
                        'requests': 'requests'
                    }
                    
                    for collection_name, table_name in table_mapping.items():
                        try:
                            logger.info(f"  ⏳ Syncing {table_name} to MotherDuck...")
                            
                            # Drop table if exists (for clean upload)
                            self.md_conn.execute(f'DROP TABLE IF EXISTS "NHIA"."{table_name}"')
                            
                            # Copy table structure and data from local to MotherDuck
                            self.md_conn.execute(f'CREATE TABLE "NHIA"."{table_name}" AS SELECT * FROM local_db."NHIA"."{table_name}"')
                            
                            # Get row count to verify
                            md_count = self.md_conn.execute(f'SELECT COUNT(*) FROM "NHIA"."{table_name}"').fetchone()[0]
                            logger.info(f"  ✅ [MOTHERDUCK] {table_name}: {md_count:,} rows")
                            
                        except Exception as e:
                            logger.error(f"  ❌ [MOTHERDUCK] Failed to sync {table_name}: {e}")
                            self.update_stats['motherduck_errors'].append(f"NHIA.{table_name}: {e}")
                    
                    self.md_conn.execute("DETACH local_db")
                else:
                    # In motherduck_only mode, push directly from DataFrames
                    logger.info("  📤 Pushing NHIA tables directly to MotherDuck...")
                    
                    table_mapping = {
                        'nhisprocedures': 'nhisprocedures',
                        'nhiaenrollees': 'nhiaenrollees',
                        'medicaldiagnoses': 'medicaldiagnoses',
                        'requests': 'requests'
                    }
                    
                    for collection_name, table_name in table_mapping.items():
                        if collection_name in dfs:
                            df = dfs[collection_name]
                            try:
                                logger.info(f"  ⏳ Pushing {table_name} to MotherDuck...")
                                
                                # Use the same push_dataframe_to_duckdb function but with md_conn
                                # We need to create a temporary connection wrapper or use direct SQL
                                # For now, let's use direct SQL approach
                                
                                # Drop table if exists
                                self.md_conn.execute(f'DROP TABLE IF EXISTS "NHIA"."{table_name}"')
                                
                                # Register DataFrame and create table
                                temp_view_name = f'temp_nhia_{table_name}'
                                
                                # Convert datetime columns to strings for MotherDuck compatibility
                                df_clean = df.copy()
                                for col in df_clean.columns:
                                    if pd.api.types.is_datetime64_any_dtype(df_clean[col]):
                                        df_clean[col] = pd.to_datetime(df_clean[col], errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')
                                
                                self.md_conn.register(temp_view_name, df_clean)
                                try:
                                    self.md_conn.execute(f'CREATE TABLE "NHIA"."{table_name}" AS SELECT * FROM {temp_view_name}')
                                    md_count = self.md_conn.execute(f'SELECT COUNT(*) FROM "NHIA"."{table_name}"').fetchone()[0]
                                    logger.info(f"  ✅ [MOTHERDUCK] {table_name}: {md_count:,} rows")
                                except Exception as e:
                                    logger.error(f"  ❌ [MOTHERDUCK] Failed to push {table_name}: {e}")
                                    # Try CSV fallback
                                    import tempfile
                                    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as tmp_file:
                                        csv_path = tmp_file.name
                                        df_clean.to_csv(csv_path, index=False, na_rep='')
                                    try:
                                        self.md_conn.execute(f"CREATE TABLE \"NHIA\".\"{table_name}\" AS SELECT * FROM read_csv_auto('{csv_path}')")
                                        md_count = self.md_conn.execute(f'SELECT COUNT(*) FROM "NHIA"."{table_name}"').fetchone()[0]
                                        logger.info(f"  ✅ [MOTHERDUCK] {table_name}: {md_count:,} rows (via CSV)")
                                    finally:
                                        try:
                                            os.unlink(csv_path)
                                        except:
                                            pass
                                finally:
                                    try:
                                        self.md_conn.unregister(temp_view_name)
                                    except:
                                        pass
                                        
                            except Exception as e:
                                logger.error(f"  ❌ [MOTHERDUCK] Failed to push {table_name}: {e}")
                                self.update_stats['motherduck_errors'].append(f"NHIA.{table_name}: {e}")
                        else:
                            logger.warning(f"  ⚠️ {collection_name} not found in fetched data")
            
            logger.info("✅ NHIA schema updated successfully!")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to update NHIA schema: {e}")
            self.update_stats['errors'].append(f"NHIA schema: {e}")
            return False
    
    def run_full_pa_data_reload(self):
        """
        Drop local PA DATA, clear metadata, fetch full 2-year window from MediCloud,
        and sync PA DATA to MotherDuck when enabled. Use after changing total_pa_procedures.
        """
        self.update_stats['start_time'] = datetime.now()
        logger.info('🚀 Full reload: PA DATA only (MediCloud → local DuckDB → MotherDuck if enabled)')
        if self.motherduck_only:
            logger.error('❌ --full-reload-pa-data requires a local DuckDB (do not use --motherduck-only)')
            return False
        if not self.connect():
            return False
        self._prepare_full_pa_data_reload()
        schema = self.SCHEMA_NAME
        if not self.motherduck_only and self.conn:
            self.conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
        if self.update_motherduck and self.md_conn:
            self.md_conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
        schema_sql = '''
            CREATE TABLE IF NOT EXISTS "AI DRIVEN DATA"."PA DATA" (
                panumber VARCHAR, groupname VARCHAR, divisionname VARCHAR, plancode VARCHAR,
                IID VARCHAR, providerid VARCHAR, requestdate TIMESTAMP, pastatus VARCHAR,
                code VARCHAR, userid VARCHAR, totaltariff DOUBLE, benefitcode VARCHAR,
                dependantnumber VARCHAR, quantity DOUBLE, requested DOUBLE, granted DOUBLE
            )
        '''.replace('"AI DRIVEN DATA"', f'"{schema}"')
        try:
            ok = self._smart_update_table(
                'PA DATA',
                total_pa_procedures,
                'requestdate',
                schema_sql,
            )
        finally:
            self.update_stats['end_time'] = datetime.now()
            self.disconnect()
        self.log_update_summary()
        return ok

    def update_all_tables(self):
        """Update all tables in the database"""
        self.update_stats['start_time'] = datetime.now()
        logger.info("🚀 Starting database update process...")
        
        if not self.connect():
            return False
        
        # Ensure schema exists in databases
        schema = self.SCHEMA_NAME
        if not self.motherduck_only and self.conn:
            self.conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
        if self.update_motherduck and self.md_conn:
            self.md_conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
        
        # Define table update configurations
        table_configs = [
            {
                'name': 'PA DATA',
                'loader': total_pa_procedures,
                'schema': '''
                    CREATE TABLE IF NOT EXISTS "AI DRIVEN DATA"."PA DATA" (
                        panumber VARCHAR, groupname VARCHAR, divisionname VARCHAR, plancode VARCHAR,
                        IID VARCHAR, providerid VARCHAR, requestdate TIMESTAMP, pastatus VARCHAR,
                        code VARCHAR, userid VARCHAR, totaltariff DOUBLE, benefitcode VARCHAR,
                        dependantnumber VARCHAR, quantity DOUBLE, requested DOUBLE, granted DOUBLE
                    )
                ''',
                'sample_creator': self.create_sample_pa_data
            },
            {
                'name': 'CLAIMS DATA',
                'loader': claims,
                'schema': '''
                    CREATE TABLE IF NOT EXISTS "AI DRIVEN DATA"."CLAIMS DATA" (
                        enrollee_id VARCHAR, providerid VARCHAR, groupid VARCHAR, nhisgroupid VARCHAR, 
                        nhisproviderid VARCHAR, panumber INTEGER, encounterdatefrom DATE, encounterdateto DATE,
                        datesubmitted TIMESTAMP, chargeamount DOUBLE, approvedamount DOUBLE, code VARCHAR, 
                        deniedamount DOUBLE, diagnosiscode VARCHAR, claimnumber VARCHAR, memberid INTEGER,
                        dependantnumber INTEGER, isinpatient BOOLEAN, discount DOUBLE, datereceived TIMESTAMP,
                        claimstatusid INTEGER, adjusterid VARCHAR, unitfactor INTEGER, isapproved BOOLEAN,
                        isfinal BOOLEAN, ispaid BOOLEAN, amountpaid DOUBLE, datepaid TIMESTAMP,
                        paymentbatchno VARCHAR, dateadded TIMESTAMP, claimid INTEGER, nhisdependantnumber INTEGER
                    )
                ''',
                'sample_creator': self.create_sample_claims_data
            },
            {
                'name': 'PROVIDERS',
                'loader': all_providers,
                'schema': '''
                    CREATE TABLE IF NOT EXISTS "AI DRIVEN DATA"."PROVIDERS" (
                        providerid VARCHAR, providername VARCHAR, dateadded TIMESTAMP,
                        isvisible BOOLEAN, lganame VARCHAR, statename VARCHAR, bands VARCHAR
                    )
                ''',
                'sample_creator': self.create_sample_providers_data
            },
            {
                'name': 'GROUPS',
                'loader': all_group,
                'schema': '''
                    CREATE TABLE IF NOT EXISTS "AI DRIVEN DATA"."GROUPS" (
                        groupid INTEGER, groupname VARCHAR, address1 VARCHAR, address2 VARCHAR,
                        contactlastname VARCHAR, contactfirstname VARCHAR, contactemail1 VARCHAR,
                        contactemail2 VARCHAR, contactphone1 VARCHAR, contactphone2 VARCHAR,
                        contactphone3 VARCHAR, renewaldate DATE, zipcode VARCHAR, postalcodeid INTEGER,
                        stateid INTEGER, lgaid INTEGER, countryid INTEGER, billingcycleid INTEGER,
                        billingdueday INTEGER, billinginvoiceday INTEGER, categoryid INTEGER,
                        statuscodeid INTEGER, groupemail1 VARCHAR, groupemail2 VARCHAR,
                        officephone1 VARCHAR, officephone2 VARCHAR, isgroupapproved BOOLEAN,
                        contactlastname2 VARCHAR, contactfirstname2 VARCHAR, comments VARCHAR,
                        logopath VARCHAR, groupeffectivedate DATE, groupterminationdate DATE,
                        approvalpending BOOLEAN, sagecode VARCHAR, dateadded TIMESTAMP,
                        useplanfilter BOOLEAN, exclusionmargin DOUBLE, iscapitated BOOLEAN
                    )
                ''',
                'sample_creator': self.create_sample_groups_data
            },
            {
                'name': 'MEMBERS',
                'loader': all_active_member,
                'schema': '''
                    CREATE TABLE IF NOT EXISTS "AI DRIVEN DATA"."MEMBERS" (
                        memberid VARCHAR, groupid INTEGER, enrollee_id VARCHAR, planid VARCHAR,
                        iscurrent BOOLEAN, isterminated BOOLEAN, registrationdate TIMESTAMP,
                        effectivedate TIMESTAMP, terminationdate TIMESTAMP
                    )
                ''',
                'sample_creator': self.create_sample_members_data
            },
            {
                'name': 'BENEFITCODES',
                'loader': benefitcode,
                'schema': '''
                    CREATE TABLE IF NOT EXISTS "AI DRIVEN DATA"."BENEFITCODES" (
                        benefitcodeid INTEGER, benefitcodename VARCHAR, benefitcodedesc VARCHAR,
                        benefitlevelid INTEGER, dateadded TIMESTAMP
                    )
                ''',
                'sample_creator': self.create_sample_benefitcode_data
            },
            {
                'name': 'BENEFITCODE_PROCEDURES',
                'loader': benefitcode_procedure,
                'schema': '''
                    CREATE TABLE IF NOT EXISTS "AI DRIVEN DATA"."BENEFITCODE_PROCEDURES" (
                        benefitcodeid INTEGER, procedurecode VARCHAR, benefitlevelid INTEGER,
                        dateadded TIMESTAMP, iscurrent BOOLEAN
                    )
                ''',
                'sample_creator': self.create_sample_benefitcode_procedure_data
            },
            {
                'name': 'GROUP_PLANS',
                'loader': group_plan,
                'schema': '''
                    CREATE TABLE IF NOT EXISTS "AI DRIVEN DATA"."GROUP_PLANS" (
                        grouplanid INTEGER, groupid INTEGER, planid INTEGER, comments VARCHAR,
                        effectivedate DATE, terminationdate DATE, planlimit DOUBLE, inelcodeid INTEGER,
                        principalmaxage INTEGER, dependantmaxage INTEGER, dependantminage INTEGER,
                        principalminage INTEGER, iscurrent BOOLEAN, countofindividual INTEGER,
                        countoffamily INTEGER, individualprice DOUBLE, familyprice DOUBLE,
                        maxnumdependant INTEGER, dateadded TIMESTAMP, grouplegacycode VARCHAR,
                        plancode VARCHAR, endorsementid INTEGER, isFamilyLimit BOOLEAN
                    )
                ''',
                'sample_creator': self.create_sample_group_plan_data
            },
            {
                'name': 'PA ISSUE REQUEST',
                'loader': pa_issue_request,
                'schema': '''
                    CREATE TABLE IF NOT EXISTS "AI DRIVEN DATA"."PA ISSUE REQUEST" (
                        panumber VARCHAR, groupname VARCHAR, divisionname VARCHAR, plancode VARCHAR,
                        IID VARCHAR, providerid VARCHAR, requestdate TIMESTAMP, pastatus VARCHAR,
                        code VARCHAR, userid VARCHAR, totaltariff DOUBLE, benefitcode VARCHAR,
                        dependantnumber VARCHAR, requested DOUBLE, granted DOUBLE, resolutiontime TIMESTAMP
                    )
                ''',
                'sample_creator': None
            },
            {
                'name': 'PROCEDURE DATA',
                'loader': proceduredata,
                'schema': '''
                    CREATE TABLE IF NOT EXISTS "AI DRIVEN DATA"."PROCEDURE DATA" (
                        procedurecode VARCHAR, proceduredesc VARCHAR
                    )
                ''',
                'sample_creator': None
            },
            {
                'name': 'E_ACCOUNT_GROUP',
                'loader': e_account_group,
                'schema': '''
                    CREATE TABLE IF NOT EXISTS "AI DRIVEN DATA"."E_ACCOUNT_GROUP" (
                        groupid INTEGER, ID_Company INTEGER, groupname VARCHAR, address1 VARCHAR,
                        address2 VARCHAR, CompCode VARCHAR, AgentCode VARCHAR
                    )
                ''',
                'sample_creator': None
            },
            {
                'name': 'DEBIT_NOTE',
                'loader': debit_note,
                'schema': '''
                    CREATE TABLE IF NOT EXISTS "AI DRIVEN DATA"."DEBIT_NOTE" (
                        DRNT BIGINT, RefNo VARCHAR, DBRef INTEGER, CompanyName VARCHAR,
                        PersonName VARCHAR, Address VARCHAR, PolicyNo INTEGER, Agent VARCHAR,
                        Class VARCHAR, "From" DATE, "To" DATE, Insured INTEGER, Description VARCHAR,
                        Amount DOUBLE, Date DATE, Preparedby VARCHAR, Checkby VARCHAR,
                        SignID VARCHAR, PaymentMode INTEGER
                    )
                ''',
                'sample_creator': None
            },
            {
                'name': 'GROUP_CONTRACT',
                'loader': group_contract,
                'schema': '''
                    CREATE TABLE IF NOT EXISTS "AI DRIVEN DATA"."GROUP_CONTRACT" (
                        groupid INTEGER, startdate TIMESTAMP, enddate TIMESTAMP,
                        iscurrent BOOLEAN, groupname VARCHAR
                    )
                ''',
                'sample_creator': None
            }
        ]

        # Add additional source tables to DuckDB (created dynamically from source data)
        table_configs.extend([
            { 'name': 'TARIFF', 'loader': tariff, 'schema': '' },
            { 'name': 'GROUP_COVERAGE', 'loader': group_coverage, 'schema': '' },
            { 'name': 'MEMBER_PLANS', 'loader': member_plans, 'schema': '' },
            { 'name': 'PLANBENEFITCODE_LIMIT', 'loader': planbenefitcode_limit, 'schema': '' },
            { 'name': 'PLANS', 'loader': plans, 'schema': '' },
            { 'name': 'GROUP_INVOICE', 'loader': group_invoice, 'schema': '' },
            { 'name': 'PREMIUM1_SCHEDULE', 'loader': premium1_schedule, 'schema': '' },
            { 'name': 'FIN_ACCSETUP', 'loader': fin_accsetup, 'schema': '' },
            { 'name': 'PROVIDERS_TARIFF', 'loader': providers_tariff, 'schema': '' },
            { 'name': 'MEMBER_PROVIDER', 'loader': member_provider, 'schema': '' },
            { 'name': 'DIAGNOSIS', 'loader': diagnosis, 'schema': '' },
            { 'name': 'MEMBER', 'loader': member, 'schema': '' },
            { 'name': 'MEMBER_COVERAGE', 'loader': member_coverage, 'schema': '' },
        ])
        
        # Update each table
        schema = self.SCHEMA_NAME
        for config in table_configs:
            # Ensure table schema exists if provided
            schema_sql = config.get('schema')
            if schema_sql:
                # Replace hardcoded schema name with constant
                schema_sql = schema_sql.replace('"AI DRIVEN DATA"', f'"{schema}"')
                # Execute in local (if not motherduck_only)
                if not self.motherduck_only and self.conn:
                    self.conn.execute(schema_sql)
                # Also execute in MotherDuck if enabled
                if self.update_motherduck and self.md_conn:
                    try:
                        self.md_conn.execute(schema_sql)
                    except Exception:
                        pass  # Schema might already exist
            
            # Update table data
            # Enable smart updates for large tables
            date_column = None
            if config['name'] == 'PA DATA':
                date_column = 'requestdate'
            elif config['name'] == 'CLAIMS DATA':
                date_column = 'datesubmitted'
            
            success = self.update_table(
                config['name'],
                config['loader'],
                schema_sql if schema_sql else '',
                config.get('sample_creator'),
                enable_smart_update=True,
                date_column=date_column
            )
            
            if not success:
                logger.warning(f"⚠️ Failed to update {config['name']}")
        
        # Update FIN_GL raw tables (skip if motherduck_only, as it needs local DB for Excel/EACCOUNT)
        if not self.motherduck_only:
            logger.info("🔄 Updating FIN_GL raw tables...")
            fin_gl_success = self.update_fin_gl_tables()
            if not fin_gl_success:
                logger.warning("⚠️ Some FIN_GL tables failed to update")
        elif self.motherduck_only:
            # In motherduck_only mode, try to sync FIN_GL from local if available
            logger.info("🔄 Syncing FIN_GL raw tables from local...")
            try:
                schema = self.SCHEMA_NAME
                if self.update_motherduck and self.md_conn:
                    self.md_conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
                    try:
                        self.md_conn.execute("DETACH local_db")
                    except Exception:
                        pass
                    self.md_conn.execute(f"ATTACH '{self.db_path}' AS local_db (READ_ONLY)")
                    for year in [2023, 2024, 2025]:
                        table_name = f"FIN_GL_{year}_RAW"
                        try:
                            self.md_conn.execute(f'DROP TABLE IF EXISTS "{schema}"."{table_name}"')
                            self.md_conn.execute(f'CREATE TABLE "{schema}"."{table_name}" AS SELECT * FROM local_db."{schema}"."{table_name}"')
                            count = self.md_conn.execute(f'SELECT COUNT(*) FROM "{schema}"."{table_name}"').fetchone()[0]
                            logger.info(f"  ✅ [MOTHERDUCK] {table_name}: {count:,} rows (synced from local)")
                        except Exception as e:
                            logger.warning(f"  ⚠️ [MOTHERDUCK] Could not sync {table_name}: {e}")
                    self.md_conn.execute("DETACH local_db")
            except Exception as e:
                logger.warning(f"⚠️ Could not sync FIN_GL tables from local: {e}")
        
        # Update derived tables after basic tables are updated
        logger.info("🔄 Updating derived tables...")
        derived_success = self.update_derived_tables()
        if not derived_success:
            logger.warning("⚠️ Some derived tables failed to update")
        
        # Update NHIA schema from MongoDB
        logger.info("🔄 Updating NHIA schema from MongoDB...")
        nhia_success = self.update_nhia_schema()
        if not nhia_success:
            logger.warning("⚠️ NHIA schema update failed")
        
        self.update_stats['end_time'] = datetime.now()
        self.log_update_summary()
        
        self.disconnect()
        return True
    
    def log_update_summary(self):
        """Log the update summary"""
        duration = self.update_stats['end_time'] - self.update_stats['start_time']
        
        logger.info("=" * 60)
        logger.info("📊 DATABASE UPDATE SUMMARY")
        logger.info("=" * 60)
        logger.info(f"⏱️  Duration: {duration}")
        logger.info(f"📋 Tables Updated: {self.update_stats['tables_updated']}")
        logger.info(f"📈 Total Rows Updated: {self.update_stats['total_rows_updated']:,}")
        
        if self.update_motherduck:
            logger.info(f"☁️  MotherDuck: {'✅ Enabled' if self.md_conn else '❌ Disabled'}")
        
        if self.update_stats['local_errors']:
            logger.info(f"❌ Local Errors: {len(self.update_stats['local_errors'])}")
            for error in self.update_stats['local_errors']:
                logger.error(f"   [LOCAL] - {error}")
        
        if self.update_stats['motherduck_errors']:
            logger.info(f"❌ MotherDuck Errors: {len(self.update_stats['motherduck_errors'])}")
            for error in self.update_stats['motherduck_errors']:
                logger.error(f"   [MOTHERDUCK] - {error}")
        
        if self.update_stats['errors']:
            logger.info(f"❌ General Errors: {len(self.update_stats['errors'])}")
            for error in self.update_stats['errors']:
                logger.error(f"   - {error}")
        
        if not self.update_stats['errors'] and not self.update_stats['local_errors'] and not self.update_stats['motherduck_errors']:
            logger.info("✅ No errors encountered")
        
        logger.info("=" * 60)

def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Auto-update AI DRIVEN DATA database (Local and/or MotherDuck)')
    parser.add_argument('--db-path', default='ai_driven_data.duckdb', help='Database file path')
    parser.add_argument('--verbose', '-v', action='store_true', help='Verbose logging')
    parser.add_argument('--no-motherduck', action='store_true', help='Disable MotherDuck updates (local only)')
    parser.add_argument('--motherduck-only', action='store_true', help='Update only MotherDuck (skip local updates) - for cron jobs')
    parser.add_argument('--cleanup-old-data', action='store_true', help='Remove data older than 2 years from PA DATA and CLAIMS DATA')
    parser.add_argument('--keep-years', type=int, default=2, help='Number of years to keep when cleaning (default: 2)')
    parser.add_argument(
        '--full-reload-pa-data',
        action='store_true',
        help='Drop local PA DATA and metadata, then reload full 2-year window from MediCloud (use after PA query changes)',
    )
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Create updater and run update
    update_motherduck = not args.no_motherduck
    motherduck_only = args.motherduck_only
    
    if motherduck_only:
        logger.info("☁️  Running in MOTHERDUCK-ONLY mode (for automated updates)")
        logger.info("   Local DuckDB will NOT be updated")
    
    updater = DatabaseUpdater(args.db_path, update_motherduck=update_motherduck, motherduck_only=motherduck_only)
    
    # Run cleanup if requested
    if args.cleanup_old_data:
        if not updater.connect():
            logger.error("❌ Failed to connect to database")
            sys.exit(1)
        cleanup_success = updater.cleanup_old_data(keep_years=args.keep_years)
        updater.disconnect()
        if cleanup_success:
            logger.info("🎉 Cleanup completed successfully!")
            sys.exit(0)
        else:
            logger.error("❌ Cleanup failed!")
            sys.exit(1)
    
    # Run normal update, or PA-only full reload
    if args.full_reload_pa_data:
        success = updater.run_full_pa_data_reload()
    else:
        success = updater.update_all_tables()
    
    if success:
        logger.info("🎉 Database update completed successfully!")
        sys.exit(0)
    else:
        logger.error("❌ Database update failed!")
        sys.exit(1)

if __name__ == "__main__":
    main()
