import pandas as pd
import logging
import time
import threading
from pathlib import Path
from typing import Dict, Tuple, Optional
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError

logger = logging.getLogger(__name__)


class ParquetConverter:
    """Utility class for converting ODS files to Parquet format for faster processing"""
    
    def __init__(self):
        self.stats = {
            "main_data_rows": 0,
            "dual_registration_rows": 0,
            "conversion_time": 0,
            "errors": []
        }
    
    def convert_ods_to_parquet(self, file_path: str, output_dir: str = None) -> Dict[str, str]:
        """
        Convert ODS or XLSX file to two Parquet files: main data and dual registrations
        
        Args:
            file_path: Path to the source ODS or XLSX file
            output_dir: Directory to save Parquet files (defaults to same directory as source file)
            
        Returns:
            Dict with paths to created Parquet files
        """
        import time
        start_time = time.time()
        
        try:
            file_path_obj = Path(file_path)
            if not file_path_obj.exists():
                raise FileNotFoundError(f"File not found: {file_path}")
            
            # Detect file type and set appropriate engine
            file_extension = file_path_obj.suffix.lower()
            if file_extension == '.ods':
                engine = 'odf'
                file_type = "ODS"
            elif file_extension in ['.xlsx', '.xls']:
                engine = 'openpyxl'  # Faster engine for Excel files
                file_type = "XLSX"
            else:
                raise ValueError(f"Unsupported file format: {file_extension}. Only .ods, .xlsx, and .xls files are supported.")
            
            # Set output directory
            if output_dir is None:
                output_dir = file_path_obj.parent
            else:
                output_dir = Path(output_dir)
                output_dir.mkdir(exist_ok=True)
            
            # Generate output file names based on input file
            base_name = file_path_obj.stem
            main_parquet_path = output_dir / f"{base_name}_main.parquet"
            dual_parquet_path = output_dir / f"{base_name}_dual.parquet"
            
            # Check file size and warn about potential performance issues
            file_size_mb = file_path_obj.stat().st_size / (1024 * 1024)
            logger.info(f"🔄 Converting {file_type} file: {file_path}")
            logger.info(f"📁 Output directory: {output_dir}")
            logger.info(f"📏 File size: {file_size_mb:.1f} MB")
            logger.info(f"🔧 Using {engine} engine for {file_type} format")
            
            if file_size_mb > 25:
                estimated_time = "30-40 minutes" if file_type == "ODS" else "15-25 minutes"
                logger.warning(f"⚠️  Large file detected ({file_size_mb:.1f} MB) - processing may take {estimated_time}")
                logger.warning("💡 Consider using smaller monthly files or be patient during processing")
            elif file_size_mb > 15:
                estimated_time = "15-25 minutes" if file_type == "ODS" else "8-15 minutes"  
                logger.info(f"📊 Medium file size ({file_size_mb:.1f} MB) - processing may take {estimated_time}")
            elif file_size_mb > 5:
                estimated_time = "5-10 minutes" if file_type == "ODS" else "3-8 minutes"
                logger.info(f"📊 Processing {file_type} file ({file_size_mb:.1f} MB) - estimated time: {estimated_time}")
            
            # Convert main data sheet (sheet 1: HSCA_Active_Locations) with progress monitoring
            logger.info("📊 Step 1: Converting main data sheet (HSCA_Active_Locations) to Parquet...")
            try:
                
                logger.info(f"📖 Reading {file_type} file - main data sheet with progress monitoring...")
                
                # Set timeout for main data reading (adjust based on file type)
                timeout_seconds = (20 if file_type == "ODS" else 10) * 60  # XLSX is typically faster
                
                def read_main_sheet():
                    """Function to read main sheet in separate thread"""
                    start_read = time.time()
                    
                    # For very large XLSX files, try additional optimizations
                    read_kwargs = {
                        'sheet_name': 'HSCA_Active_Locations',
                        'engine': engine,
                        'na_filter': True,
                        'keep_default_na': True,
                        'dtype': str
                    }
                    
                    # Add XLSX-specific optimizations
                    if file_type == "XLSX" and file_size_mb > 20:
                        logger.info("🚀 Applying XLSX-specific optimizations for large file...")
                        read_kwargs.update({
                            'nrows': None,  # Read all rows but optimize memory
                        })
                    
                    df_main = pd.read_excel(file_path, **read_kwargs)
                    read_time = time.time() - start_read
                    
                    return df_main, read_time
                
                # Execute with timeout using ThreadPoolExecutor
                try:
                    with ThreadPoolExecutor(max_workers=1) as executor:
                        future = executor.submit(read_main_sheet)
                        df_main, read_time = future.result(timeout=timeout_seconds)
                    
                    logger.info(f"📋 Loaded {len(df_main)} rows from main data sheet (read: {read_time:.1f}s)")
                    
                    # Log performance for different file types
                    rate = len(df_main) / read_time if read_time > 0 else 0
                    logger.info(f"📈 Processing rate: {rate:.0f} rows/second")
                    
                except FutureTimeoutError:
                    timeout_minutes = timeout_seconds // 60
                    error_msg = f"⏱️  Main sheet reading timeout ({timeout_minutes}min) - file too large or corrupted"
                    logger.error(error_msg)
                    self.stats["errors"].append(error_msg)
                    raise Exception(error_msg)
                
                logger.info(f"💾 Writing main data to Parquet: {main_parquet_path}")
                df_main.to_parquet(main_parquet_path, compression='snappy')
                self.stats["main_data_rows"] = len(df_main)
                
                # Calculate file sizes for comparison
                parquet_size = Path(main_parquet_path).stat().st_size / (1024 * 1024)
                logger.info(f"✅ Main data conversion complete: {len(df_main)} rows → {parquet_size:.1f} MB Parquet file")
            except Exception as e:
                error_msg = f"❌ Failed to convert main data sheet: {str(e)}"
                self.stats["errors"].append(error_msg)
                logger.error(error_msg)
                raise
            
            # Convert dual registration sheet (sheet 3) with progress monitoring
            logger.info("🔗 Step 2: Converting dual registration sheet to Parquet...")
            try:
                logger.info("🔍 Detecting sheet structure with progress monitoring...")
                
                def detect_sheets():
                    """Function to detect sheets in separate thread"""
                    start_detection = time.time()
                    excel_file = pd.ExcelFile(file_path, engine=engine)
                    sheet_names = excel_file.sheet_names
                    detection_time = time.time() - start_detection
                    return sheet_names, detection_time
                
                # Execute sheet detection with timeout (30 seconds)
                try:
                    with ThreadPoolExecutor(max_workers=1) as executor:
                        future = executor.submit(detect_sheets)
                        sheet_names, detection_time = future.result(timeout=30)
                    
                    logger.info(f"📑 Found {len(sheet_names)} sheets: {sheet_names} (detection: {detection_time:.1f}s)")
                    
                except FutureTimeoutError:
                    logger.warning("⏱️  Sheet detection timeout (30s) - assuming standard 3-sheet structure")
                    sheet_names = ['README', 'HSCA_Active_Locations', 'Dual_Registration_Locations']
                
                if len(sheet_names) >= 3:
                    third_sheet_name = sheet_names[2]
                    logger.info(f"📖 Reading dual registration sheet: '{third_sheet_name}' with optimizations...")
                    
                    # Set timeout for dual registration reading (adjust based on file type)
                    dual_timeout_seconds = (10 if file_type == "ODS" else 5) * 60  # XLSX is faster
                    
                    def read_dual_sheet():
                        """Function to read dual registration sheet in separate thread"""
                        start_read = time.time()
                        
                        # Try to read with optimized parameters
                        df_dual = pd.read_excel(
                            file_path, 
                            sheet_name=third_sheet_name, 
                            engine=engine,
                            na_filter=True,  # Enable NA filtering for performance
                            keep_default_na=True,
                            dtype=str  # Read everything as string to avoid type inference overhead
                        )
                        
                        read_time = time.time() - start_read
                        return df_dual, read_time
                    
                    try:
                        with ThreadPoolExecutor(max_workers=1) as executor:
                            future = executor.submit(read_dual_sheet)
                            df_dual, read_time = future.result(timeout=dual_timeout_seconds)
                        
                        logger.info(f"📋 Raw dual registration data: {len(df_dual)} rows (read: {read_time:.1f}s)")
                        
                    except FutureTimeoutError:
                        dual_timeout_minutes = dual_timeout_seconds // 60
                        logger.warning(f"⏱️  Dual registration reading timeout ({dual_timeout_minutes}min) - creating empty file")
                        logger.warning("📝 Note: Main data import will proceed without dual registration data")
                        df_dual = pd.DataFrame()  # Create empty DataFrame
                    
                    # Remove completely empty rows
                    df_dual = df_dual.dropna(how='all')
                    logger.info(f"🧹 After cleaning empty rows: {len(df_dual)} rows")
                    
                    if not df_dual.empty:
                        logger.info(f"💾 Writing dual registrations to Parquet: {dual_parquet_path}")
                        df_dual.to_parquet(dual_parquet_path, compression='snappy')
                        self.stats["dual_registration_rows"] = len(df_dual)
                        
                        dual_size = Path(dual_parquet_path).stat().st_size / 1024
                        logger.info(f"✅ Dual registration conversion complete: {len(df_dual)} rows → {dual_size:.1f} KB Parquet file")
                    else:
                        logger.info("⚠️  Dual registration sheet is empty, creating empty Parquet file")
                        # Create empty DataFrame with expected columns
                        empty_df = pd.DataFrame(columns=[
                            'Location ID', 'Location Name', 'Location HSCA Start Date',
                            'Location Type/Sector', 'Provider ID', 'Provider Name',
                            'Linked Organisation ID', 'Linked Organisation Name',
                            'Relationship', 'Relationship Start Date', 'Primary ID'
                        ])
                        empty_df.to_parquet(dual_parquet_path, compression='snappy')
                        self.stats["dual_registration_rows"] = 0
                else:
                    logger.warning("⚠️  No third sheet found for dual registrations, creating empty Parquet file")
                    # Create empty DataFrame
                    empty_df = pd.DataFrame(columns=[
                        'Location ID', 'Location Name', 'Location HSCA Start Date',
                        'Location Type/Sector', 'Provider ID', 'Provider Name',
                        'Linked Organisation ID', 'Linked Organisation Name', 
                        'Relationship', 'Relationship Start Date', 'Primary ID'
                    ])
                    logger.info(f"💾 Creating empty dual registration Parquet: {dual_parquet_path}")
                    empty_df.to_parquet(dual_parquet_path, compression='snappy')
                    self.stats["dual_registration_rows"] = 0
                    logger.info("✅ Empty dual registration Parquet file created")
                    
            except Exception as e:
                error_msg = f"❌ Failed to convert dual registration sheet: {str(e)}"
                self.stats["errors"].append(error_msg)
                logger.warning(error_msg)
                # Create empty dual registration file so import doesn't fail
                logger.info("🔧 Creating fallback empty dual registration file...")
                empty_df = pd.DataFrame(columns=[
                    'Location ID', 'Location Name', 'Location HSCA Start Date',
                    'Location Type/Sector', 'Provider ID', 'Provider Name',
                    'Linked Organisation ID', 'Linked Organisation Name',
                    'Relationship', 'Relationship Start Date', 'Primary ID'
                ])
                empty_df.to_parquet(dual_parquet_path, compression='snappy')
                self.stats["dual_registration_rows"] = 0
                logger.info("✅ Fallback empty dual registration file created")
            
            # Calculate conversion time
            self.stats["conversion_time"] = time.time() - start_time
            
            # Final summary
            main_size = Path(main_parquet_path).stat().st_size / (1024 * 1024)
            dual_size = Path(dual_parquet_path).stat().st_size / 1024
            
            logger.info("🎉 CONVERSION SUMMARY:")
            logger.info(f"   📊 Main data: {self.stats['main_data_rows']} rows → {main_size:.1f} MB")
            logger.info(f"   🔗 Dual registrations: {self.stats['dual_registration_rows']} rows → {dual_size:.1f} KB")
            logger.info(f"   ⏱️  Total conversion time: {self.stats['conversion_time']:.2f} seconds")
            logger.info(f"   🔧 Engine used: {engine} for {file_type} format")
            logger.info(f"✅ {file_type} to Parquet conversion completed successfully!")
            
            return {
                "main_parquet": str(main_parquet_path),
                "dual_parquet": str(dual_parquet_path),
                "stats": self.stats
            }
            
        except Exception as e:
            error_msg = f"ODS to Parquet conversion failed: {str(e)}"
            self.stats["errors"].append(error_msg)
            logger.error(error_msg)
            raise
    
    def get_parquet_info(self, parquet_file_path: str) -> Dict:
        """Get information about a Parquet file"""
        try:
            df = pd.read_parquet(parquet_file_path)
            return {
                "rows": len(df),
                "columns": len(df.columns),
                "column_names": list(df.columns),
                "file_size": Path(parquet_file_path).stat().st_size,
                "memory_usage": df.memory_usage(deep=True).sum()
            }
        except Exception as e:
            logger.error(f"Failed to get Parquet info: {str(e)}")
            return {"error": str(e)}
    
    def validate_parquet_files(self, main_parquet: str, dual_parquet: str) -> bool:
        """Validate that Parquet files are readable and have expected structure"""
        try:
            # Check main parquet file
            df_main = pd.read_parquet(main_parquet)
            required_main_columns = ['Location ID', 'Provider ID', 'Location Name']
            
            missing_main_cols = [col for col in required_main_columns if col not in df_main.columns]
            if missing_main_cols:
                logger.error(f"Main Parquet file missing required columns: {missing_main_cols}")
                return False
            
            # Check dual parquet file
            df_dual = pd.read_parquet(dual_parquet)
            required_dual_columns = ['Location ID', 'Linked Organisation ID']
            
            # Only check if dual file has data
            if not df_dual.empty:
                missing_dual_cols = [col for col in required_dual_columns if col not in df_dual.columns]
                if missing_dual_cols:
                    logger.error(f"Dual Parquet file missing required columns: {missing_dual_cols}")
                    return False
            
            logger.info("✓ Parquet files validation passed")
            return True
            
        except Exception as e:
            logger.error(f"Parquet validation failed: {str(e)}")
            return False