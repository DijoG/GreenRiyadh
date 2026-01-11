# HIGHLY OPTIMIZED bi-weekly NDVI and VC IMAGERY ACQUISITION SCRIPT WITH LIGHTWEIGHT METADATA

import ee
import os
import time
import csv
import concurrent.futures
from datetime import datetime
from typing import List, Dict, Any
import geedim  # This adds .gd accessors to ee objects - IMPORTANT!

# ===============================================
# CONFIGURATION
# ===============================================
SERVICE_ACCOUNT_EMAIL = "vegcov-mailer@ee-dijogergo.iam.gserviceaccount.com"
SERVICE_ACCOUNT_KEY_FILE = r"D:\Gergo\GEEpy\json\ee-dijogergo-c8a021808704.json"
OUTPUT_PATH = r"D:\Gergo\GEEpy\output_biweekly"

# Processing parameters
YEAR = 2025
MONTH = 12
NDVI_THRESHOLD = 0.15
CLOUD_COVER_MAX = 40
ACQUISITION_WINDOW = 21
MAX_WORKERS = 6  # Number of parallel threads

# ===============================================
# NDVI (VC) EXPORT CONTROL 
# ===============================================
EXPORT_NDVI = False  # VC only export
# EXPORT_NDVI = True  # VC and NDVI export

# ===============================================
# INITIALIZE EARTH ENGINE
# ===============================================
print("=" * 70)
print("🌱 INITIALIZING WITH SERVICE ACCOUNT")
print("=" * 70)

try:
    credentials = ee.ServiceAccountCredentials(
        SERVICE_ACCOUNT_EMAIL,
        SERVICE_ACCOUNT_KEY_FILE
    )
    ee.Initialize(credentials)
    print(f"✅ Initialized with service account: {SERVICE_ACCOUNT_EMAIL}")
    print(f"✅ Geedim accessors enabled")
except Exception as e:
    print(f"❌ Service account auth failed: {str(e)}")
    exit(1)

# Show current configuration
print("=" * 70)
print("🌱 VEGETATION COVER ANALYSIS - FAST WITH METADATA")
print("=" * 70)
print(f"📂 Output directory: {OUTPUT_PATH}")
print(f"📅 Year: {YEAR}, Month: {MONTH}")
print(f"🌿 NDVI threshold: {NDVI_THRESHOLD}")
print(f"☁️ Max cloud cover: {CLOUD_COVER_MAX}%")
print(f"⚡ Parallel workers: {MAX_WORKERS}")
print(f"📊 NDVI Export: {'ENABLED' if EXPORT_NDVI else 'DISABLED (VC only)'}")
print("=" * 70)

# Define spatial extents
metro = ee.FeatureCollection("projects/ee-dijogergo/assets/METRO")
region = metro.geometry()  # This is used for geedim exports

# ===============================================
# CORRECT GEEDIM EXPORT FUNCTION (Official API)
# ===============================================
def export_with_geedim_correct(image, filename: str):
    """
    Export using the CORRECT geedim API from official documentation
    Uses: image.gd.prepareForExport() → .gd.toGeoTIFF()
    """
    print(f'  Exporting {filename}...')
    
    full_path = os.path.join(OUTPUT_PATH, filename)
    
    try:
        # STEP 1: Prepare image for export (CORRECT parameters from docs)
        prep_im = image.gd.prepareForExport(
            crs='EPSG:32638',      # Your projection
            region=region,         # ee.Geometry object
            scale=10,              # Resolution in meters
            dtype='float32'        # Data type
        )
        
        # STEP 2: Download to GeoTIFF
        prep_im.gd.toGeoTIFF(full_path)
        
        if os.path.exists(full_path):
            file_size = os.path.getsize(full_path) / (1024 * 1024)
            print(f'    ✅ Exported: {filename} ({file_size:.1f} MB)')
            return True
        else:
            print(f'    ❌ File not created')
            return False
            
    except Exception as e:
        print(f'    ❌ Export failed: {str(e)[:100]}')
        return False

# ===============================================
# CLOUD MASKING AND NDVI FUNCTIONS
# ===============================================
def maskS2clouds(image):
    """Cloud masking for Sentinel-2"""
    qa = image.select('QA60')
    cloudBitMask = 1 << 10
    cirrusBitMask = 1 << 11
    mask = qa.bitwiseAnd(cloudBitMask).eq(0).And(
        qa.bitwiseAnd(cirrusBitMask).eq(0))
    return image.updateMask(mask).divide(10000)

def addNDVI(image):
    """Calculate NDVI and binary mask"""
    ndvi = image.normalizedDifference(['B8', 'B4']).rename('ndvi')
    ndviMask = ndvi.gte(NDVI_THRESHOLD).rename('ndvi_binary')
    return image.addBands([ndvi, ndviMask])

# ===============================================
# BI-WEEKLY PERIOD MANAGEMENT
# ===============================================
def create_biweekly_periods(year: int, months: int):
    """Create bi-weekly periods for processing"""
    total_periods = months * 2
    periods = []
    
    for period in range(1, total_periods + 1):
        start_day = (period - 1) * 15 + 1
        end_day = min(period * 15, 365)
        
        start_date = ee.Date.fromYMD(year, 1, 1).advance(start_day - 1, 'day')
        output_end = ee.Date.fromYMD(year, 1, 1).advance(end_day - 1, 'day')
        
        periods.append({
            'period': period,
            'start': start_date,
            'output_end': output_end,
            'label': start_date.format('YYYY-MM-dd').getInfo()
        })
    
    print(f'📅 Processing {months} months ({total_periods} bi-weekly periods)')
    print(f'📅 Acquisition window: {ACQUISITION_WINDOW} days')
    print(f'⚡ Parallel workers: {MAX_WORKERS}')
    
    return periods

# ===============================================
# PARALLEL PERIOD PROCESSING (FAST - NO HEAVY STATS)
# ===============================================
def process_period_parallel(period_info: Dict[str, Any]):
    """
    Process a single bi-weekly period - FAST version without heavy stats
    Returns VC image, NDVI image (if EXPORT_NDVI), and LIGHTWEIGHT metadata
    """
    period_num = period_info['period']
    label = period_info['label']
    start = period_info['start']
    output_end = period_info['output_end']
    end = start.advance(ACQUISITION_WINDOW, 'days')
    
    # Get Sentinel-2 image collection
    ic = ee.ImageCollection('COPERNICUS/S2_HARMONIZED') \
        .filterDate(start, end) \
        .filter(ee.Filter.lt('CLOUDY_PIXEL_PERCENTAGE', CLOUD_COVER_MAX)) \
        .filterBounds(metro)
    
    image_count = ic.size().getInfo()
    
    # Get source image names (limited to avoid excessive data transfer)
    source_names = []
    if image_count > 0:
        # Get up to 20 source image names (usually enough)
        source_names = ic.limit(20).aggregate_array('system:index').getInfo()
    
    # Create lightweight metadata
    metadata = {
        'Year': YEAR,
        'Months_Processed': MONTH,
        'Period_Number': period_num,
        'Period_Label': label,
        'Output_Start': start.format('YYYY-MM-dd').getInfo(),
        'Output_End': output_end.format('YYYY-MM-dd').getInfo(),
        'Acquisition_Start': start.format('YYYY-MM-dd').getInfo(),
        'Acquisition_End': end.format('YYYY-MM-dd').getInfo(),
        'Acquisition_Window_Days': ACQUISITION_WINDOW,
        'Image_Count': image_count,
        'QA_Flag': image_count > 0,
        'Source_Images': ', '.join(source_names[:10]) + ('...' if len(source_names) > 10 else ''),
        'NDVI_Threshold': NDVI_THRESHOLD,
        'Cloud_Cover_Max': CLOUD_COVER_MAX,
        'Data_Type': 'VC',
        'Processing_Date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }
    
    # Create result dictionary
    result = {
        'period': period_num,
        'label': label,
        'image_count': image_count,
        'source_names': source_names,
        'success': True,
        'metadata': metadata
    }
    
    if image_count == 0:
        # Create empty mosaics for periods with no images
        result['vc_image'] = ee.Image.constant(0).rename('ndvi_binary').clip(metro).rename(label)
        if EXPORT_NDVI:
            result['ndvi_image'] = ee.Image.constant(-9999).rename('ndvi').clip(metro).rename(label)
        return result
    
    # Apply cloud masking and NDVI calculation on GEE server
    processed_ic = ic.map(maskS2clouds).map(addNDVI)
    
    # Always create VC mosaic
    binaryVC = processed_ic.select('ndvi_binary').mosaic() \
        .unmask(0) \
        .clip(metro) \
        .round()
    result['vc_image'] = binaryVC.rename(label)
    
    # Create NDVI mosaic only if needed
    if EXPORT_NDVI:
        ndviMean = processed_ic.select('ndvi').mean() \
            .unmask(-9999) \
            .clip(metro)
        result['ndvi_image'] = ndviMean.rename(label)
        # Add NDVI metadata entry
        ndvi_metadata = metadata.copy()
        ndvi_metadata['Data_Type'] = 'NDVI_mean'
        result['ndvi_metadata'] = ndvi_metadata
    
    return result

def process_all_periods_parallel(period_infos: List[Dict]):
    """Process all periods in parallel using ThreadPoolExecutor"""
    print(f"\n🔄 Processing {len(period_infos)} periods in parallel...")
    start_time = time.time()
    
    results = []
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Submit all processing tasks
        future_to_period = {
            executor.submit(process_period_parallel, period_info): period_info['period']
            for period_info in period_infos
        }
        
        # Process results as they complete
        completed = 0
        for future in concurrent.futures.as_completed(future_to_period):
            period_num = future_to_period[future]
            try:
                result = future.result()
                results.append(result)
                completed += 1
                
                # Show quick summary
                qa = "✅" if result['image_count'] > 0 else "⚠️"
                print(f"  {qa} Period {period_num}: {result['label']} ({result['image_count']} images)")
                
            except Exception as e:
                print(f"  ❌ Period {period_num} failed: {str(e)[:100]}")
                # Add placeholder for failed period
                placeholder = {
                    'period': period_num,
                    'label': f'period_{period_num}',
                    'vc_image': ee.Image.constant(0).rename(f'period_{period_num}').clip(metro),
                    'image_count': 0,
                    'source_names': [],
                    'success': False,
                    'metadata': {
                        'Year': YEAR,
                        'Months_Processed': MONTH,
                        'Period_Number': period_num,
                        'Period_Label': f'period_{period_num}',
                        'Image_Count': 0,
                        'QA_Flag': False,
                        'Processing_Date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    }
                }
                if EXPORT_NDVI:
                    placeholder['ndvi_image'] = ee.Image.constant(-9999).rename(f'period_{period_num}').clip(metro)
                results.append(placeholder)
    
    # Sort by period number
    results.sort(key=lambda x: x['period'])
    
    elapsed_time = time.time() - start_time
    print(f"\n✅ Parallel processing completed in {elapsed_time:.1f} seconds")
    print(f"   Processed {len(results)} periods")
    
    return results

# ===============================================
# LIGHTWEIGHT METADATA CSV EXPORT (FAST)
# ===============================================
def export_metadata_csv_fast(results: List[Dict]):
    """
    Export lightweight metadata to CSV file - FAST version
    Only includes essential information about source images
    """
    filename = f"{YEAR}_BiWeekly_VC_NDVI_Metadata.csv"
    full_path = os.path.join(OUTPUT_PATH, filename)
    
    print(f"\n📊 Exporting lightweight metadata to CSV: {filename}")
    
    # Collect all metadata records
    all_metadata = []
    
    for result in results:
        if 'metadata' in result:
            # Add VC metadata
            vc_meta = result['metadata'].copy()
            all_metadata.append(vc_meta)
            
            # Add NDVI metadata if we have NDVI data
            if EXPORT_NDVI and 'ndvi_metadata' in result:
                ndvi_meta = result['ndvi_metadata'].copy()
                all_metadata.append(ndvi_meta)
    
    if not all_metadata:
        print("  ⚠️ No metadata to export")
        return False
    
    try:
        # Write to CSV
        with open(full_path, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = all_metadata[0].keys()
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(all_metadata)
        
        print(f"  ✅ Metadata CSV exported: {full_path}")
        print(f"  📋 {len(all_metadata)} records written")
        
        # Quick summary
        total_images = sum(result['image_count'] for result in results)
        successful_periods = sum(1 for result in results if result['image_count'] > 0)
        
        print(f"\n📈 Quick Summary:")
        print(f"  • Total source images used: {total_images}")
        print(f"  • Periods with data: {successful_periods}/{len(results)}")
        print(f"  • Success rate: {(successful_periods/len(results)*100):.1f}%")
        
        return True
        
    except Exception as e:
        print(f"  ❌ Failed to export metadata CSV: {str(e)}")
        return False

# ===============================================
# FILE EXPORT MANAGEMENT (FLEXIBLE)
# ===============================================
def export_files_flexible(results: List[Dict]):
    """
    Export files based on EXPORT_NDVI parameter:
    - If EXPORT_NDVI = False: Export ONLY VC files
    - If EXPORT_NDVI = True: Export BOTH VC and NDVI files
    """
    # Define period pairs for 12 months (full year)
    period_pairs = ['01_02', '03_04', '05_06', '07_08', '09_10', 
                    '11_12', '13_14', '15_16', '17_18', '19_20', '21_22', '23_24']
    
    # Only use pairs needed for the specified months
    needed_pairs = period_pairs[:MONTH]
    
    if EXPORT_NDVI:
        print(f"\n📊 Exporting {len(needed_pairs)} pairs (VC + NDVI)...")
        total_files = len(needed_pairs) * 2  # VC + NDVI for each pair
    else:
        print(f"\n📊 Exporting {len(needed_pairs)} VC pairs (NDVI disabled)...")
        total_files = len(needed_pairs)  # VC only
    
    print("=" * 70)
    
    # Create output directory
    if not os.path.exists(OUTPUT_PATH):
        os.makedirs(OUTPUT_PATH)
        print(f"📁 Created output directory: {OUTPUT_PATH}")
    
    export_start = time.time()
    successful_exports = 0
    
    for i, periods in enumerate(needed_pairs):
        start_period = i * 2 + 1
        end_period = i * 2 + 2
        
        if EXPORT_NDVI:
            print(f"\n📦 Exporting pair {periods} (VC + NDVI)...")
        else:
            print(f"\n📦 Exporting VC pair {periods}...")
        
        # Get results for this pair
        pair_results = [r for r in results if start_period <= r['period'] <= end_period]
        
        if len(pair_results) == 2:
            # Extract VC images and labels
            vc_images = [r['vc_image'] for r in pair_results]
            labels = [r['label'] for r in pair_results]
            
            # Create 2-band combined VC image
            vc_combined = ee.ImageCollection(vc_images).toBands().rename(labels).clip(metro)
            
            # Export VC image (ALWAYS exported)
            vc_filename = f'{YEAR}_BiWeekly_VC_{periods}.tif'
            if export_with_geedim_correct(vc_combined, vc_filename):
                successful_exports += 1
            
            # Small delay between VC and potential NDVI export
            time.sleep(1)
            
            # Export NDVI image only if enabled
            if EXPORT_NDVI:
                # Extract NDVI images
                ndvi_images = [r['ndvi_image'] for r in pair_results]
                
                # Create 2-band combined NDVI image
                ndvi_combined = ee.ImageCollection(ndvi_images).toBands().rename(labels).clip(metro)
                
                # Export NDVI image
                ndvi_filename = f'{YEAR}_BiWeekly_NDVI_{periods}.tif'
                if export_with_geedim_correct(ndvi_combined, ndvi_filename):
                    successful_exports += 1
                
                time.sleep(1)  # Additional delay for NDVI export
            
        else:
            print(f"  ⚠️ Missing data for pair {periods}")
    
    export_time = time.time() - export_start
    
    # Generate appropriate success message
    if EXPORT_NDVI:
        print(f"\n✅ Export completed in {export_time:.1f} seconds")
        print(f"   Successful exports: {successful_exports}/{total_files} files (VC + NDVI)")
    else:
        print(f"\n✅ Export completed in {export_time:.1f} seconds")
        print(f"   Successful VC exports: {successful_exports}/{total_files} files")
    
    return successful_exports, total_files

# ===============================================
# MAIN EXECUTION FUNCTION (FAST VERSION)
# ===============================================
def main():
    """Main processing workflow - FAST version"""
    total_start_time = time.time()
    
    # Step 1: Create bi-weekly periods
    period_infos = create_biweekly_periods(YEAR, MONTH)
    
    # Step 2: Process all periods in parallel (FAST!)
    results = process_all_periods_parallel(period_infos)
    
    # Step 3: Export lightweight metadata to CSV (FAST)
    metadata_success = export_metadata_csv_fast(results)
    
    # Step 4: Export image files based on EXPORT_NDVI parameter
    successful_exports, total_files = export_files_flexible(results)
    
    # Step 5: Generate summary
    total_time = time.time() - total_start_time
    
    print("\n" + "=" * 70)
    print("📊 FINAL SUMMARY")
    print("=" * 70)
    print(f"Total processing time: {total_time:.1f} seconds")
    print(f"Successful image exports: {successful_exports}/{total_files} files")
    print(f"Metadata export: {'✅ SUCCESS' if metadata_success else '❌ FAILED'}")
    
    # List generated files
    if os.path.exists(OUTPUT_PATH):
        files = os.listdir(OUTPUT_PATH)
        tif_files = [f for f in files if f.endswith('.tif')]
        csv_files = [f for f in files if f.endswith('.csv')]
        
        if tif_files or csv_files:
            print(f"\n📁 Generated files in {OUTPUT_PATH}:")
            
            if tif_files:
                print(f"  Image files ({len(tif_files)}):")
                for file in sorted(tif_files):
                    file_path = os.path.join(OUTPUT_PATH, file)
                    if os.path.exists(file_path):
                        file_size = os.path.getsize(file_path) / (1024 * 1024)
                        print(f"    • {file} ({file_size:.1f} MB)")
            
            if csv_files:
                print(f"  Metadata files ({len(csv_files)}):")
                for file in csv_files:
                    file_path = os.path.join(OUTPUT_PATH, file)
                    if os.path.exists(file_path):
                        file_size = os.path.getsize(file_path) / 1024  # KB
                        print(f"    • {file} ({file_size:.1f} KB)")
        else:
            print("\n⚠️ No files were generated")
    
    print("\n" + "=" * 70)
    if successful_exports == total_files and metadata_success:
        print("🎉 COMPLETE SUCCESS! All files and metadata exported.")
    else:
        print(f"⚠️ Partial success. Check export results above.")
    print("=" * 70)

# ===============================================
# TEST FUNCTION TO VERIFY GEEDIM WORKS
# ===============================================
def test_geedim_functionality():
    """Test if geedim is working correctly with the new API"""
    print("\n🔧 Testing geedim functionality...")
    
    # Create a simple test image
    test_image = ee.Image.constant(1).rename('test').clip(metro)
    
    test_path = os.path.join(OUTPUT_PATH, "geedim_test.tif")
    
    try:
        print("  Testing .gd.prepareForExport()...")
        prep_im = test_image.gd.prepareForExport(
            crs='EPSG:32638',
            region=region,
            scale=100,  # Fast test resolution
            dtype='float32'
        )
        print("  ✅ .gd.prepareForExport() works")
        
        print("  Testing .gd.toGeoTIFF()...")
        prep_im.gd.toGeoTIFF(test_path)
        
        if os.path.exists(test_path):
            file_size = os.path.getsize(test_path) / 1024  # KB
            print(f"  ✅ .gd.toGeoTIFF() works (file: {file_size:.1f} KB)")
            os.remove(test_path)  # Clean up test file
            return True
        else:
            print("  ❌ .gd.toGeoTIFF() didn't create file")
            return False
            
    except Exception as e:
        print(f"  ❌ Geedim test failed: {str(e)}")
        print("  ℹ️ Make sure you have the latest geedim: pip install --upgrade geedim")
        return False

# ===============================================
# SCRIPT ENTRY POINT WITH USER CONFIRMATION
# ===============================================
if __name__ == "__main__":
    try:
        # First, test if geedim works with the new API
        if test_geedim_functionality():
            print("\n" + "=" * 70)
            print("✅ GEEDIM IS WORKING! Starting full processing...")
            print("=" * 70)
            
            # Show user the current configuration
            print(f"\nCurrent configuration:")
            print(f"  • Year: {YEAR}")
            print(f"  • Months: {MONTH}")
            print(f"  • NDVI Export: {'ENABLED' if EXPORT_NDVI else 'DISABLED (VC only)'}")
            print(f"  • Metadata: CSV to local directory (lightweight)")
            print(f"  • Output folder: {OUTPUT_PATH}")
            
            # Ask for confirmation
            print("\n" + "-" * 50)
            
            # Calculate expected files
            image_pairs = MONTH  # One pair per month
            if EXPORT_NDVI:
                expected_image_files = image_pairs * 2  # VC + NDVI
            else:
                expected_image_files = image_pairs  # VC only
            
            expected_metadata_files = 1  # One CSV file
            
            print(f"⚠️  This will generate:")
            print(f"   • {expected_image_files} image files")
            print(f"   • {expected_metadata_files} metadata CSV file")
            print(f"   • Total: {expected_image_files + expected_metadata_files} files")
            
            response = input("\nContinue? (y/n): ").strip().lower()
            
            if response == 'y':
                # Run the main processing pipeline
                main()
            else:
                print("\n❌ Processing cancelled by user.")
        else:
            print("\n❌ Geedim test failed. Cannot proceed with processing.")
            
    except KeyboardInterrupt:
        print("\n\n⚠️ Process interrupted by user")
    except Exception as e:
        print(f"\n\n❌ Unexpected error: {str(e)}")
        import traceback
        traceback.print_exc()
    
    # Keep console open
    input("\nPress Enter to exit...")