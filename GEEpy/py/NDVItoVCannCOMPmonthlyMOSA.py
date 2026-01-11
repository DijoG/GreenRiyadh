# HIGHLY OPTIMIZED monthly Vegetation Cover IMAGERY ACQUISITION SCRIPT WITH LIGHTWEIGHT METADATA

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
OUTPUT_PATH = r"D:\Gergo\GEEpy\output_monthly"

# Processing parameters
YEAR = 2025
START_MONTH = 1  # Starting month (1 = January)
END_MONTH = 12   # Ending month (12 = December)
NDVI_THRESHOLD = 0.15
CLOUD_COVER_MAX = 15
MAX_WORKERS = 4  # Number of parallel threads

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
print("🌱 MONTHLY VEGETATION COVER ANALYSIS - OPTIMIZED")
print("=" * 70)
print(f"📂 Output directory: {OUTPUT_PATH}")
print(f"📅 Year: {YEAR}")
print(f"📆 Months: {START_MONTH} to {END_MONTH}")
print(f"🌿 NDVI threshold: {NDVI_THRESHOLD}")
print(f"☁️ Max cloud cover: {CLOUD_COVER_MAX}%")
print(f"⚡ Parallel workers: {MAX_WORKERS} (optimized)")
print("=" * 70)

# Define spatial extents
metro = ee.FeatureCollection("projects/ee-dijogergo/assets/METRO")
aoi = ee.FeatureCollection("projects/ee-dijogergo/assets/Metropol_R")
region = metro.geometry()  # Used for geedim exports

# ===============================================
# OPTIMIZED GEEDIM EXPORT FUNCTION
# ===============================================
def export_with_geedim_optimized(image, filename: str):
    """
    Optimized export function with better error handling and progress
    """
    print(f'  Exporting {filename}...')
    start_time = time.time()
    
    full_path = os.path.join(OUTPUT_PATH, filename)
    
    try:
        # Use batch mode for better performance
        prep_im = image.gd.prepareForExport(
            crs='EPSG:32638',
            region=region,
            scale=10,
            dtype='float32',
            # batch=True  # Uncomment if geedim supports this
        )
        
        prep_im.gd.toGeoTIFF(full_path)
        
        if os.path.exists(full_path):
            file_size = os.path.getsize(full_path) / (1024 * 1024)
            elapsed = time.time() - start_time
            print(f'    ✅ Exported: {filename} ({file_size:.1f} MB, {elapsed:.1f}s)')
            return True
        else:
            print(f'    ❌ File not created')
            return False
            
    except Exception as e:
        elapsed = time.time() - start_time
        print(f'    ❌ Export failed after {elapsed:.1f}s: {str(e)[:100]}')
        return False

# ===============================================
# OPTIMIZED FUNCTIONS
# ===============================================
def maskS2clouds(image):
    """Optimized cloud masking"""
    qa = image.select('QA60')
    cloudBitMask = 1 << 10
    cirrusBitMask = 1 << 11
    mask = qa.bitwiseAnd(cloudBitMask).eq(0).And(
        qa.bitwiseAnd(cirrusBitMask).eq(0))
    return image.updateMask(mask).divide(10000)

def addNDVI(image):
    """Calculate NDVI and VC - optimized"""
    ndvi = image.normalizedDifference(['B8', 'B4']).rename('ndvi').clip(metro)
    vc = ndvi.gte(NDVI_THRESHOLD).rename('vc')
    return image.addBands([ndvi, vc])

# ===============================================
# OPTIMIZED MONTH PROCESSING - BATCH OPERATIONS
# ===============================================
def process_month_batch(month_info: Dict[str, Any]):
    """
    Optimized month processing with batch operations
    """
    month_num = month_info['month']
    label = month_info['label']
    start = month_info['start']
    end = month_info['end']
    
    # Get image collection with optimized filters
    ic = ee.ImageCollection('COPERNICUS/S2_HARMONIZED') \
        .filterDate(start, end) \
        .filter(ee.Filter.lt('CLOUDY_PIXEL_PERCENTAGE', CLOUD_COVER_MAX)) \
        .filterBounds(metro) \
        .select(['B4', 'B8', 'QA60'])  # Only select needed bands
    
    # Get metadata in batch
    image_count = ic.size().getInfo()
    
    # Process only if we have images
    if image_count == 0:
        return {
            'month': month_num,
            'label': label,
            'vc_mosaic': ee.Image.constant(0).rename('vc').clip(metro).rename(label),
            'image_count': 0,
            'coverage_percent': 0,
            'success': True
        }
    
    # Apply processing in batch
    processed_ic = ic.map(maskS2clouds).map(addNDVI)
    
    # Create mosaic
    vc_mosaic = processed_ic.select('vc').mosaic().rename(label).clip(metro)
    
    # Calculate coverage
    coverage = processed_ic.select('vc').mosaic().reduceRegion(
        reducer=ee.Reducer.mean(),
        geometry=aoi.geometry(),
        scale=10,
        maxPixels=1e13
    ).get('vc')
    
    coverage_val = coverage.getInfo() if coverage else 0
    coverage_percent = coverage_val * 100 if coverage_val else 0
    
    return {
        'month': month_num,
        'label': label,
        'vc_mosaic': vc_mosaic,
        'image_count': image_count,
        'coverage_percent': coverage_percent,
        'success': True
    }

# ===============================================
# OPTIMIZED PARALLEL PROCESSING WITH BATCHING
# ===============================================
def process_all_months_optimized(month_infos: List[Dict]):
    """
    Optimized parallel processing with better batching
    """
    print(f"\n🔄 Processing {len(month_infos)} months with optimized parallel execution...")
    start_time = time.time()
    
    # Process in smaller batches to avoid overwhelming the API
    batch_size = min(4, len(month_infos))  # Process 4 months at a time
    results = []
    
    for i in range(0, len(month_infos), batch_size):
        batch = month_infos[i:i + batch_size]
        print(f"\n  Processing batch {i//batch_size + 1}/{(len(month_infos) + batch_size - 1)//batch_size}")
        
        batch_start = time.time()
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(MAX_WORKERS, len(batch))) as executor:
            future_to_month = {
                executor.submit(process_month_batch, month_info): month_info['month']
                for month_info in batch
            }
            
            for future in concurrent.futures.as_completed(future_to_month):
                month_num = future_to_month[future]
                try:
                    result = future.result(timeout=300)  # 5 minute timeout
                    results.append(result)
                    
                    qa = "✅" if result['image_count'] > 0 else "⚠️"
                    print(f"    {qa} Month {month_num:02d}: {result['image_count']} images, "
                          f"VC: {result.get('coverage_percent', 0):.1f}%")
                    
                except concurrent.futures.TimeoutError:
                    print(f"    ⏰ Month {month_num:02d}: TIMEOUT - adding placeholder")
                    results.append({
                        'month': month_num,
                        'label': f'{YEAR}-{month_num:02d}',
                        'vc_mosaic': ee.Image.constant(0).rename('vc').clip(metro).rename(f'{YEAR}-{month_num:02d}'),
                        'image_count': 0,
                        'coverage_percent': 0,
                        'success': False
                    })
                except Exception as e:
                    print(f"    ❌ Month {month_num:02d}: Error - {str(e)[:80]}")
                    results.append({
                        'month': month_num,
                        'label': f'{YEAR}-{month_num:02d}',
                        'vc_mosaic': ee.Image.constant(0).rename('vc').clip(metro).rename(f'{YEAR}-{month_num:02d}'),
                        'image_count': 0,
                        'coverage_percent': 0,
                        'success': False
                    })
        
        batch_time = time.time() - batch_start
        print(f"    Batch completed in {batch_time:.1f} seconds")
        
        # Small delay between batches to avoid rate limiting
        if i + batch_size < len(month_infos):
            print(f"    Waiting 2 seconds before next batch...")
            time.sleep(2)
    
    # Sort by month number
    results.sort(key=lambda x: x['month'])
    
    elapsed_time = time.time() - start_time
    print(f"\n✅ Parallel processing completed in {elapsed_time:.1f} seconds")
    print(f"   Average: {elapsed_time/len(month_infos):.1f} seconds per month")
    
    return results

# ===============================================
# OPTIMIZED METADATA COLLECTION
# ===============================================
def collect_metadata(results: List[Dict]):
    """
    Collect metadata from results
    """
    all_metadata = []
    
    for result in results:
        month_label = result['label']
        
        metadata = {
            'Year': YEAR,
            'Month': month_label,
            'DataType': 'VC',
            'ImageCount': result['image_count'],
            'CoveragePercent': result.get('coverage_percent', 0),
            'VC_Filename': f'VC_{month_label}_thr_{str(NDVI_THRESHOLD).replace(".", "_")}',
            'Threshold': NDVI_THRESHOLD,
            'CloudCoverMax': CLOUD_COVER_MAX,
            'Processing_Date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        all_metadata.append(metadata)
    
    return all_metadata

# ===============================================
# OPTIMIZED METADATA EXPORT
# ===============================================
def export_metadata_optimized(results: List[Dict]):
    """
    Export metadata to CSV - optimized
    """
    filename = f"{YEAR}_Monthly_VC_Metadata.csv"
    full_path = os.path.join(OUTPUT_PATH, filename)
    
    print(f"\n📊 Exporting optimized metadata CSV...")
    
    metadata = collect_metadata(results)
    
    try:
        with open(full_path, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['Year', 'Month', 'DataType', 'ImageCount', 
                         'CoveragePercent', 'VC_Filename', 'Threshold', 'CloudCoverMax', 'Processing_Date']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(metadata)
        
        file_size = os.path.getsize(full_path) / 1024
        print(f"  ✅ Metadata exported: {filename} ({file_size:.1f} KB)")
        print(f"  📋 {len(metadata)} records written")
        
        # Summary statistics
        total_images = sum(r['image_count'] for r in results)
        successful_months = sum(1 for r in results if r['image_count'] > 0)
        
        print(f"\n📈 Processing Summary:")
        print(f"  • Total source images: {total_images}")
        print(f"  • Months with data: {successful_months}/{len(results)}")
        print(f"  • Success rate: {(successful_months/len(results)*100):.1f}%")
        
        return True
        
    except Exception as e:
        print(f"  ❌ Failed to export metadata: {str(e)}")
        return False

# ===============================================
# OPTIMIZED ANNUAL COMPOSITE CREATION
# ===============================================
def create_annual_composite_optimized(results: List[Dict]):
    """
    Create annual VC stacked composite - optimized
    """
    print(f"\n🎯 Creating optimized annual VC stacked composite...")
    start_time = time.time()
    
    vc_mosaics = []
    labels = []
    
    for result in results:
        if 'vc_mosaic' in result:
            vc_mosaics.append(result['vc_mosaic'])
            labels.append(result['label'])
    
    if not vc_mosaics:
        print("  ⚠️ No mosaics available")
        return None
    
    # Create composite with optimized band naming
    vc_ic = ee.ImageCollection.fromImages(vc_mosaics)
    annual_vc = vc_ic.toBands() \
        .rename(labels) \
        .clip(metro) \
        .set({
            'year': YEAR,
            'threshold': NDVI_THRESHOLD,
            'data_type': 'VC',
            'number_of_bands': len(labels),
            'creation_date': datetime.now().strftime('%Y-%m-%d')
        })
    
    elapsed = time.time() - start_time
    print(f"  ✅ Created annual composite with {len(labels)} bands in {elapsed:.1f}s")
    print(f"  📊 Band order: {', '.join(labels[:5])}" + (f", ..." if len(labels) > 5 else ""))
    
    return annual_vc

# ===============================================
# MAIN OPTIMIZED EXECUTION
# ===============================================
def main_optimized():
    """Optimized main processing workflow"""
    total_start = time.time()
    
    print("=" * 70)
    print("🚀 STARTING OPTIMIZED PROCESSING PIPELINE")
    print("=" * 70)
    
    # Create output directory
    if not os.path.exists(OUTPUT_PATH):
        os.makedirs(OUTPUT_PATH)
    
    # Step 1: Create monthly periods
    periods = []
    for month in range(START_MONTH, END_MONTH + 1):
        start_date = ee.Date.fromYMD(YEAR, month, 1)
        end_date = start_date.advance(1, 'month')
        label = start_date.format('YYYY-MM').getInfo()
        periods.append({
            'month': month,
            'start': start_date,
            'end': end_date,
            'label': label
        })
    
    print(f"📅 Processing {len(periods)} months ({START_MONTH} to {END_MONTH})")
    
    # Step 2: Process months in optimized parallel batches
    results = process_all_months_optimized(periods)
    
    # Step 3: Create annual composite
    annual_vc = create_annual_composite_optimized(results)
    
    # Step 4: Export metadata
    metadata_success = export_metadata_optimized(results)
    
    # Step 5: Export annual composite
    print(f"\n📤 Exporting annual composite...")
    if annual_vc is not None:
        filename = f'VC_Annual_{YEAR}_thr_{str(NDVI_THRESHOLD).replace(".", "_")}.tif'
        export_success = export_with_geedim_optimized(annual_vc, filename)
    else:
        export_success = False
    
    # Final summary
    total_time = time.time() - total_start
    
    print("\n" + "=" * 70)
    print("📊 FINAL OPTIMIZED SUMMARY")
    print("=" * 70)
    print(f"Total processing time: {total_time:.1f} seconds")
    print(f"Average per month: {total_time/len(periods):.1f} seconds")
    print(f"Image export: {'✅ SUCCESS' if export_success else '❌ FAILED'}")
    print(f"Metadata export: {'✅ SUCCESS' if metadata_success else '❌ FAILED'}")
    
    # Performance metrics
    total_images = sum(r['image_count'] for r in results)
    print(f"\n📈 Performance Metrics:")
    print(f"  • Total Sentinel-2 images processed: {total_images}")
    print(f"  • Processing speed: {total_images/total_time:.2f} images/second" if total_time > 0 else "")
    print(f"  • Memory efficient: Using only {MAX_WORKERS} parallel workers")
    
    print("\n" + "=" * 70)
    if export_success and metadata_success:
        print("🎉 OPTIMIZED PROCESSING COMPLETE!")
    else:
        print("⚠️ Processing completed with some issues")
    print("=" * 70)

# ===============================================
# QUICK GEEDIM TEST
# ===============================================
def quick_geedim_test():
    """Quick test of geedim functionality"""
    print("\n🔧 Running quick geedim test...")
    
    test_image = ee.Image.constant(1).rename('test').clip(metro)
    test_path = os.path.join(OUTPUT_PATH, "test.tif")
    
    try:
        # Quick export test
        prep = test_image.gd.prepareForExport(
            crs='EPSG:32638',
            region=region,
            scale=100,
            dtype='float32'
        )
        prep.gd.toGeoTIFF(test_path)
        
        if os.path.exists(test_path):
            os.remove(test_path)
            print("✅ Geedim test passed")
            return True
        return False
    except Exception as e:
        print(f"❌ Geedim test failed: {str(e)[:80]}")
        return False

# ===============================================
# SCRIPT ENTRY POINT
# ===============================================
if __name__ == "__main__":
    try:
        # Quick test first
        if quick_geedim_test():
            print("\n" + "=" * 70)
            print("🚀 READY FOR OPTIMIZED PROCESSING")
            print("=" * 70)
            
            print(f"\nConfiguration:")
            print(f"  • Year: {YEAR}")
            print(f"  • Months: {START_MONTH} to {END_MONTH}")
            print(f"  • Workers: {MAX_WORKERS} (optimized)")
            print(f"  • Output: {OUTPUT_PATH}")
            
            print("\n" + "-" * 50)
            print(f"⚠️  Will generate:")
            print(f"   • 1 annual VC stacked composite")
            print(f"   • 1 metadata CSV file")
            print(f"   • Total: 2 files")
            
            response = input("\nStart optimized processing? (y/n): ").strip().lower()
            
            if response == 'y':
                main_optimized()
            else:
                print("\n❌ Processing cancelled")
        else:
            print("\n❌ Cannot proceed - geedim test failed")
            
    except KeyboardInterrupt:
        print("\n\n⚠️ Processing interrupted by user")
    except Exception as e:
        print(f"\n\n❌ Unexpected error: {str(e)}")
        import traceback
        traceback.print_exc()
    
    # Keep console open
    input("\nPress Enter to exit...")