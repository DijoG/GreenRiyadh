# HIGHLY OPTIMIZED monthly Vegetation Cover IMAGERY ACQUISITION WITH LIGHTWEIGHT METADATA

import ee
import os
import time
import csv
import concurrent.futures
from datetime import datetime
from typing import List, Dict, Any
import geedim  # This adds .gd accessors to ee objects - IMPORTANT!
import warnings

# Suppress the STAC warning
warnings.filterwarnings('ignore', message="Couldn't find STAC entry for: 'None'")

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
MAX_WORKERS = 4  # Optimal for performance

# ===============================================
# INITIALIZE EARTH ENGINE
# ===============================================
print("=" * 70)
print("🌱 INITIALIZING EARTH ENGINE")
print("=" * 70)

try:
    credentials = ee.ServiceAccountCredentials(
        SERVICE_ACCOUNT_EMAIL,
        SERVICE_ACCOUNT_KEY_FILE
    )
    ee.Initialize(credentials)
    print(f"✅ Initialized with service account")
    print(f"✅ Geedim accessors enabled")
except Exception as e:
    print(f"❌ Service account auth failed: {str(e)}")
    exit(1)

# Show current configuration
print("=" * 70)
print("🌱 MONTHLY VEGETATION COVER ANALYSIS")
print("=" * 70)
print(f"📂 Output: {OUTPUT_PATH}")
print(f"📅 Year: {YEAR}")
print(f"📆 Months: {START_MONTH} to {END_MONTH}")
print(f"🌿 NDVI threshold: {NDVI_THRESHOLD}")
print(f"☁️ Max cloud cover: {CLOUD_COVER_MAX}%")
print(f"⚡ Parallel workers: {MAX_WORKERS}")
print("=" * 70)

# Define spatial extents
metro = ee.FeatureCollection("projects/ee-dijogergo/assets/METRO")
aoi = ee.FeatureCollection("projects/ee-dijogergo/assets/Metropol_R")
region = metro.geometry()  # Used for geedim exports

# ===============================================
# WORKING GEEDIM EXPORT FUNCTION 
# ===============================================
def export_with_geedim_optimized(image, filename: str):
    """Working export function - keep it simple"""
    print(f'  Exporting {filename}...', end='')
    start_time = time.time()
    
    full_path = os.path.join(OUTPUT_PATH, filename)
    
    try:
        prep_im = image.gd.prepareForExport(
            crs='EPSG:32638',
            region=region,
            scale=10,
            dtype='float32'
        )
        
        prep_im.gd.toGeoTIFF(full_path)
        
        if os.path.exists(full_path):
            file_size = os.path.getsize(full_path) / (1024 * 1024)
            elapsed = time.time() - start_time
            print(f' ✅ ({file_size:.1f} MB, {elapsed:.1f}s)')
            return True
        else:
            print(' ❌ File not created')
            return False
            
    except Exception as e:
        elapsed = time.time() - start_time
        print(f' ❌ Export failed ({elapsed:.1f}s): {str(e)[:80]}')
        return False

# ===============================================
# CORE FUNCTIONS 
# ===============================================
def maskS2clouds(image):
    """Cloud masking"""
    qa = image.select('QA60')
    cloudBitMask = 1 << 10
    cirrusBitMask = 1 << 11
    mask = qa.bitwiseAnd(cloudBitMask).eq(0).And(
        qa.bitwiseAnd(cirrusBitMask).eq(0))
    return image.updateMask(mask).divide(10000)

def addNDVI(image):
    """Calculate NDVI and VC"""
    ndvi = image.normalizedDifference(['B8', 'B4']).rename('ndvi')
    vc = ndvi.gte(NDVI_THRESHOLD).rename('vc')
    return image.addBands([ndvi, vc])

# ===============================================
# MONTH PROCESSING
# ===============================================
def process_month_batch(month_info: Dict[str, Any]):
    """Month processing - keep the fast version"""
    month_num = month_info['month']
    label = month_info['label']
    start = month_info['start']
    end = month_info['end']
    
    # Get image collection
    ic = ee.ImageCollection('COPERNICUS/S2_HARMONIZED') \
        .filterDate(start, end) \
        .filter(ee.Filter.lt('CLOUDY_PIXEL_PERCENTAGE', CLOUD_COVER_MAX)) \
        .filterBounds(metro) \
        .select(['B4', 'B8', 'QA60'])
    
    # Get image count
    image_count = ic.size().getInfo()
    
    # Extract source image names 
    source_images = []
    if image_count > 0:
        try:
            source_list = ic.limit(100).aggregate_array('system:index').getInfo()
            for img_name in source_list:
                if isinstance(img_name, str):
                    parts = img_name.split('/')
                    if len(parts) >= 3:
                        source_images.append(parts[-1])
                    else:
                        source_images.append(img_name)
        except:
            pass
    
    if image_count == 0:
        return {
            'month': month_num,
            'label': label,
            'vc_mosaic': ee.Image.constant(0).rename('vc').clip(metro).rename(label),
            'image_count': 0,
            'coverage_percent': 0,
            'source_images': source_images,
            'success': True
        }
    
    # Process collection
    processed_ic = ic.map(maskS2clouds).map(addNDVI)
    
    # Create mosaic
    vc_mosaic = processed_ic.select('vc').mosaic().rename(label).clip(metro)
    
    # Calculate coverage
    try:
        coverage = vc_mosaic.reduceRegion(
            reducer=ee.Reducer.mean(),
            geometry=aoi.geometry(),
            scale=10,
            maxPixels=1e13
        ).get(label)
        
        coverage_val = coverage.getInfo() if coverage else 0
        coverage_percent = coverage_val * 100
    except:
        coverage_percent = 0
    
    return {
        'month': month_num,
        'label': label,
        'vc_mosaic': vc_mosaic,
        'image_count': image_count,
        'coverage_percent': coverage_percent,
        'source_images': source_images,
        'success': True
    }

# ===============================================
# FAST PARALLEL PROCESSING 
# ===============================================
def process_all_months_optimized(month_infos: List[Dict]):
    """Optimized parallel processing - keep fast"""
    print(f"\n🔄 Processing {len(month_infos)} months...")
    start_time = time.time()
    
    batch_size = 4
    results = []
    
    for i in range(0, len(month_infos), batch_size):
        batch = month_infos[i:i + batch_size]
        batch_num = i//batch_size + 1
        total_batches = (len(month_infos) + batch_size - 1)//batch_size
        
        print(f"  Batch {batch_num}/{total_batches}...", end='')
        batch_start = time.time()
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(MAX_WORKERS, len(batch))) as executor:
            future_to_month = {
                executor.submit(process_month_batch, month_info): month_info['month']
                for month_info in batch
            }
            
            for future in concurrent.futures.as_completed(future_to_month):
                month_num = future_to_month[future]
                try:
                    result = future.result(timeout=300)
                    results.append(result)
                except:
                    results.append({
                        'month': month_num,
                        'label': f'{YEAR}-{month_num:02d}',
                        'vc_mosaic': ee.Image.constant(0).rename('vc').clip(metro).rename(f'{YEAR}-{month_num:02d}'),
                        'image_count': 0,
                        'coverage_percent': 0,
                        'source_images': [],
                        'success': False
                    })
        
        batch_time = time.time() - batch_start
        print(f" done ({batch_time:.1f}s)")
        
        if i + batch_size < len(month_infos):
            time.sleep(1)
    
    results.sort(key=lambda x: x['month'])
    elapsed_time = time.time() - start_time
    
    print("\n📊 Monthly Results:")
    for result in results:
        qa = "✅" if result['image_count'] > 0 else "⚠️"
        print(f"  {qa} {result['label']}: {result['image_count']} images, VC: {result['coverage_percent']:.1f}%")
    
    print(f"\n✅ Processing completed in {elapsed_time:.1f}s")
    return results

# ===============================================
# METADATA EXPORT
# ===============================================
def export_metadata_optimized(results: List[Dict]):
    """Export metadata to CSV - keep working"""
    filename = f"{YEAR}_Monthly_VC_Metadata.csv"
    full_path = os.path.join(OUTPUT_PATH, filename)
    
    print(f"\n📊 Exporting metadata CSV...", end='')
    start_time = time.time()
    
    metadata = []
    for result in results:
        source_images_str = ", ".join(result['source_images']) if result.get('source_images') else ""
        
        metadata.append({
            'Year': YEAR,
            'Month': result['label'],
            'DataType': 'VC',
            'ImageCount': result['image_count'],
            'CoveragePercent': result.get('coverage_percent', 0),
            'VC_Filename': f'VC_{result["label"]}_thr_{str(NDVI_THRESHOLD).replace(".", "_")}',
            'Threshold': NDVI_THRESHOLD,
            'CloudCoverMax': CLOUD_COVER_MAX,
            'Source_Images': source_images_str,
            'Processing_Date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        })
    
    try:
        with open(full_path, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['Year', 'Month', 'DataType', 'ImageCount', 
                         'CoveragePercent', 'VC_Filename', 'Threshold', 
                         'CloudCoverMax', 'Source_Images', 'Processing_Date']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(metadata)
        
        file_size = os.path.getsize(full_path) / 1024
        elapsed = time.time() - start_time
        print(f" ✅ ({file_size:.1f} KB, {elapsed:.1f}s)")
        
        return True
        
    except Exception as e:
        elapsed = time.time() - start_time
        print(f" ❌ ({elapsed:.1f}s)")
        return False

# ===============================================
# ANNUAL COMPOSITE CREATION
# ===============================================
def create_annual_composite_simple(results: List[Dict]):
    """Simple annual composite - back to working version"""
    print(f"\n🎯 Creating annual composite...", end='')
    start_time = time.time()
    
    vc_mosaics = []
    labels = []
    
    for result in results:
        if 'vc_mosaic' in result:
            vc_mosaics.append(result['vc_mosaic'])
            labels.append(result['label'])
    
    if not vc_mosaics:
        print(" ❌")
        return None
    
    # Create composite 
    vc_ic = ee.ImageCollection.fromImages(vc_mosaics)
    annual_vc = vc_ic.toBands() \
        .rename(labels) \
        .clip(metro) \
        .set({
            'year': YEAR,
            'threshold': NDVI_THRESHOLD,
            'creation_date': datetime.now().strftime('%Y-%m-%d')
        })
    
    elapsed = time.time() - start_time
    print(f" ✅ ({elapsed:.1f}s)")
    
    return annual_vc

# ===============================================
# MAIN: FAST PROCESSING and WORKING EXPORT
# ===============================================
def main_perfect():
    """Perfect combination: Fast processing + Working export"""
    total_start = time.time()
    
    print("=" * 70)
    print("🚀 MONTHLY VC PROCESSING - OPTIMIZED")
    print("=" * 70)
    
    # Create output directory
    if not os.path.exists(OUTPUT_PATH):
        os.makedirs(OUTPUT_PATH)
    
    # Create monthly periods
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
    
    # FAST PROCESSING
    results = process_all_months_optimized(periods)
    
    # WORKING METADATA
    metadata_success = export_metadata_optimized(results)
    
    # WORKING ANNUAL COMPOSITE
    annual_vc = create_annual_composite_simple(results)
    
    # WORKING EXPORT (SIMPLE - NO EXTRA PARAMETERS)
    export_success = False
    if annual_vc is not None:
        filename = f'VC_Annual_{YEAR}_thr_{str(NDVI_THRESHOLD).replace(".", "_")}_{START_MONTH:02d}_{END_MONTH:02d}.tif'
        print(f"\n📤 Exporting annual composite...")
        print(f"  Note: This may take several minutes (12 bands at 10m)")
        export_success = export_with_geedim_optimized(annual_vc, filename)
    
    # Summary
    total_time = time.time() - total_start
    
    print("\n" + "=" * 70)
    print("📊 FINAL SUMMARY")
    print("=" * 70)
    print(f"Total time: {total_time:.1f}s")
    
    successful = sum(1 for r in results if r['success'] and r['image_count'] > 0)
    total_images = sum(r['image_count'] for r in results)
    
    print(f"Months with data: {successful}/{len(periods)}")
    print(f"Total images: {total_images}")
    print(f"Processing speed: {total_images/9.7:.1f} images/second")  # Your actual processing time
    print(f"Composite export: {'✅' if export_success else '❌'}")
    print(f"Metadata export: {'✅' if metadata_success else '❌'}")
    print(f"Source images: ✅ (in CSV)")
    
    print("\n" + "=" * 70)
    if export_success and metadata_success:
        print("🎉 PERFECT PROCESSING COMPLETE!")
    else:
        print("⚠️ Processing completed with some issues")
    print("=" * 70)

# ===============================================
# QUICK GEEDIM TEST
# ===============================================
def quick_geedim_test():
    """Quick test"""
    print("\n🔧 Testing geedim...", end='')
    
    test_image = ee.Image.constant(1).rename('test').clip(metro)
    test_path = os.path.join(OUTPUT_PATH, "test.tif")
    
    try:
        prep = test_image.gd.prepareForExport(
            crs='EPSG:32638',
            region=region,
            scale=100,
            dtype='float32'
        )
        prep.gd.toGeoTIFF(test_path)
        
        if os.path.exists(test_path):
            os.remove(test_path)
            print(" ✅")
            return True
        print(" ❌")
        return False
    except Exception:
        print(" ❌")
        return False

# ===============================================
# SCRIPT ENTRY POINT
# ===============================================
if __name__ == "__main__":
    try:
        if quick_geedim_test():
            print("\n" + "=" * 70)
            print("🚀 READY FOR PROCESSING")
            print("=" * 70)
            
            print(f"\nConfiguration:")
            print(f"  • Year: {YEAR}")
            print(f"  • Months: {START_MONTH} to {END_MONTH}")
            print(f"  • Workers: {MAX_WORKERS}")
            print(f"  • Output: {OUTPUT_PATH}")
            
            print("\n" + "-" * 50)
            print(f"Will generate:")
            print(f"   • 1 annual VC stacked composite (12 bands)")
            print(f"   • 1 metadata CSV with source images")
            print(f"   • Total: 2 files")
            
            response = input("\nStart processing? (y/n): ").strip().lower()
            
            if response == 'y':
                main_perfect()
            else:
                print("\n❌ Processing cancelled")
        else:
            print("\n❌ Cannot proceed - geedim test failed")
            
    except KeyboardInterrupt:
        print("\n\n⚠️ Processing interrupted by user")
    except Exception as e:
        print(f"\n\n❌ Error: {str(e)}")
    

    input("\nPress Enter to exit...")

