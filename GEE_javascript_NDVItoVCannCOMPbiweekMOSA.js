// Import the batch processing library
var batch = require('users/fitoprincipe/geetools:batch');

// Define spatial extents
var metro = ee.FeatureCollection("projects/ee-dijogergo/assets/METRO");
var AOI = ee.FeatureCollection("projects/ee-dijogergo/assets/Metropol_R");

// ===============================================
// Parameters ------------------------------------
// Year to process
var year = 2017;

// Months to process (1-12, e.g., 9 for Jan-Sept)
var month = 12; 

// NDVI threshold
var thrash = 0.15; 

// Cloud cover filter (%)
var cloud = 30; 

// Acquisition window (days)
var acquisition_window = 21; 
// -----------------------------------------------
// ===============================================

// Predefined reducer 
var meanReducer = ee.Reducer.mean();
var statsScale = 10;

// Cloud masking function for Sentinel-2 
function maskS2clouds(image) {
  var qa = image.select('QA60');
  var cloudBitMask = 1 << 10;
  var cirrusBitMask = 1 << 11;
  var mask = qa.bitwiseAnd(cloudBitMask).eq(0)
             .and(qa.bitwiseAnd(cirrusBitMask).eq(0));
  return image.updateMask(mask).divide(10000);
}

// Add NDVI and create binary vegetation mask
function addNDVI(image) {
  var ndvi = image.normalizedDifference(['B8', 'B4']).rename('ndvi');
  var ndviMask = ndvi.gte(thrash).rename('ndvi_binary');
  return image.addBands([ndvi, ndviMask]);
}

// Create bi-weekly periods based on the number of months
var totalPeriods = ee.Number(month).multiply(2);
var biweeklyPeriods = ee.List.sequence(1, totalPeriods).map(function(period) {
  period = ee.Number(period);
  var startDay = period.subtract(1).multiply(15).add(1);
  var endDay = period.multiply(15).min(365);
  
  var startDate = ee.Date(year + '-01-01').advance(startDay.subtract(1), 'day');
  var outputEnd = ee.Date(year + '-01-01').advance(endDay.subtract(1), 'day');
  
  return ee.Dictionary({
    'period': period,
    'start': startDate,
    'output_end': outputEnd
  });
});

print('Processing ' + month + ' months (' + totalPeriods.getInfo() + ' bi-weekly periods)');
print('Acquisition window:', acquisition_window, 'days');

// Function to process a single period
function processPeriod(periodInfo) {
  periodInfo = ee.Dictionary(periodInfo);
  var start = ee.Date(periodInfo.get('start'));
  var output_end = ee.Date(periodInfo.get('output_end'));
  var end = start.advance(acquisition_window, 'days');
  var period_num = ee.Number(periodInfo.get('period'));
  var label = start.format('YYYY-MM-dd');

  // Get the image collection 
  var ic = ee.ImageCollection('COPERNICUS/S2_HARMONIZED')
    .filterDate(start, end)
    .filter(ee.Filter.lt('CLOUDY_PIXEL_PERCENTAGE', cloud))
    .filterBounds(metro)
    .map(maskS2clouds)
    .map(addNDVI);
  
  var imageCount = ic.size();
  
  // Create binary vegetation cover mosaic
  var binaryVC = ic.select('ndvi_binary').mosaic()
    .unmask(0)
    .clip(metro)
    .round();

  // Calculate statistics
  var stats = binaryVC.addBands(ic.select('ndvi').mean())
    .reduceRegion({
      reducer: meanReducer,
      geometry: AOI.geometry(),
      scale: statsScale, 
      crs: 'EPSG:32638', 
      maxPixels: 1e13
    });

  var coverage = ee.Number(stats.get('ndvi_binary')).multiply(100);
  var mean_ndvi = ee.Number(stats.get('ndvi'));

  var sourceNames = ic.aggregate_array('system:index').join(', ');

  // Set QA flag based on image availability 
  var qaFlag = imageCount.gt(0);

  return binaryVC.rename(label).set({
    'period': label,
    'period_number': period_num,
    'image_count': imageCount,
    'source_images': sourceNames,
    'mean_ndvi': mean_ndvi,
    'coverage_percent': coverage,
    'acquisition_start': start.format('YYYY-MM-dd'),
    'acquisition_end': end.format('YYYY-MM-dd'),
    'output_start': start.format('YYYY-MM-dd'),
    'output_end': output_end.format('YYYY-MM-dd'),
    'acquisition_window_days': acquisition_window,
    'QA_flag': qaFlag  
  });
}

// Process bi-weekly mosaics
var biweeklyVC = biweeklyPeriods.map(processPeriod);

// ImageCollection from periods
var biweeklyVC_IC = ee.ImageCollection.fromImages(biweeklyVC);
print('Bi-weekly VC ImageCollection (' + totalPeriods.getInfo() + ' periods):', biweeklyVC_IC);

// ===============================================
// MANUAL 2-BAND CHUNKS SOLUTION
// ===============================================

// Manual period pairs based on months
var periodPairs;
if (month === 12) {
  periodPairs = ['01_02', '03_04', '05_06', '07_08', '09_10', '11_12', '13_14', '15_16', '17_18', '19_20', '21_22', '23_24'];
} else if (month === 11) {
  periodPairs = ['01_02', '03_04', '05_06', '07_08', '09_10', '11_12', '13_14', '15_16', '17_18',  '19_20', '21_22'];
} else if (month === 10) {
  periodPairs = ['01_02', '03_04', '05_06', '07_08', '09_10', '11_12', '13_14', '15_16', '17_18',  '19_20'];
} else if (month === 9) {
  periodPairs = ['01_02', '03_04', '05_06', '07_08', '09_10', '11_12', '13_14', '15_16', '17_18'];
} else if (month === 8) {
  periodPairs = ['01_02', '03_04', '05_06', '07_08', '09_10', '11_12', '13_14', '15_16'];
} else if (month === 7) {
  periodPairs = ['01_02', '03_04', '05_06', '07_08', '09_10', '11_12', '13_14'];
} else if (month === 6) {
  periodPairs = ['01_02', '03_04', '05_06', '07_08', '09_10', '11_12'];
} else if (month === 5) {
  periodPairs = ['01_02', '03_04', '05_06', '07_08', '09_10'];
} else if (month === 4) {
  periodPairs = ['01_02', '03_04', '05_06', '07_08'];
} else if (month === 3) {
  periodPairs = ['01_02', '03_04', '05_06'];
} else if (month === 2) {
  periodPairs = ['01_02', '03_04'];
} else {
  // Default to full year if month not in common values
  periodPairs = ['01_02'];
}

print('Using ' + periodPairs.length + ' period pairs:', periodPairs);

// Export 2-band chunks
for (var i = 0; i < periodPairs.length; i++) {
  var periods = periodPairs[i];
  var startPeriod = i * 2 + 1;
  var endPeriod = i * 2 + 2;
  
  // Filter the collection for these two periods
  var chunk = biweeklyVC_IC.filter(ee.Filter.and(
    ee.Filter.gte('period_number', startPeriod),
    ee.Filter.lte('period_number', endPeriod)
  ));
  
  var chunkImage = chunk.toBands()
    .rename(chunk.aggregate_array('period'))
    .clip(metro)
    .set({
      'year': year,
      'months_processed': month,
      'period_range': periods,
      'data_type': 'VC',
      'threshold': thrash
    });
  
  Export.image.toDrive({
    image: chunkImage,
    description: year + '_BiWeekly_VC_' + periods,
    folder: 'GEE_VC_BiWeekly',
    fileNamePrefix: year + '_BiWeekly_VC_' + periods,
    region: metro.geometry(),
    scale: 10,
    crs: 'EPSG:32638',
    maxPixels: 1e13,
    fileDimensions: [16384, 16384],
    skipEmptyTiles: true
  });
  
  print('Configured export: ' + year + '_BiWeekly_VC_' + periods + '.tif');
}

// ===============================================
// EXPORT METADATA
// ===============================================

var metadataTable = ee.FeatureCollection(
  biweeklyVC_IC.map(function(image) {
    return ee.Feature(null, {
      'Year': year,
      'Months_Processed': month,
      'Period_Number': image.get('period_number'),
      'Period_Label': image.get('period'),
      'Output_Start': image.get('output_start'),
      'Output_End': image.get('output_end'),
      'Acquisition_Start': image.get('acquisition_start'),
      'Acquisition_End': image.get('acquisition_end'),
      'Acquisition_Window_Days': image.get('acquisition_window_days'),
      'Image_Count': image.get('image_count'),
      'QA_Flag': image.get('QA_flag'),
      'Coverage_Percent': image.get('coverage_percent'),
      'Mean_NDVI': image.get('mean_ndvi'),
      'Source_Images': image.get('source_images'),
      'NDVI_Threshold': thrash,
      'Cloud_Cover_Max': cloud
    });
  })
);

Export.table.toDrive({
  collection: metadataTable,
  description: year + '_BiWeekly_VC_Metadata',
  fileFormat: 'CSV',
  folder: 'GEE_VC_BiWeekly',
  fileNamePrefix: year + '_BiWeekly_VC_Metadata'
});

// ===============================================
// FINAL SUMMARY
// ===============================================

print('=== FINAL EXPORT SUMMARY ===');
print('Year: ' + year);
print('Months to process: ' + month);
print('Total periods: ' + (month * 2) + ' (bi-weekly)');
print('Export structure: ' + periodPairs.length + ' files × 2 bands each');
print('File naming: ' + year + '_BiWeekly_VC_XX_YY.tif');
print('Expected outputs:');
for (var i = 0; i < periodPairs.length; i++) {
  print((i + 1) + '. ' + year + '_BiWeekly_VC_' + periodPairs[i] + '.tif');
}
print('Metadata: ' + year + '_BiWeekly_VC_Metadata.csv');
print('All files will be single untiled GeoTIFFs');
