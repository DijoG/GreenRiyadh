// Import the batch processing library
var batch = require('users/fitoprincipe/geetools:batch');

// Define spatial extents
var metro = ee.FeatureCollection("projects/ee-dijogergo/assets/METRO");
var AOI = ee.FeatureCollection("projects/ee-dijogergo/assets/Metropol_R");

// ===============================================
// Parameters ------------------------------------
// Year to process
var year = 2025;
// Months to process (12 for all months)
var month = 8; 
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

print('Bi-weekly periods created:', biweeklyPeriods.size());
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

  // Calculate statistics with DEFINED scale and CRS 
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
print('Bi-weekly VC ImageCollection (Binary):', biweeklyVC_IC);

// Export tasks - SINGLE BATCH
var exportTasks = [];

// Export ALL periods in ONE batch
var annualCompositeFull = biweeklyVC_IC.toBands()
  .rename(biweeklyVC_IC.aggregate_array('period'))
  .clip(metro)
  .reproject({
    crs: 'EPSG:32638',
    scale: statsScale
  })
  .selfMask() // Mask all zero values
  .set({
    'system:time_start': ee.Date(year + '-01-01').millis(),
    'year': year,
    'threshold': thrash,
    'acquisition_window': acquisition_window
  });

exportTasks.push(Export.image.toDrive({
  image: annualCompositeFull,
  description: 'VCbiwAnnual_' + year + '_' + totalPeriods.getInfo(),
  folder: 'VCbiw',
  fileNamePrefix: 'VCbiwAnnual_' + year + '_' + totalPeriods.getInfo(),
  region: metro.geometry(),
  scale: statsScale,
  crs: 'EPSG:32638',
  maxPixels: 1e13,
  fileFormat: 'GeoTIFF',
  formatOptions: {
    cloudOptimized: true,
    noData: 0
  }
}));

// Export metadata table with QA flag included (single file for all periods)
var metadataTable = ee.FeatureCollection(
  biweeklyVC_IC.map(function(image) {
    return ee.Feature(null, {
      'Period_Number': image.get('period_number'),
      'Period_Label': image.get('period'),
      'Output_Start': image.get('output_start'),
      'Output_End': image.get('output_end'),
      'Acquisition_Start': image.get('acquisition_start'),
      'Acquisition_End': image.get('acquisition_end'),
      'Acquisition_Window_Days': image.get('acquisition_window_days'),
      'QC_ImageCount': image.get('image_count'),
      'QA_Flag': image.get('QA_flag'),
      'Coverage_Percent': image.get('coverage_percent'),
      'Mean_NDVI': image.get('mean_ndvi'),
      'Source_Images': image.get('source_images')
    });
  })
);

exportTasks.push(Export.table.toDrive({
  collection: metadataTable,
  description: 'VCbiwAnnual_' + year + '_' + totalPeriods.getInfo() + '_Metadata',
  fileFormat: 'CSV',
  folder: 'VCbiw',
  fileNamePrefix: 'VCbiwAnnual_' + year + '_' + totalPeriods.getInfo() + '_Metadata',
  selectors: [
    'Period_Number', 'Period_Label', 'Output_Start', 'Output_End',
    'Acquisition_Start', 'Acquisition_End', 'Acquisition_Window_Days',
    'QC_ImageCount', 'QA_Flag', 'Coverage_Percent', 'Mean_NDVI', 'Source_Images'
  ]
}));

// Batch export all tasks
// Uncomment the line below when ready to export
// batch.Task.startAll(exportTasks);

print('Export configured for ' + totalPeriods.getInfo() + ' periods:');
print('1. ' + year + '_Annual_VC_biw_' + totalPeriods.getInfo());
print('2. ' + year + '_Binary_VC_biw_' + totalPeriods.getInfo() + '_Metadata');
print('Uncomment batch.Task.startAll() to begin exports.');