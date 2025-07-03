// Import the batch processing library (optional)
var batch = require('users/fitoprincipe/geetools:batch');

// Define main spatial extents
var metro = ee.FeatureCollection("projects/ee-dijogergo/assets/METRO");      // for raster export!!!! //
var AOI   = ee.FeatureCollection("projects/ee-dijogergo/assets/Metropol_R"); // for stats only!!!! //

// Parameters 
// 1) ~ year
var year = 2024;

// 2) ~ NDVI->VC threshold
var thrash = 0.15;

// 3) ~ Max cloudiness allowance
var cloud = 15;

// 4) ~ DateStart and DateEnd
var DateStart = "-01-01";
var DateEnd = "-01-31";

// Mask clouds
function maskS2clouds(image) {
  var qa = image.select('QA60');
  var cloudBitMask = 1 << 10;
  var cirrusBitMask = 1 << 11;
  var mask = qa.bitwiseAnd(cloudBitMask).eq(0)
               .and(qa.bitwiseAnd(cirrusBitMask).eq(0));
  return image.updateMask(mask).divide(10000);
}

// Add NDVI bands
function addNDVI(image) {
  var ndvi = image.normalizedDifference(['B8', 'B4']).rename('ndvi').clip(metro);
  var ndvi02 = ndvi.gte(thrash).rename('ndvi02');
  return image.addBands([ndvi, ndvi02]);
}

// Create monthly date list
var Date_Start = ee.Date(year + DateStart);
var Date_End = ee.Date(year + DateEnd);
var n_months = Date_End.difference(Date_Start, 'months').round();
var dates = ee.List.sequence(0, n_months.subtract(1)).map(function(n) {
  return Date_Start.advance(n, 'month');
});

// Create monthly mosaics with QC
var monthlyMosaics = dates.map(function(dd) {
  var start = ee.Date(dd);
  var end = start.advance(1, 'month');
  var name = start.format('YYYY-MM');

  var ic = ee.ImageCollection('COPERNICUS/S2_HARMONIZED')
    .filterDate(start, end)
    .filter(ee.Filter.lt('CLOUDY_PIXEL_PERCENTAGE', cloud))
    .filterBounds(metro)
    .map(maskS2clouds)
    .map(addNDVI);

  var sourceNames = ic.aggregate_array('system:index').join(', ');
  var imageCount = ic.size();

  var ndviStack = ic.select('ndvi').mosaic();
  var ndvi02Stack = ic.select('ndvi02').mosaic();

  // Compute stats on AOI
  var mean_ndvi = ndviStack.reduceRegion({
    reducer: ee.Reducer.mean(),
    geometry: AOI.geometry(),
    scale: 10,
    maxPixels: 1e13
  }).get('ndvi');

  var coverage = ndvi02Stack.reduceRegion({
    reducer: ee.Reducer.mean(),
    geometry: AOI.geometry(),
    scale: 10,
    maxPixels: 1e13
  }).get('ndvi02');  // Fraction of AOI with NDVI ≥ threshold

  // Prepare both NDVI and VC mosaics using metro clipping
  var ndviMosaic = ndviStack.rename(name)
    .clip(metro)
    .set({
      'month': name,
      'source_images': sourceNames,
      'image_count': imageCount,
      'mean_ndvi': mean_ndvi,
      'coverage_percent': ee.Number(coverage).multiply(100).format('%.2f'),
      'data_type': 'NDVI'
    });
    
  var vcMosaic = ndvi02Stack.rename(name)
    .clip(metro)
    .set({
      'month': name,
      'source_images': sourceNames,
      'image_count': imageCount,
      'mean_ndvi': mean_ndvi,
      'coverage_percent': ee.Number(coverage).multiply(100).format('%.2f'),
      'data_type': 'VC'
    });

  return ee.FeatureCollection([ee.Feature(null, {
    'ndvi_image': ndviMosaic,
    'vc_image': vcMosaic,
    'month': name,
    'source_images': sourceNames,
    'image_count': imageCount,
    'mean_ndvi': mean_ndvi,
    'coverage_percent': ee.Number(coverage).multiply(100).format('%.2f')
  })]);
});

// Flatten the feature collections to get separate lists of NDVI and VC images
var allMosaics = ee.FeatureCollection(monthlyMosaics).flatten();
var ndviImages = allMosaics.aggregate_array('ndvi_image');
var vcImages = allMosaics.aggregate_array('vc_image');

// Convert to ImageCollections
var monthlyNDVI_IC = ee.ImageCollection.fromImages(ndviImages);
var monthlyVC_IC = ee.ImageCollection.fromImages(vcImages);

print('Monthly NDVI Collection:', monthlyNDVI_IC);
print('Monthly VC Collection:', monthlyVC_IC);

// Create annual stacked images with 12 bands (one for each month)
var annualNDVI = monthlyNDVI_IC.toBands()
  .rename(monthlyNDVI_IC.aggregate_array('month'))
  .clip(metro)
  .set('system:time_start', Date_Start.millis())
  .set('year', year)
  .set('data_type', 'NDVI');

var annualVC = monthlyVC_IC.toBands()
  .rename(monthlyVC_IC.aggregate_array('month'))
  .clip(metro)
  .set('system:time_start', Date_Start.millis())
  .set('year', year)
  .set('threshold', thrash)
  .set('data_type', 'VC');

print('Annual NDVI Composite:', annualNDVI);
print('Annual VC Composite:', annualVC);

// Export annual stacked rasters
// Export NDVI
Export.image.toDrive({
  image: annualNDVI,
  description: year + '_Annual_Stacked_NDVI_Export',
  folder: 'GEE_VC',
  fileNamePrefix: 'NDVI_Annual_' + year + '_' + Date_Start.format('YYYY-MM-dd').getInfo() + '_' + Date_End.format('YYYY-MM-dd').getInfo(),
  region: metro.geometry(),
  scale: 10,
  crs: 'EPSG:32638',
  maxPixels: 1e13
});

// Export VC
Export.image.toDrive({
  image: annualVC,
  description: year + '_Annual_Stacked_VC_Export',
  folder: 'GEE_VC',
  fileNamePrefix: 'VC_Annual_' + year + '_thr_' + thrash.toString().replace('.', '_') + '_' + Date_Start.format('YYYY-MM-dd').getInfo() + '_' + Date_End.format('YYYY-MM-dd').getInfo(),
  region: metro.geometry(),
  scale: 10,
  crs: 'EPSG:32638',
  maxPixels: 1e13
});

// Export metadata table (now includes both NDVI and VC info)
var metadataTable = allMosaics.map(function(feature) {
  var month = feature.get('month');
  return ee.Feature(null, {
    'Month': month,
    'DataType_NDVI': 'NDVI',
    'DataType_VC': 'VC',
    'SourceImages': feature.get('source_images'),
    'ImageCount': feature.get('image_count'),
    'MeanNDVI': feature.get('mean_ndvi'),
    'CoveragePercent': feature.get('coverage_percent'),
    'NDVI_Filename': ee.String('NDVI_').cat(month),
    'VC_Filename': ee.String('VC_').cat(month).cat('_thr_').cat(thrash.toString().replace('.', '_'))
  });
});

Export.table.toDrive({
  collection: metadataTable,
  description: year + '_NDVI_VC_Metadata_with_QC',
  fileFormat: 'CSV',
  folder: 'GEE_VC',
  fileNamePrefix: 'NDVI_VC_Metadata_QC_' + year
});
