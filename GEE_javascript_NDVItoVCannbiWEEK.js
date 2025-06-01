// Import the batch processing library
var batch = require('users/fitoprincipe/geetools:batch');

// Define spatial extents
var metro = ee.FeatureCollection("projects/ee-dijogergo/assets/METRO");      // Export region (raster composite)
var AOI = ee.FeatureCollection("projects/ee-dijogergo/assets/Metropol_R");   // Analysis region ('CoveragePercent' in csv)

// Parameters
// 1) ~ year
var year = 2020;

// 2) ~ NDVI->VC threshold
var thrash = 0.15;  

// 3) ~ Max cloud cover allowance (%)
var cloud = 15;     

// Cloud masking function for Sentinel-2
function maskS2clouds(image) {
  var qa = image.select('QA60');
  var cloudBitMask = 1 << 10;
  var cirrusBitMask = 1 << 11;
  var mask = qa.bitwiseAnd(cloudBitMask).eq(0)
    .and(qa.bitwiseAnd(cirrusBitMask).eq(0));
  return image.updateMask(mask).divide(10000);
}

// Add NDVI and NDVI threshold mask
function addNDVI(image) {
  var ndvi = image.normalizedDifference(['B8', 'B4']).rename('ndvi').clip(metro);
  var ndviMask = ndvi.gte(thrash).rename('ndvi02');
  return image.addBands([ndvi, ndviMask]);
}

// Bi-weekly date generation
var Date_Start = ee.Date(year + '-01-01');
var Date_End = ee.Date(year + '-12-31');
var months = ee.List.sequence(0, 11);
var biweeklyDates = months.map(function(m) {
  var d1 = ee.Date(Date_Start).advance(m, 'months');
  var d2 = d1.advance(14, 'days');
  return [d1, d2];
}).flatten();

// Process bi-weekly mosaics
var biweeklyVC = biweeklyDates.map(function(date) {
  date = ee.Date(date);
  var start = date;
  var end = date.advance(14, 'days');
  var label = start.format('YYYY-MM-dd');

  var ic = ee.ImageCollection('COPERNICUS/S2_HARMONIZED')
    .filterDate(start, end)
    .filter(ee.Filter.lt('CLOUDY_PIXEL_PERCENTAGE', cloud))
    .filterBounds(metro)
    .map(maskS2clouds)
    .map(addNDVI);

  var sourceNames = ic.aggregate_array('system:index').join(', ');
  var imageCount = ic.size();

  var ndvi02 = ic.select('ndvi02').mosaic().unmask(0).clip(metro);

  var mean_ndvi = ic.select('ndvi').mean().reduceRegion({
    reducer: ee.Reducer.mean(),
    geometry: AOI.geometry(),
    scale: 10,
    maxPixels: 1e13
  }).get('ndvi');

  var coverage = ndvi02.reduceRegion({
    reducer: ee.Reducer.mean(),
    geometry: AOI.geometry(),
    scale: 10,
    maxPixels: 1e13
  }).get('ndvi02');

  // QA Flag: 1 = Good, 0 = Empty or Poor
  var QA_flag = ee.Algorithms.If(imageCount.gt(0), 1, 0);

  return ndvi02.rename(label).set({
    'period': label,
    'image_count': imageCount,
    'source_images': sourceNames,
    'mean_ndvi': mean_ndvi,
    'coverage_percent': ee.Number(coverage).multiply(100).format('%.2f'),
    'QA_flag': QA_flag
  });
});

// ImageCollection from 24 periods
var biweeklyVC_IC = ee.ImageCollection.fromImages(biweeklyVC);
print('Bi-weekly VC ImageCollection:', biweeklyVC_IC);

// Annual stacked image (24 bands)
var annualComposite = biweeklyVC_IC.toBands()
  .rename(biweeklyVC_IC.aggregate_array('period'))
  .clip(metro.geometry())  // Prevent tiling issue
  .set('system:time_start', Date_Start.millis())
  .set('year', year)
  .set('threshold', thrash);

print('Annual Composite (24 bands):', annualComposite);

// Export composite as single GeoTIFF
Export.image.toDrive({
  image: annualComposite,
  description: year + '_Annual_Stacked_VC_24bands',
  folder: 'GEE_VC',
  fileNamePrefix: 'VC_Annual_' + year + '_24bands_thr_' + thrash.toString().replace('.', '_'),
  region: metro.geometry(),  // Use exact feature geometry
  scale: 10,
  crs: 'EPSG:32638',
  maxPixels: 1e13,
  fileFormat: 'GeoTIFF',
  formatOptions: {
    cloudOptimized: true
  }
});

// Export metadata table with QC and QA flag
var metadataTable = ee.FeatureCollection(
  biweeklyVC_IC.map(function(image) {
    return ee.Feature(null, {
      'Period': image.get('period'),
      'QC_ImageCount': image.get('image_count'),
      'QA_Flag': image.get('QA_Flag'),
      'CoveragePercent': image.get('coverage_percent'),
      'SourceImages': image.get('source_images'),
      'MeanNDVI': image.get('mean_ndvi')
    });
  })
);

Export.table.toDrive({
  collection: metadataTable,
  description: year + '_VC_Metadata_with_QC_QA',
  fileFormat: 'CSV',
  folder: 'GEE_VC',
  fileNamePrefix: 'VC_Metadata_QC_QA_' + year,
  selectors: ['Period', 'QC_ImageCount', 'QA_Flag', 'CoveragePercent', 'SourceImages', 'MeanNDVI']
});
