// Import the batch processing library (optional)
var batch = require('users/fitoprincipe/geetools:batch');

// Define main spatial extents
var metro = ee.FeatureCollection("projects/ee-dijogergo/assets/METRO");      // for raster export!!!! //
var AOI   = ee.FeatureCollection("projects/ee-dijogergo/assets/Metropol_R"); // for stats only!!!! //

// Parameters //
// 1) ~ year
var year = 2024;

// 2) ~ NDVI->VC threshold
var thrash = 0.15;

// 3) ~ Max cloudiness allowance
var cloud = 15;

// 4) ~ DateStart and DateEnd
var DateStart = "-01-01";
var DateEnd = "-12-31";

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
var monthlyVC = dates.map(function(dd) {
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

  // Prepare mosaic output using metro clipping
  var mosaic = ndvi02Stack.rename(name)
    .clip(metro)
    .set({
      'month': name,
      'source_images': sourceNames,
      'image_count': imageCount,
      'mean_ndvi': mean_ndvi,
      'coverage_percent': ee.Number(coverage).multiply(100).format('%.2f')
    });

  return mosaic;
});

// Convert to ImageCollection
var monthlyVC_IC = ee.ImageCollection.fromImages(monthlyVC);
print('Monthly VC Collection:', monthlyVC_IC);

// Create annual stacked image with 12 bands
var annualComposite = monthlyVC_IC.toBands()
  .rename(monthlyVC_IC.aggregate_array('month'))
  .clip(metro)
  .set('system:time_start', Date_Start.millis())
  .set('year', year)
  .set('threshold', thrash);
print('Annual Composite:', annualComposite);

// Export annual stacked raster
Export.image.toDrive({
  image: annualComposite,
  description: year + '_Annual_Stacked_VC',
  folder: 'GEE_VC',
  fileNamePrefix: 'VC_Annual_' + year + '_thr_' + thrash.toString().replace('.', '_'),
  region: metro.geometry(),
  scale: 10,
  crs: 'EPSG:32638',
  maxPixels: 1e13
});

// Export metadata table
var metadataTable = ee.FeatureCollection(
  monthlyVC_IC.map(function(image) {
    return ee.Feature(null, {
      'Mosaic': ee.String('VC_').cat(image.get('month')).cat('_').cat(thrash.toString().replace('.', '_')),
      'SourceImages': image.get('source_images'),
      'ImageCount': image.get('image_count'),
      'MeanNDVI': image.get('mean_ndvi'),
      'CoveragePercent': image.get('coverage_percent')
    });
  })
);

Export.table.toDrive({
  collection: metadataTable,
  description: year + '_VC_Metadata_with_QC',
  fileFormat: 'CSV',
  folder: 'GEE_VC',
  fileNamePrefix: 'VC_Metadata_QC_' + year
});
