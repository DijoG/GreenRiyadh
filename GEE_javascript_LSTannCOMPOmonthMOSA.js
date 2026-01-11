// =====================================
// Configuration =======================
// =====================================
//--------------------------------------
// AOI 
var metro = ee.FeatureCollection("projects/ee-dijogergo/assets/METRO");

// Year of interest
var year = 2016; 

// Maximum cloud cover percentage
var maxCloudCover = 30; 

// Projection
var CRS = 'EPSG:32638';  // 'EPSG:32638'  'EPSG:4326'
//--------------------------------------
// =====================================


// Scale factor function
function applyScaleFactors(image) {
  var opticalBands = image.select('SR_B.*').multiply(0.0000275).add(-0.2);
  var thermalBands = image.select('ST_B.*').multiply(0.00341802).add(149.0);
  var qaband = image.select('QA_PIXEL');
  return image.addBands(opticalBands, null, true)
              .addBands(thermalBands, null, true)
              .addBands(qaband, null, true);
}

// Cloud mask function
function maskCloud(image) {
  var cloudShadowBitMask = (1 << 3);
  var cloudsBitMask = (1 << 5);
  var qa = image.select('QA_PIXEL');
  var mask = qa.bitwiseAnd(cloudShadowBitMask).eq(0)
               .and(qa.bitwiseAnd(cloudsBitMask).eq(0));
  return image.updateMask(mask);
}

// Function to calculate LST from thermal band
function calculateLST(image) {
  var thermal = image.select('ST_B10');
  var EM = ee.Image.constant(0.986); // Assume emissivity constant for simplicity
  var LST = thermal.expression(
    '(Tb/(1 + (0.00115 * (Tb / 1.438)) * log(Ep)))-273.15', {
    'Tb': thermal,
    'Ep': EM
  }).rename('LST');
  return image.addBands(LST);
}

// List of months to iterate over
var months = ee.List.sequence(1, 12);

// Initialize an empty list to store CSV data
var csvData = ee.List([]);

// Initialize a list to store monthly mosaic images
var monthlyMosaics = [];

// Function to process each month and export
var processMonth = function(month) {
  // Convert month to a client-side number
  month = ee.Number(month).getInfo();
  
  var startDate = ee.Date.fromYMD(year, month, 1);
  var endDate = startDate.advance(1, 'month');
  var formattedDate = startDate.format('YYYY_MM').getInfo();
  
  var dataset = ee.ImageCollection("LANDSAT/LC08/C02/T1_L2")
    .filterMetadata('CLOUD_COVER', 'less_than', maxCloudCover)
    .map(maskCloud)
    .filterDate(startDate, endDate)
    .filterBounds(metro)
    .map(applyScaleFactors)
    .map(calculateLST);
  
  // Create a mosaic for the month (mean composite)
  var monthlyMosaic = dataset.select('LST').mean() //.median() < .mean() is in general better  >
    .rename('LST_' + formattedDate)
    .set('month', month)
    .set('year', year)
    .set('date', formattedDate);
  
  // Clip to AOI
  monthlyMosaic = monthlyMosaic.clip(metro);
  
  // Store the mosaic image for annual composite
  monthlyMosaics.push(monthlyMosaic);
  
  // Export the monthly mosaic
  Export.image.toDrive({
    image: monthlyMosaic,
    description: 'LST_Mosaic_' + formattedDate,
    folder: 'LST_Mosaics',
    fileNamePrefix: 'LST_Mosaic_' + formattedDate,
    region: metro.geometry(),
    scale: 30,
    crs: CRS,
    maxPixels: 1e13
  });
  
  // Collect metadata for CSV
  var metadataList = dataset.map(function(image) {
    return ee.Feature(null, {
      'YYYY_MM': formattedDate,
      'Sourcename': image.get('system:index'),
      'Cloud_cover': image.get('CLOUD_COVER'),
      'Time_stamp': ee.Date(image.get('system:time_start')).format('YYYY-MM-dd HH:mm:ss')
    });
  });
  
  // Add to CSV data list
  csvData = csvData.cat(metadataList.toList(dataset.size()));
  
  return null;
};

// Process all months and then create annual composite
months.evaluate(function(monthList) {
  // Process each month
  monthList.forEach(function(month) {
    processMonth(month);
  });
  
  // Create annual composite from the collected monthly mosaics
  var annualCollection = ee.ImageCollection(monthlyMosaics);
  var annualComposite = annualCollection.toBands();
  
  // Export annual composite
  Export.image.toDrive({
    image: annualComposite,
    description: 'LST_Annual_Composite_' + year,
    folder: 'LST_Annual',
    fileNamePrefix: 'LST_Annual_Composite_' + year,
    region: metro.geometry(),
    scale: 30,
    crs: CRS, 
    maxPixels: 1e13
  });
  
  // Export the metadata CSV
  Export.table.toDrive({
    collection: ee.FeatureCollection(csvData),
    folder: 'LST_Annual',
    description: year + '_LST_Metadata',
    fileFormat: 'CSV'
  });
  
  print('All monthly mosaics and annual composite export tasks have been created.');
});
