// Import the batch processing library
var batch = require('users/fitoprincipe/geetools:batch');

// Add the spatial scale (boundaries)
var metro = ee.FeatureCollection("projects/ee-dijogergo/assets/METRO");

// Define the year  
var year = 2024;

// Define the NDVI->VC threshold 
var thrash = 0.2;

// Define max cloud percentage 
var cloud = 15;

// Function to mask clouds using the Sentinel-2 QA band
function maskS2clouds(image) {
    var qa = image.select('QA60');
    var cloudBitMask = 1 << 10;
    var cirrusBitMask = 1 << 11;
    var mask = qa.bitwiseAnd(cloudBitMask).eq(0)
                 .and(qa.bitwiseAnd(cirrusBitMask).eq(0));
    return image.updateMask(mask).divide(10000);
  }

// Date range for the year  
var Date_Start = ee.Date(year + '-01-01');
var Date_End = ee.Date(year + '-12-31');

// Create a list of dates for the time series (monthly)
var n_months = Date_End.difference(Date_Start, 'months').round();
var dates = ee.List.sequence(0, n_months.subtract(1), 1);

var make_datelist = function(n) {
  return Date_Start.advance(n, 'months');
};
dates = dates.map(make_datelist);

// Function to add NDVI bands and threshold setting
function addNDVI(image) {
  var ndvi = image.normalizedDifference(['B8', 'B4']).rename('ndvi').clip(metro);
  var ndvi02 = ndvi.gte(thrash).rename('ndvi02');
  return image.addBands(ndvi02).select('ndvi02'); 
}

// Function to create monthly mosaics
var CreateMosaic = function(dd) {
  var start = ee.Date(dd);
  var end = start.advance(1, 'month');
  var name = start.format('YYYY-MM-dd').cat('_').cat(end.format('YYYY-MM-dd'));
  
  var ic = ee.ImageCollection('COPERNICUS/S2_HARMONIZED')
    .filterDate(start, end)
    .filter(ee.Filter.lt('CLOUDY_PIXEL_PERCENTAGE', cloud))
    .filterBounds(metro)
    .map(maskS2clouds)
    .map(addNDVI);
  
  // Collect original image names
  var sourceNames = ic.aggregate_array('system:index').join(', '); 
  var mosaic = ic.mosaic().set({
    "system:time_start": start.millis(), 
    "system:id": start.format("YYYY-MM-dd"),
    "name": name,
    "range": ee.DateRange(start, end),
    "source_images": sourceNames // Store original image IDs
  });
  
  return mosaic;
};

// Generate NDVI mosaics for each month
var VegCov = ee.ImageCollection(dates.map(CreateMosaic));
print(VegCov);

// Create a feature collection with image IDs for export
var Img = VegCov.map(function(image) {
  var sysid = image.get('system:index');
  var month = image.get('name');
  var sourceImages = image.get('source_images'); // Retrieve original image names
  
  return ee.Feature(null, {
    'Mosaic': ee.String('VC_').cat(month).cat('_').cat(thrash.toString().replace('.', '_')),
    'Sourcenames': sourceImages
  });
});

// Export the IDs to CSV
Export.table.toDrive({
  collection: Img,
  description: year + '_VC_Metadata',
  fileFormat: 'CSV'
});

// Batch export of Vegetation Cover (NDVI >= thrash)
var CRS = 'EPSG:32638';
batch.Download.ImageCollection.toDrive(VegCov, 
  'VC',
  { 
    name: 'VC_{name}_' + thrash.toString().replace('.', '_'),
    scale: 10,
    maxPixels: 15e8,
    region: metro.geometry().getInfo(),
    crs: CRS
  }
);
