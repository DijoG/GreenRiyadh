// Import the batch processing library
var batch = require('users/fitoprincipe/geetools:batch');

// Add the spatial scale (boundaries)
var metro = ee.FeatureCollection("projects/ee-dijogergo/assets/AOI_32638"); //"projects/ee-dijogergo/assets/METRO"

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

// Create a list of dates for the time series (bi-weekly)
var n_months = Date_End.difference(Date_Start, 'months').round();
var dates = ee.List.sequence(0, n_months.subtract(1), 1);

var make_datelist_first = function(n) {
  return ee.Date(Date_Start).advance(n, 'months').format('YYYY-MM-dd');
};

var make_datelist_mid = function(n) {
  return ee.Date(Date_Start).advance(n, 'months').advance(14, 'days').format('YYYY-MM-dd');
};

var first_of_month_dates = dates.map(make_datelist_first);
var fifteenth_of_month_dates = dates.map(make_datelist_mid);

var all_start_dates = first_of_month_dates.cat(fifteenth_of_month_dates).distinct().sort();

// Function to add NDVI bands and threshold setting
function addNDVI(image) {
  var ndvi = image.normalizedDifference(['B8', 'B4']).rename('ndvi').clip(metro);
  var ndvi02 = ndvi.gte(thrash).rename('ndvi02');
  return image.addBands(ndvi02).select('ndvi02');
}

// Function to create bi-weekly mosaics
var CreateMosaic = function(start_date_str) {
  var start = ee.Date(start_date_str);
  var end;
  var mosaic_name;

  if (start.get('day') === 1) {
    end = start.advance(14, 'days');
    mosaic_name = start.format('YYYY-MM-dd').cat('_').cat(end.format('YYYY-MM-dd'));
  } else {
    end = start.advance(14, 'days');
    mosaic_name = start.format('YYYY-MM-dd').cat('_').cat(end.format('YYYY-MM-dd'));
  }

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
    "name": mosaic_name,
    "range": ee.DateRange(start, end),
    "source_images": sourceNames // Store original image IDs
  });

  return mosaic;
};

// Generate NDVI mosaics for each bi-weekly period
var VegCov = ee.ImageCollection(all_start_dates.map(CreateMosaic));
print(VegCov);

// Create a feature collection with image IDs for export
var Img = VegCov.map(function(image) {
  var dateObject = ee.String(image.get('name'));
  var formattedDate = dateObject.replace(ee.String('-'), ee.String('_'));
  var sourceImages = image.get('source_images');              // Retrieve original image names

  return ee.Feature(null, {
    'Date_Range': formattedDate,
    'Sourcenames': sourceImages
  });
});

// Export the IDs to CSV
Export.table.toDrive({
  collection: Img,
  description: year + '_BiWeekly_VC_Metadata',
  fileFormat: 'CSV',
  selectors: ['Date_Range', 'Sourcenames']                   // Specify the columns to export
});

// Batch export of Vegetation Cover (NDVI >= thrash)
var CRS = 'EPSG:32638'; // 'EPSG:32638' // 'EPSG:4326'
batch.Download.ImageCollection.toDrive(VegCov,
  'VC_BiWeekly',
  {
    name: 'VC_{name}',
    scale: 10,
    maxPixels: 15e8,
    region: metro.geometry().getInfo(),
    crs: CRS
  }
);