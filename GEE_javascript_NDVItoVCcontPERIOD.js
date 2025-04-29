// Import the batch processing library
var batch = require('users/fitoprincipe/geetools:batch');

// Add the spatial scale (boundaries)
var metro = ee.FeatureCollection("projects/ee-dijogergo/assets/METRO"); //"projects/ee-dijogergo/assets/METRO"

// Define the year
var year = 2024;

// Define the NDVI->VC threshold
var thrash = 0.2;

// Define max cloud percentage
var cloud = 15;

// n-day periods
var period = 15;

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
var numberOfDays = Date_End.difference(Date_Start, 'days');  // Get the difference in days (an ee.Number)
var numberOfIntervals = numberOfDays.divide(period).floor(); // Divide by period and round down (up ~ .ceil())
var numberOfIntervalsInt = numberOfIntervals.toInt();        // Convert to an integer for sequence

print('numberOfDays:', numberOfDays);
print('numberOfIntervals:', numberOfIntervals);
print('numberOfIntervalsInt:', numberOfIntervalsInt);

// Create a list of start dates for consecutive n-day periods
var intervalList = ee.List.sequence(0, numberOfIntervalsInt.subtract(1));
var all_start_dates = intervalList.map(function(n) {
  return Date_Start.advance(ee.Number(n).multiply(period), 'days').format('YYYY-MM-dd');
});

// Function to add NDVI bands and threshold setting
function addNDVI(image) {
  var ndvi = image.normalizedDifference(['B8', 'B4']).rename('ndvi').clip(metro);
  var ndvi02 = ndvi.gte(thrash).rename('ndvi02');
  return image.addBands(ndvi02).select('ndvi02');
}

// Function to create 15-day (or shorter at the end) mosaics
var CreateMosaic = function(start_date_str) {
  var start = ee.Date(start_date_str);
  var end = start.advance(period, 'days');
  var endMillis = end.millis();
  var endDateMillis = Date_End.millis();

  // Ensure the end date does not exceed the end of the year
  if (endMillis > endDateMillis) {
    end = Date_End;
  }

  var mosaic_name = start.format('YYYY-MM-dd').cat('_').cat(end.format('YYYY-MM-dd'));

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
    "source_images": sourceNames                          // Store original image IDs
  });

  return mosaic;
};

// Generate NDVI mosaics for each n-day period
var VegCov = ee.ImageCollection(all_start_dates.map(CreateMosaic));
print(VegCov);

// Create a feature collection with image IDs for export
var Img = VegCov.map(function(image) {
  var dateObject = ee.String(image.get('name'));
  var formattedDate = dateObject.replace(ee.String('-'), ee.String('_'));
  var sourceImages = image.get('source_images');          // Retrieve original image names

  return ee.Feature(null, {
    'Date_Range': formattedDate,
    'Sourcenames': sourceImages
  });
});

// Export the IDs to CSV
Export.table.toDrive({
  collection: Img,
  description: year + '_Consecutive_' + period + 'Day_VC_Metadata',
  fileFormat: 'CSV',
  selectors: ['Date_Range', 'Sourcenames']                // Specify the columns to export
});

// Batch export of Vegetation Cover (NDVI >= thrash)
var CRS = 'EPSG:32638'; // 'EPSG:32638' // 'EPSG:4326'
batch.Download.ImageCollection.toDrive(VegCov,
  'VC_Consecutive_15Day',
  {
    name: 'VC_{name}',
    scale: 10,
    maxPixels: 15e8,
    region: metro.geometry().getInfo(),
    crs: CRS
  }
);