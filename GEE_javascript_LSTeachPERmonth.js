// Set up the geometry and parameters
var geometry = ee.FeatureCollection("projects/ee-dijogergo/assets/METRO");

// Maximum cloud cover percentage
var maxCloudCover = 20; 

// Year of interest
var year = 2023; 

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

// List of months to iterate over
var months = ee.List.sequence(1, 12);

// Initialize an empty list to store CSV data
var csvData = ee.List([]);

// Function to process each month
var processMonth = function(month) {
  var startDate = ee.Date.fromYMD(year, month, 1);
  var endDate = startDate.advance(1, 'month');
  var formattedDate = startDate.format('YYYY_MM');

  var dataset = ee.ImageCollection("LANDSAT/LC08/C02/T1_L2")
    .filterMetadata('CLOUD_COVER', 'less_than', maxCloudCover) // Filter by cloud cover
    .map(maskCloud)
    .filterDate(startDate, endDate)
    .filterBounds(geometry)
    .map(applyScaleFactors);

  // Check if the dataset is empty
  var datasetSize = dataset.size();
  print('Dataset size for month ' + month, datasetSize);
  
  if (datasetSize.gt(0).getInfo()) {
    dataset.toList(datasetSize).getInfo().forEach(function(_, index) {
      var image = ee.Image(dataset.toList(datasetSize).get(index)).clip(geometry);
      var sourceName = image.get('system:index');  // Full name of the multispectral image
      var cloudCover = image.get('CLOUD_COVER'); // Cloud percentage
      var timestamp = image.get('system:time_start'); // Timestamp

      // Store YYYY_MM, image ID, cloud cover, and timestamp in CSV list (without geometry)
      var row = ee.Feature(null, {
        'YYYY_MM': formattedDate,
        'Sourcename': sourceName,
        'Cloud_cover': cloudCover,
        'Time_stamp': ee.Date(timestamp).format('YYYY-MM-dd HH:mm:ss')
      });

      csvData = csvData.add(row);

      var thermal = image.select('ST_B10');
      var EM = ee.Image.constant(0.986); // Assume emissivity constant for simplicity
      var LST = thermal.expression(
        '(Tb/(1 + (0.00115 * (Tb / 1.438)) * log(Ep)))-273.15', {
        'Tb': thermal,
        'Ep': EM
      }).rename('LST').clip(geometry);

      // Export LST Image to Google Drive
      var CRS = 'EPSG:32638';
      Export.image.toDrive({
        image: LST,
        description: 'LST_' + formattedDate.getInfo() + '_' + (index + 1),
        region: geometry,
        scale: 30,
        crs: CRS
      });
    });
  } else {
    print('No images found for month ' + month);
  }
};

// Iterate over each month and process
months.evaluate(function(monthList) {
  monthList.forEach(function(month) {
    processMonth(month);
  });

  // Export the combined CSV after processing all months (without geometry)
  var csvDataCollection = ee.FeatureCollection(csvData);

  // Export the table to CSV
  Export.table.toDrive({
    collection: csvDataCollection,
    description: year + '_LST_Metadata',
    fileFormat: 'CSV'
  });
});
