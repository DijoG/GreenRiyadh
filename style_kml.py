import simplekml
import random
from bs4 import BeautifulSoup

def style_kml(input_path, output_path, attribute = "ET"):
    """
    This function placemarks randomly colored pins based on categories of an attribute.
    """
    kml = simplekml.Kml()
    
    try:
        with open(input_path, 'r', encoding='utf-8') as f:
            soup = BeautifulSoup(f, 'lxml-xml')
        
        # Find all unique attribute values
        unique_values = set()
        for placemark in soup.find_all('Placemark'):
            ext_data = placemark.find('ExtendedData')
            if ext_data:
                schema_data = ext_data.find('SchemaData')
                if schema_data:
                    for data in schema_data.find_all('SimpleData', {'name': attribute.upper()}):
                        if data.text.strip():
                            unique_values.add(data.text.strip().lower())
        
        print(f"Debug - Found values: {unique_values}")
        
        if not unique_values:
            raise ValueError(f"No values found for attribute '{attribute}'.")
        
        # Generate random but distinct colors
        styles = {}
        used_colors = set()
        
        for i, value in enumerate(sorted(unique_values)):
            # Generate random color ensuring it's distinct
            while True:
                r = random.randint(50, 200)  # Avoid too dark/light colors
                g = random.randint(50, 200)
                b = random.randint(50, 200)
                color_tuple = (r, g, b)
                
                # Ensure color hasn't been used and has sufficient contrast
                if color_tuple not in used_colors:
                    used_colors.add(color_tuple)
                    break
            
            style = simplekml.Style()
            style.iconstyle.icon.href = 'http://maps.google.com/mapfiles/kml/pushpin/wht-pushpin.png'
            style.iconstyle.color = simplekml.Color.rgb(b, g, r)  # KML uses BGR order
            style.iconstyle.scale = 1.0 + (i * 0.1)  # Slightly vary size
            styles[value] = style
        
        # Process placemarks
        for placemark in soup.find_all('Placemark'):
            ext_data = placemark.find('ExtendedData')
            if not ext_data:
                continue
                
            schema_data = ext_data.find('SchemaData')
            if not schema_data:
                continue
                
            attr_data = schema_data.find('SimpleData', {'name': attribute.upper()})
            if not attr_data or not attr_data.text.strip():
                continue
                
            attr_value = attr_data.text.strip().lower()
            coords = placemark.find('coordinates')
            
            if coords and coords.text.strip():
                try:
                    lon, lat = map(float, coords.text.split(',')[:2])
                    point = kml.newpoint(coords=[(lon, lat)])
                    point.name = f"{attribute}: {attr_value}"
                    
                    if attr_value in styles:
                        point.style = styles[attr_value]
                        
                    # Copy all extended data
                    for data in schema_data.find_all('SimpleData'):
                        if data.has_attr('name'):
                            point.extendeddata.newdata(data['name'], data.text)
                except ValueError:
                    continue
        
        kml.save(output_path)
        print(f"Success! Styled KML saved to: {output_path}")
        print(f"Categories found ({len(unique_values)}): {', '.join(sorted(unique_values))}")
        print("Color assignments:")
        for value, style in styles.items():
            color = style.iconstyle.color
            print(f"- {value}: RGB({color[2]}, {color[1]}, {color[0]})")  # Convert back from BGR
        
    except Exception as e:
        print(f"Error: {str(e)}")

if __name__ == "__main__":
    input_file = "D:/BPLA Dropbox/03 Planning/1232-T2-TM2_1-GIS-Remote-Sensing/06_GIS-Data/13_ExceptionalTrees/FromShadeTrees_ET_Public4326Riyadh.kml"
    output_file = "D:/BPLA Dropbox/03 Planning/1232-T2-TM2_1-GIS-Remote-Sensing/06_GIS-Data/13_ExceptionalTrees/FromShadeTrees_ET_Public4326RiyadhTC.kml"
    attribute_name = "ET"
    
    style_kml(input_file, output_file, attribute_name)