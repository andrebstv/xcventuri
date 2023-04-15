import simplekml
from shapely.geometry import Point, Polygon

# Leitura do arquivo KML
kml_file = 'geofence_mg_ES.kml'
kml = simplekml.Kml()
kml.addfile(kml_file)
poligono = kml.features[0]
# Definir o ponto a ser verificado
lat, lon = -17.11, -48.654384
ponto = Point(lon, lat)

# Verificar se o ponto está dentro do polígono
if poligono.geometryType() == 'Polygon':
    polygon = Polygon(poligono.coordinates())
    if polygon.contains(ponto):
        print('O ponto está dentro do polígono.')
    else:
        print('O ponto está fora do polígono.')
else:
    print('Apenas polígonos são suportados.')