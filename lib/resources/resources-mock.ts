export default {
  folders: {
    'category-geospatial': {
      title: 'Données Géospatiales',
      resourceIds: []
    },
    'subcategory-transport': {
      title: 'Transport',
      parentId: 'category-geospatial',
      resourceIds: [
        'category-geospatial/subcategory-transport/resource-metro-stations',
        'category-geospatial/subcategory-transport/resource-bus-lines',
        'category-geospatial/subcategory-transport/resource-tram-lines',
        'category-geospatial/subcategory-transport/resource-bike-stations',
        'category-geospatial/subcategory-transport/resource-parking-lots',
        'category-geospatial/subcategory-transport/resource-taxi-stations',
        'category-geospatial/subcategory-transport/resource-traffic-data',
        'category-geospatial/subcategory-transport/resource-road-works',
        'category-geospatial/subcategory-transport/resource-speed-limits',
        'category-geospatial/subcategory-transport/resource-pedestrian-zones',
        'category-geospatial/subcategory-transport/resource-cycle-lanes',
        'category-geospatial/subcategory-transport/resource-public-transport-schedules'
      ]
    },
    'category-economic': {
      title: 'Données Économiques',
      parentId: 'subcategory-transport',
      resourceIds: [
        'category-geospatial/subcategory-transport/category-economic/resource-gdp-data'
      ]
    },
    'subcategory-boundaries': {
      title: 'Délimitations administratives',
      parentId: 'category-geospatial',
      resourceIds: [
        'category-geospatial/subcategory-boundaries/resource-communes'
      ]
    },
    'category-demographic': {
      title: 'Données Démographiques',
      resourceIds: [
        'category-demographic/resource-population-2023'
      ]
    }
  },
  resources: {
    'category-geospatial/subcategory-transport/category-economic/resource-gdp-data': {
      title: 'PIB par région',
      description: 'Produit intérieur brut par région française',
      format: 'json',
      origin: 'https://example.com/gdp-data.json',
      mimeType: 'application/json',
      size: 512000,
      updatedAt: '2025-07-15T10:30:00Z'
    },
    'category-geospatial/subcategory-transport/resource-metro-stations': {
      title: 'Stations de métro Paris',
      description: 'Liste complète des stations de métro parisien avec coordonnées',
      format: 'geojson',
      origin: 'https://example.com/metro-stations.geojson',
      mimeType: 'application/geo+json',
      size: 2048000
    },
    'category-geospatial/subcategory-transport/resource-bus-lines': {
      title: 'Lignes de bus Paris',
      description: 'Tracés des lignes de bus avec horaires',
      format: 'csv',
      origin: 'https://example.com/bus-lines.csv',
      mimeType: 'text/csv',
      size: 1024000,
      updatedAt: new Date('2024-06-01T12:00:00Z').toISOString()
    },
    'category-geospatial/subcategory-transport/resource-tram-lines': {
      title: 'Lignes de tramway Paris',
      description: 'Tracés des lignes de tramway avec arrêts',
      format: 'geojson',
      origin: 'https://example.com/tram-lines.geojson',
      mimeType: 'application/geo+json',
      size: 1536000
    },
    'category-geospatial/subcategory-transport/resource-bike-stations': {
      title: 'Stations Vélib Paris',
      description: 'Emplacements et disponibilités des stations Vélib',
      format: 'json',
      origin: 'https://example.com/velib-stations.json',
      mimeType: 'application/json',
      size: 512000,
      updatedAt: '2025-01-20T08:15:00Z'
    },
    'category-geospatial/subcategory-transport/resource-parking-lots': {
      title: 'Parkings publics Paris',
      description: 'Localisation et capacité des parkings publics',
      format: 'csv',
      origin: 'https://example.com/parking-lots.csv',
      mimeType: 'text/csv',
      size: 768000,
      updatedAt: '2025-03-05T14:00:00Z'
    },
    'category-geospatial/subcategory-transport/resource-taxi-stations': {
      title: 'Stations de taxi Paris',
      description: 'Emplacements des stations de taxi officielles',
      format: 'geojson',
      origin: 'https://example.com/taxi-stations.geojson',
      mimeType: 'application/geo+json',
      size: 256000
    },
    'category-geospatial/subcategory-transport/resource-traffic-data': {
      title: 'Données de trafic temps réel',
      description: 'Informations de trafic en temps réel sur les axes principaux',
      format: 'json',
      origin: 'https://example.com/traffic-data.json',
      mimeType: 'application/json',
      size: 2048000
    },
    'category-geospatial/subcategory-transport/resource-road-works': {
      title: 'Travaux de voirie Paris',
      description: 'Informations sur les travaux en cours et à venir',
      format: 'csv',
      origin: 'https://example.com/road-works.csv',
      mimeType: 'text/csv',
      size: 384000,
      updatedAt: '2025-04-18T07:30:00Z'
    },
    'category-geospatial/subcategory-transport/resource-speed-limits': {
      title: 'Limitations de vitesse',
      description: 'Cartographie des limitations de vitesse par rue',
      format: 'shapefile',
      origin: 'https://example.com/speed-limits.zip',
      mimeType: 'application/zip',
      size: 3072000,
      updatedAt: '2023-12-02T11:20:00Z'
    },
    'category-geospatial/subcategory-transport/resource-pedestrian-zones': {
      title: 'Zones piétonnes Paris',
      description: 'Délimitation des zones réservées aux piétons',
      format: 'geojson',
      origin: 'https://example.com/pedestrian-zones.geojson',
      mimeType: 'application/geo+json',
      size: 1024000,
      updatedAt: '2025-02-12T13:05:00Z'
    },
    'category-geospatial/subcategory-transport/resource-cycle-lanes': {
      title: 'Pistes cyclables Paris',
      description: 'Réseau des pistes cyclables et voies vertes',
      format: 'geojson',
      origin: 'https://example.com/cycle-lanes.geojson',
      mimeType: 'application/geo+json',
      size: 1792000,
      updatedAt: '2025-06-28T18:25:00Z'
    },
    'category-geospatial/subcategory-transport/resource-public-transport-schedules': {
      title: 'Horaires transports publics',
      description: 'Horaires théoriques des lignes de transport public',
      format: 'gtfs',
      origin: 'https://example.com/schedules.zip',
      mimeType: 'application/zip',
      size: 25600000,
      updatedAt: '2025-10-03T05:55:00Z'
    },
    'category-geospatial/subcategory-boundaries/resource-communes': {
      slug: 'communes',
      title: 'Limites communales',
      description: 'Délimitations des communes françaises',
      format: 'shapefile',
      origin: 'https://example.com/communes.zip',
      mimeType: 'application/zip',
      size: 15360000
    },
    'category-demographic/resource-population-2023': {
      slug: 'population-2023',
      title: 'Population par commune 2023',
      description: 'Données démographiques détaillées par commune',
      format: 'xlsx',
      origin: 'https://example.com/population-2023.xlsx',
      mimeType: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
      size: 5120000,
      updatedAt: '2024-08-19T21:40:00Z'
    }
  }
} as {
  folders: Record<string, { title: string; parentId?: string; resourceIds: string[] }>
  resources: Record<string, { slug?: string; title: string; description: string; format: string; origin: string; mimeType: string; size: number; updatedAt?: string }>
}
