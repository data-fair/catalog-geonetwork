import type CatalogPlugin from '@data-fair/types-catalogs'
import { importConfigSchema, configSchema, assertConfigValid, type GeoNetworkConfig } from '#types'
import { type GeoNetworkCapabilities, capabilities } from './lib/capabilities.ts'

const plugin: CatalogPlugin<GeoNetworkConfig, GeoNetworkCapabilities> = {
  async prepare (context) {
    if (context.catalogConfig.url) {
      let baseUrl = context.catalogConfig.url.split('?')[0].trim()
      if (baseUrl.endsWith('/')) {
        baseUrl = baseUrl.slice(0, -1) // Remove trailing slash
      }
      if (baseUrl.includes('/srv/')) {
        baseUrl = baseUrl.split('/srv/')[0] // Remove anything after /srv/ and including it
      } else if (baseUrl.endsWith('/csw')) {
        baseUrl = baseUrl.substring(0, baseUrl.lastIndexOf('/csw'))
      }
      context.catalogConfig.url = baseUrl
    }
    return context
  },
  async list (context) {
    const { list } = await import('./lib/list.ts')
    return list(context)
  },

  async getResource (context) {
    const { getResource } = await import('./lib/imports.ts')
    return getResource(context)
  },

  metadata: {
    title: 'geoNetwork',
    thumbnailPath: './lib/resources/logo.png',
    i18n: {
      en: { description: 'Uses CSW to import datasets (GeoNetwork, ...)' },
      fr: { description: 'Utilise du CSW pour importer des datasets (GeoNetwork, ...)' }
    },
    capabilities
  },

  importConfigSchema,
  configSchema,
  assertConfigValid
}
export default plugin
