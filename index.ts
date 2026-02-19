import type CatalogPlugin from '@data-fair/types-catalogs'
import { configSchema, assertConfigValid, type CSWConfig } from '#types'
import { type CSWCapabilities, capabilities } from './lib/capabilities.ts'

const plugin: CatalogPlugin<CSWConfig, CSWCapabilities> = {
  async prepare (context) {
    const prepare = (await import('./lib/prepare.ts')).default
    return prepare(context)
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
    title: 'CSW',
    thumbnailPath: './lib/resource/logo.svg',
    i18n: {
      en: { description: 'Uses CSW 2.0.2 to import datasets (GeoNetwork, ...)' },
      fr: { description: 'Utilise du CSW 2.0.2 pour importer des datasets (GeoNetwork, ...)' }
    },
    capabilities
  },

  configSchema,
  assertConfigValid
}
export default plugin
