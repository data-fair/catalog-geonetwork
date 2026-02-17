import type CatalogPlugin from '@data-fair/types-catalogs'
import { configSchema, assertConfigValid, type CSWConfig } from '#types'
import { type CSWCapabilities, capabilities } from './lib/capabilities.ts'

const plugin: CatalogPlugin<CSWConfig, CSWCapabilities> = {
  async prepare (context) {
    if (context.catalogConfig.url) {
      context.catalogConfig.url = context.catalogConfig.url.trim()
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
    title: 'CSW',
    i18n: {
      en: { description: 'Uses CSW to import datasets (CSW, ...)' },
      fr: { description: 'Utilise du CSW pour importer des datasets (CSW, ...)' }
    },
    capabilities
  },

  configSchema,
  assertConfigValid
}
export default plugin
