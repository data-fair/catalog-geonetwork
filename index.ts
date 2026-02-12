import type CatalogPlugin from '@data-fair/types-catalogs'
import { importConfigSchema, configSchema, assertConfigValid, type MockConfig } from '#types'
import { type MockCapabilities, capabilities } from './lib/capabilities.ts'
import i18n from './lib/i18n.ts'

// Since the plugin is very frequently imported, each function is imported on demand,
// instead of loading the entire plugin.
// This file should not contain any code, but only constants and dynamic imports of functions.

const plugin: CatalogPlugin<MockConfig, MockCapabilities> = {
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

  async publishDataset (context) {
    const { publishDataset } = await import('./lib/publications.ts')
    return publishDataset(context)
  },

  async deletePublication (context) {
    const { deletePublication } = await import('./lib/publications.ts')
    return deletePublication(context)
  },

  metadata: {
    title: 'Mock',
    thumbnailPath: './lib/resources/thumbnail.svg',
    i18n,
    capabilities
  },

  importConfigSchema,
  configSchema,
  assertConfigValid
}
export default plugin
