import type CatalogPlugin from '@data-fair/types-catalogs'
import { strict as assert } from 'node:assert'
import { it, describe, before, beforeEach } from 'node:test'
import fs from 'fs-extra'
import { logFunctions } from './test-utils.ts'

// Import plugin and use default type like it's done in Catalogs
import plugin from '../index.ts'
const catalogPlugin: CatalogPlugin = plugin as CatalogPlugin

/** CSW catalog configuration for testing purposes. */
const catalogConfig = {
  url: 'https://geobretagne.fr/CSW',
  delay: 100,
}

const secrets = { secretField: 'Hey' }
const tmpDir = './data/test/downloads'

const getResourceParams = {
  catalogConfig,
  resourceId: '5514e0c5-4406-4880-9cff-4df6f39c0f4f',
  update: { metadata: true, schema: true },
  secrets,
  importConfig: { nbRows: 10 },
  tmpDir,
  log: logFunctions
}

describe('catalog-CSW', () => {
  it('should list resources and folder from root', async () => {
    const res = await catalogPlugin.list({
      catalogConfig,
      secrets,
      params: {}
    })

    assert.ok(res, 'The resource should exist')
  })

  it('should list resources and folder with pagination', { skip: 'This catalog does not support pagination' }, async () => {})

  describe('should download a resource', async () => {
    // Ensure the temporary directory exists once for all tests
    before(async () => await fs.ensureDir(tmpDir))

    // Clear the temporary directory before each test
    beforeEach(async () => await fs.emptyDir(tmpDir))

    it('with correct params', async () => {
      const resource = await catalogPlugin.getResource({
        ...getResourceParams
      })

      assert.ok(resource, 'The resource should exist')

      assert.ok(resource.filePath, 'Download URL should not be undefined')

      // Check if the file exists
      const fileExists = await fs.pathExists(resource.filePath)
      assert.ok(fileExists, 'The downloaded file should exist')
    })

    it('should fail for resource not found', async () => {
      const resourceId = 'non-existent-resource'

      await assert.rejects(
        async () => {
          await catalogPlugin.getResource({
            ...getResourceParams,
            resourceId
          })
        },
        'Should throw an error for non-existent resource'
      )
    })
  })
})
