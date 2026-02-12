import type { CatalogPlugin, GetResourceContext } from '@data-fair/types-catalogs'
import type { MockConfig } from '#types'

export const getResource = async ({ catalogConfig, secrets, resourceId, importConfig, tmpDir, log }: GetResourceContext<MockConfig>): ReturnType<CatalogPlugin['getResource']> => {
  await log.info(`Downloading resource ${resourceId}`, { catalogConfig, secrets, importConfig })

  // Simulate a delay for the mock plugin
  await log.task('delay', 'Simulate delay for mock plugin (Response Delay * 10) ', catalogConfig.delay * 10)
  for (let i = 0; i < catalogConfig.delay * 10; i += catalogConfig.delay) {
    await new Promise(resolve => setTimeout(resolve, catalogConfig.delay))
    await log.progress('delay', i + catalogConfig.delay)
  }

  // Validate the importConfig
  await log.step('Validate import configuraiton')
  const { returnValid } = await import('#type/importConfig/index.ts')
  returnValid(importConfig)
  await log.info('Import configuration is valid', { importConfig })

  // First check if the resource exists
  const resources = (await import('./resources/resources-mock.ts')).default
  const resource = resources.resources[resourceId]
  if (!resource) { throw new Error(`Resource with ID ${resourceId} not found`) }

  // Import necessary modules dynamically
  const fs = await import('node:fs/promises')
  const path = await import('node:path')

  await log.step('Download resource file')
  await log.warning('This task can take a while, please be patient')
  // Simulate downloading by copying a dummy file with limited rows
  const sourceFile = path.join(import.meta.dirname, 'resources', 'dataset-mock.csv')
  const destFile = path.join(tmpDir, 'dataset-mock.csv')
  const data = await fs.readFile(sourceFile, 'utf8')

  // Limit the number of rows to importConfig.nbRows (Header excluded)
  const lines = data.split('\n').slice(0, importConfig.nbRows + 1).join('\n')
  await fs.writeFile(destFile, lines, 'utf8')
  await log.info(`${importConfig.nbRows} rows downloaded`)

  await log.step('End of resource download')
  await log.info(`Resource ${resourceId} downloaded successfully`)
  await log.info(`Resource slug is ${resource.slug}`)
  await log.warning('This is a mock resource, the file is not real and does not contain real data.')
  await log.error('Example of an error log for demonstration purposes.')

  const attachments = []
  if (importConfig.importAttachments) {
    // Copy thumbnail to the tmpDir if it exists
    const thumbnailSource = path.join(import.meta.dirname, 'resources', 'thumbnail.svg')
    const thumbnailDest = path.join(tmpDir, 'thumbnail.svg')
    await fs.copyFile(thumbnailSource, thumbnailDest)
    await log.info(`Thumbnail downloaded to ${thumbnailDest}`)
    attachments.push(
      {
        title: 'Mock Attachment',
        description: 'This is a mock attachment',
        url: 'https://example.com/mock-attachment'
      })
    attachments.push({
      title: 'Another Mock Attachment',
      description: 'This is another mock attachment',
      filePath: thumbnailDest
    })
  } else {
    await log.warning('Attachments import is disabled, no attachments will be imported.')
  }

  return {
    id: resourceId,
    ...resource,
    description: resource.description + '\n\n' + secrets.secretField, // Include the secret in the description for demonstration
    filePath: destFile,
    frequency: 'monthly',
    image: 'https://koumoul.com/data-fair-portals/api/v1/portals/8cbc8974-2fd2-46aa-b328-804600dc840f/assets/logo',
    license: {
      href: 'https://www.etalab.gouv.fr/wp-content/uploads/2014/05/Licence_Ouverte.pdf',
      title: 'Licence Ouverte / Open Licence'
    },
    keywords: ['mock', 'example', 'data'],
    origin: 'https://example.com/mock',
    attachments
  }
}
