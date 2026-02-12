import type { CatalogPlugin, PublishDatasetContext, DeletePublicationContext } from '@data-fair/types-catalogs'
import type { MockConfig } from '#types'

export const publishDataset = async ({ catalogConfig, dataset, publication, log }: PublishDatasetContext<MockConfig>): ReturnType<CatalogPlugin['publishDataset']> => {
  await log.info(`Starting publication for dataset ${dataset.id}`, { action: publication.action, datasetTitle: dataset.title })

  // Simulate a delay for the mock plugin with progress
  await log.task('delay', 'Simulate delay for mock plugin (Response Delay * 10)', catalogConfig.delay * 10)
  for (let i = 0; i < catalogConfig.delay * 10; i += catalogConfig.delay) {
    await new Promise(resolve => setTimeout(resolve, catalogConfig.delay))
    await log.progress('delay', i + catalogConfig.delay)
  }

  await log.step(`Processing publication action: ${publication.action}`)

  switch (publication.action) {
    case 'createFolderInRoot':
      await log.info('Creating a new folder in root')
      // By default, publication.remoteFolder and publication.remoteResource are undefined.
      // We set publication.remoteFolder here to simulate the creation of a folder in the root.
      publication.remoteFolder = {
        id: `folder-${dataset.id}`,
        title: dataset.title,
        url: `https://example.com/folders/folder-${dataset.slug}`
      }
      await log.info('Folder created successfully in root', { folderId: publication.remoteFolder.id })
      break

    case 'createFolder':
      await log.info('Creating a new folder', { parentFolder: publication.remoteFolder?.id })
      // By default, publication.remoteFolder is the parent folder where the user wants to create the new folder,
      // and publication.remoteResource are undefined.
      // We update publication.remoteFolder to the newly created folder.
      publication.remoteFolder = {
        id: `folder-${dataset.id}`,
        title: dataset.title,
        url: `https://example.com/folders/folder-${dataset.slug}`
      }
      await log.info('Folder created successfully', { folderId: publication.remoteFolder.id })
      break

    case 'createResource':
      await log.info('Creating a new resource', { parentFolder: publication.remoteFolder?.id })
      await log.warning('This is a mock publication, the resource will not contain real data')
      // By default, publication.remoteFolder is the parent folder where the user wants to create the new resource,
      // and publication.remoteResource is undefined.
      // We set publication.remoteResource here to simulate the creation of a resource in the specified folder.
      // The publication.remoteFolder will be removed by catalogs after the publication is done.
      publication.remoteResource = {
        id: `resource-${dataset.id}`,
        title: dataset.title,
        url: `https://example.com/resources/resource-${dataset.slug}`
      }
      await log.info('Resource created successfully', { resourceId: publication.remoteResource.id })
      break

    case 'replaceFolder':
      await log.info('Replacing existing folder', { folderId: publication.remoteFolder?.id })
      // By default, publication.remoteFolder is the folder to replace,
      // and publication.remoteResource is undefined.
      // We update publication.remoteFolder to simulate the folder after being replaced.
      publication.remoteFolder = {
        id: publication.remoteFolder?.id!,
        title: dataset.title,
        url: `https://example.com/folders/${dataset.slug}`
      }
      await log.info('Folder replaced successfully', { folderId: publication.remoteFolder.id })
      await log.warning('Previous folder content has been replaced')
      break

    case 'replaceResource':
      await log.info('Replacing existing resource', { resourceId: publication.remoteResource?.id })
      // By default, publication.remoteResource is the resource to replace,
      // and publication.remoteFolder is undefined.
      // We update publication.remoteResource to simulate the resource after being replaced.
      publication.remoteResource = {
        id: publication.remoteResource?.id!,
        title: dataset.title,
        url: `https://example.com/resources/${dataset.slug}`
      }
      await log.info('Resource replaced successfully', { resourceId: publication.remoteResource.id })
      await log.warning('Previous resource data has been overwritten')
      break

    default:
      await log.error(`Unknown action: ${publication.action}`)
      throw new Error(`Unknown action: ${publication.action}`)
  }

  await log.step('Finalizing publication')
  await log.info('Publication completed successfully')
  await log.warning('This is a mock catalog, the publication is simulated and no real data was published')
  await log.error('Example of an error log for demonstration purposes')

  return publication
}

export const deletePublication = async ({ catalogConfig, folderId, resourceId, log }: DeletePublicationContext<MockConfig>): ReturnType<CatalogPlugin['deletePublication']> => {
  await log.info('Starting deletion', { folderId, resourceId })

  // Simulate a delay for the mock plugin with progress
  await log.task('delay', 'Simulate delay for mock plugin (Response Delay * 10)', catalogConfig.delay * 10)
  for (let i = 0; i < catalogConfig.delay * 10; i += catalogConfig.delay) {
    await new Promise(resolve => setTimeout(resolve, catalogConfig.delay))
    await log.progress('delay', i + catalogConfig.delay)
  }

  await log.step('Processing deletion')

  if (folderId) {
    await log.info(`Deleting folder with ID: ${folderId}`)
    await log.warning('All resources in this folder will be deleted')
  }

  if (resourceId) {
    await log.info(`Deleting resource with ID: ${resourceId}`)
  }

  await log.step('Finalizing deletion')
  await log.info('Deletion completed successfully')
  await log.warning('This is a mock catalog, the deletion is simulated')
}
