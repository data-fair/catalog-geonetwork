import { XMLParser } from 'fast-xml-parser'
import path from 'path'
import axios from '@data-fair/lib-node/axios.js'
import type { CatalogPlugin, GetResourceContext, Resource } from '@data-fair/types-catalogs'
import type { CSWConfig } from '#types'
import { downloadFileWithProgress } from './utils/download.ts'
import { findBestDownloadUrl } from './utils/link-selection.ts'
import { getText } from './utils/common.ts'

const parser = new XMLParser({
  ignoreAttributes: false,
  attributeNamePrefix: '',
  removeNSPrefix: true,
  parseTagValue: true
})

/**
 * Fetches the metadata for a given resource ID from the CSW endpoint, determines the best download URL, and downloads the file to a temporary directory.
 * @param context The context object containing catalog configuration, resource ID, temporary directory path, and logger
 * @returns An object containing details about the downloaded resource, including title, description, file path, format, and updated date
 */
export const getResource = async ({ catalogConfig, resourceId, tmpDir, log }: GetResourceContext<CSWConfig>): ReturnType<CatalogPlugin['getResource']> => {
  // CSW GetRecordById request body (Requesting full ISO 19139 metadata)
  const cswBody = `
    <csw:GetRecordById 
      xmlns:csw="http://www.opengis.net/cat/csw/2.0.2" 
      xmlns:gmd="http://www.isotc211.org/2005/gmd" 
      service="CSW" 
      version="2.0.2" 
      outputSchema="http://www.isotc211.org/2005/gmd">
      <csw:Id>${resourceId}</csw:Id>
      <csw:ElementSetName>full</csw:ElementSetName>
    </csw:GetRecordById>`

  const baseUrl = catalogConfig.url

  try {
    await log.step('Fetching metadata (CSW)')
    const response = await axios.post(baseUrl, cswBody, {
      headers: { 'Content-Type': 'application/xml' }
    })

    const parsed = parser.parse(response.data)

    // Handle responses with or without namespace prefixes
    const responseRoot = parsed.GetRecordByIdResponse || parsed['csw:GetRecordByIdResponse']
    if (!responseRoot) {
      throw new Error('Invalid or empty CSW response')
    }

    const metadata = responseRoot.MD_Metadata || responseRoot['gmd:MD_Metadata']
    if (!metadata) {
      throw new Error('ISO 19139 Metadata not found')
    }

    // Extract basic info using safe helpers
    const dataId = metadata.identificationInfo?.MD_DataIdentification || {}
    const titleObj = dataId.citation?.CI_Citation?.title
    const titleRecord = getText(titleObj) || resourceId
    const abstract = getText(dataId.abstract)

    // Determine the best URL for file download
    const downloadInfo = await findBestDownloadUrl(metadata, resourceId, log)

    if (!downloadInfo || !downloadInfo.url) {
      throw new Error(`No suitable download link found for ${resourceId}`)
    }

    await log.step('Downloading file')

    // Construct filename based on format
    let fileName = `${resourceId}`
    if (downloadInfo.format === 'shapefile') {
      fileName += '.zip'
    } else if (downloadInfo.format === 'geojson') {
      fileName += '.geojson'
    } else if (downloadInfo.format === 'csv') {
      fileName += '.csv'
    } else if (downloadInfo.format === 'json') {
      fileName += '.json'
    } else if (downloadInfo.format === 'kml') {
      fileName += '.kml'
    } else {
      // Fallback to extension from URL
      fileName += path.extname(downloadInfo.url.split('?')[0]) || ''
    }

    const destPath = path.join(tmpDir, fileName)

    await downloadFileWithProgress(downloadInfo.url, destPath, resourceId, log)

    return {
      id: resourceId,
      title: titleRecord,
      description: abstract,
      filePath: destPath,
      format: downloadInfo.format,
      updatedAt: getText(metadata.dateStamp?.Date || metadata.dateStamp?.DateTime) || new Date().toISOString(),
      size: 0
    } as Resource
  } catch (error: any) {
    throw new Error(error.message || 'Error fetching resource from CSW')
  }
}
