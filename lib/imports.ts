import { XMLParser } from 'fast-xml-parser'
import path from 'path'
import axios from '@data-fair/lib-node/axios.js'
import type { CatalogPlugin, GetResourceContext, Resource } from '@data-fair/types-catalogs'
import type { CSWConfig } from '#types'
import { downloadFileWithProgress } from './utils/download.ts'

const parser = new XMLParser({
  ignoreAttributes: false,
  attributeNamePrefix: '',
  removeNSPrefix: true,
  parseTagValue: true
})

const asArray = (input: any): any[] => {
  if (!input) return []
  return Array.isArray(input) ? input : [input]
}

const findBestDownloadUrl = (metadata: any, resourceId: string, log: any): { url: string, format: string } | null => {
  const root = metadata.MD_Metadata || metadata
  const distributionInfo = root?.distributionInfo?.MD_Distribution
  if (!distributionInfo) return null

  const transferOptions = asArray(distributionInfo.transferOptions)

  const allLinks: any[] = []
  for (const transfer of transferOptions) {
    const digitalTransfer = transfer.MD_DigitalTransferOptions
    if (digitalTransfer && digitalTransfer.onLine) {
      allLinks.push(...asArray(digitalTransfer.onLine))
    }
  }

  if (allLinks.length === 0) return null

  // Scoring system to determine the best link based on URL, protocol, and name
  const analyzeLink = (link: any) => {
    const url = link.CI_OnlineResource?.linkage?.URL || ''
    const protocol = link.CI_OnlineResource?.protocol?.CharacterString || ''
    const name = link.CI_OnlineResource?.name?.CharacterString || ''

    if (!url) return null

    const u = url.toLowerCase()
    const p = protocol.toLowerCase()
    const n = name.toLowerCase()

    if (u.includes('outputformat=shape-zip') || u.includes('shape-zip') || u.endsWith('.zip') || n.includes('shapefile')) {
      return { url, format: 'shapefile', score: 10 }
    }

    if (u.includes('geojson') || p.includes('geo+json') || n.includes('geojson')) {
      return { url, format: 'geojson', score: 4 }
    }

    if (u.includes('/csv') || u.includes('.csv') || p.includes('text/csv') || n === 'csv') {
      return { url, format: 'csv', score: 6 }
    }

    if ((u.includes('/json') || u.includes('.json') || p.includes('application/json') || n === 'json') && !u.includes('geojson')) {
      return { url, format: 'json', score: 8 }
    }

    if (p.includes('ogc:wfs') || u.includes('service=wfs')) {
      return { url, format: 'wfs_construct', score: 2 }
    }

    return null
  }

  let bestCandidate = null

  for (const link of allLinks) {
    const candidate = analyzeLink(link)
    if (candidate) {
      if (!bestCandidate || candidate.score > bestCandidate.score) {
        bestCandidate = candidate
      }
    }
  }

  if (bestCandidate) {
    let { url, format } = bestCandidate

    if (format === 'wfs_construct') {
      const baseUrl = url.split('?')[0]
      log.info('Service WFS, construction URL')
      const params = new URLSearchParams()
      params.append('SERVICE', 'WFS')
      params.append('VERSION', '2.0.0')
      params.append('REQUEST', 'GetFeature')
      params.append('typeName', resourceId)
      params.append('OUTPUTFORMAT', 'SHAPE-ZIP')
      url = `${baseUrl}?${params.toString()}`
      format = 'shapefile'
    } else if (url.toLowerCase().includes('service=wfs')) {
      url = url
        .replace(/service=[^&]*/i, 'SERVICE=WFS')
        .replace(/version=[^&]*/i, 'VERSION=2.0.0')
        .replace(/request=[^&]*/i, 'REQUEST=GetFeature')
      if (format === 'shapefile') url = url.replace(/outputFormat=[^&]*/i, 'OUTPUTFORMAT=SHAPE-ZIP')
      else if (format === 'csv') url = url.replace(/outputFormat=[^&]*/i, 'OUTPUTFORMAT=csv')
      else if (format === 'geojson') url = url.replace(/outputFormat=[^&]*/i, 'OUTPUTFORMAT=application/json')
    }
    log.info(`Selected download URL: ${url}, format: ${format}`)
    return { url, format }
  }

  return null
}

export const getResource = async ({ importConfig, catalogConfig, resourceId, tmpDir, log }: GetResourceContext<CSWConfig>): ReturnType<CatalogPlugin['getResource']> => {
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
    await log.step('Récupération des métadonnées (CSW)')
    const response = await axios.post(baseUrl, cswBody, {
      headers: { 'Content-Type': 'application/xml' }
    })

    const parsed = parser.parse(response.data)

    const responseRoot = parsed.GetRecordByIdResponse || parsed['csw:GetRecordByIdResponse']
    if (!responseRoot) {
      throw new Error('Réponse CSW vide ou invalide')
    }

    const metadata = responseRoot.MD_Metadata || responseRoot['gmd:MD_Metadata']
    if (!metadata) {
      throw new Error('Métadonnées ISO 19139 introuvables')
    }

    const dataId = metadata.identificationInfo?.MD_DataIdentification || {}
    const titleObj = dataId.citation?.CI_Citation?.title
    const titleRecord = titleObj?.CharacterString || titleObj || resourceId
    const abstract = dataId.abstract?.CharacterString || ''

    const downloadInfo = findBestDownloadUrl(metadata, resourceId, log)

    if (!downloadInfo || !downloadInfo.url) {
      throw new Error(`Aucun lien de téléchargement trouvé pour ${resourceId}`)
    }

    await log.step('Téléchargement du fichier')

    let fileName = `${resourceId}`

    if (downloadInfo.format === 'shapefile') {
      fileName += '.zip'
    } else if (downloadInfo.format === 'geojson') {
      fileName += '.geojson'
    } else if (downloadInfo.format === 'csv') {
      fileName += '.csv'
    } else if (downloadInfo.format === 'json') {
      fileName += '.json'
    } else {
      fileName += path.extname(downloadInfo.url.split('?')[0]) || ''
    }

    const destPath = path.join(tmpDir, fileName)

    const authConfig = importConfig.auth || {}
    const axiosOptions: any = {}
    if (authConfig.username && authConfig.password) {
      axiosOptions.auth = {
        username: authConfig.username,
        password: authConfig.password
      }
    }

    await downloadFileWithProgress(downloadInfo.url, destPath, resourceId, log)

    return {
      id: resourceId,
      title: titleRecord,
      description: abstract,
      filePath: destPath,
      format: downloadInfo.format,
      updatedAt: metadata.dateStamp?.Date || metadata.dateStamp?.DateTime || new Date().toISOString(),
      size: 0
    } as Resource
  } catch (error: any) {
    const msg = error.message || String(error)
    await log.error(`Erreur lors du getResource: ${msg}`)
    throw new Error(`Échec de l'import CSW: ${msg}`)
  }
}
