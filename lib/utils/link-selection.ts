import axios from '@data-fair/lib-node/axios.js'
import { getText, asArray } from './common.ts'
import type { DownloadCandidate } from './types.ts'

export const isUrlValid = async (url: string, log: any, isWFSTest = false): Promise<boolean> => {
  try {
    if (isWFSTest) {
      const testUrl = new URL(url)
      testUrl.searchParams.set('COUNT', '1')
      testUrl.searchParams.set('MAXFEATURES', '1')

      const response = await axios.get(testUrl.toString(), {
        timeout: 5000,
        validateStatus: (status) => status < 500
      })

      if (response.status >= 400) {
        log.warning(`URL non valide : ${url}`)
        return false
      }
      const content = String(response.data)
      if (content.includes('ExceptionReport') || content.includes('ServiceException')) {
        log.warning(`URL non valide : ${url}`)
        return false
      }
      // if the requested format is JSON but the response is XML, it's likely that the WFS doesn't support the requested format
      const contentType = response.headers['content-type'] || ''
      const requestedFormat = testUrl.searchParams.get('OUTPUTFORMAT') || ''
      if (requestedFormat.includes('json') && contentType.includes('xml')) {
        return false
      }

      return true
    } else {
      await axios.head(url, { timeout: 3000, validateStatus: (s) => s >= 200 && s < 400 })
      return true
    }
  } catch (err) {
    log.warning(`URL non valide : ${url}`)
    return false
  }
}

const negotiateWfsFormat = async (
  originalUrl: string,
  resourceId: string,
  layerName: string | null | undefined,
  log: any
): Promise<{ url: string; format: string } | null> => {
  log.info(`Service WFS détecté sur ${originalUrl}, test des formats supportés...`)

  const [baseUrl, existingQuery] = originalUrl.split('?')
  const params = new URLSearchParams(existingQuery || '')

  const keysToDelete: string[] = []
  for (const key of params.keys()) {
    const lowerKey = key.toLowerCase()
    if (['service', 'request', 'version', 'typename', 'typenames', 'outputformat', 'srsname'].includes(lowerKey)) {
      keysToDelete.push(key)
    }
  }
  keysToDelete.forEach(k => params.delete(k))

  params.set('SERVICE', 'WFS')
  params.set('VERSION', '2.0.0')
  params.set('REQUEST', 'GetFeature')
  params.set('TYPENAMES', layerName || resourceId)

  const formatsToTry = [
    { param: 'application/json; subtype=geojson', format: 'geojson' },
    { param: 'geojson', format: 'geojson' },
    { param: 'application/json', format: 'geojson' },
    { param: 'application/vnd.geo+json', format: 'geojson' },
    { param: 'json', format: 'geojson' },
    { param: 'SHAPE-ZIP', format: 'shapefile' },
    { param: 'shapezip', format: 'shapefile' },
    { param: 'application/zip', format: 'shapefile' },
    { param: 'application/x-shapefile', format: 'shapefile' },
    { param: 'csv', format: 'csv' },
    { param: 'text/csv', format: 'csv' },
    { param: 'kml', format: 'kml' },
    { param: 'application/vnd.google-earth.kml+xml', format: 'kml' }
  ]

  for (const f of formatsToTry) {
    const testParams = new URLSearchParams(params)
    testParams.set('OUTPUTFORMAT', f.param)
    const testUrl = `${baseUrl}?${testParams.toString()}`
    if (await isUrlValid(testUrl, log, true)) {
      log.info(`Format WFS supporté trouvé : ${f.param}. URL finale WFS : ${testUrl}`)
      return {
        url: testUrl,
        format: f.format
      }
    }
  }

  log.error('Ce service WFS ne propose aucun format supporté par DataFair (GeoJSON, Shapefile, KML, CSV)')
  return null
}

const detectFormatFromHeaders = async (url: string, log: any): Promise<string | null> => {
  try {
    const response = await axios.head(url, {
      timeout: 5000,
      validateStatus: (status) => status < 400
    })
    const contentType = response.headers['content-type']
    if (!contentType) return null

    if (contentType.includes('application/json') || contentType.includes('application/geo+json')) {
      return 'geojson'
    }
    if (contentType.includes('application/zip') || contentType.includes('application/x-zip-compressed')) {
      return 'shapefile'
    }
    if (contentType.includes('text/csv') || contentType.includes('application/csv')) {
      return 'csv'
    }
    if (contentType.includes('kml') || contentType.includes('xml')) {
      return 'kml'
    }
    if (contentType.includes('text/tab-separated-values') || contentType.includes('text/tsv')) {
      return 'tsv'
    }

    if (contentType.includes('application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')) {
      return 'xlsx'
    }

    if (contentType.includes('application/vnd.ms-excel')) {
      return 'xls'
    }

    if (contentType.includes('application/vnd.oasis.opendocument.spreadsheet')) {
      return 'ods'
    }

    if (contentType.includes('application/gpx+xml') || contentType.includes('gpx')) {
      return 'gpx'
    }

    if (contentType.includes('application/vnd.google-earth.kmz') || contentType.includes('kmz')) {
      return 'kmz'
    }
    return null
  } catch (error) {
    log.warning(`Impossible de détecter le format pour ${url} (Erreur HTTP).`)
    return null
  }
}

const analyzeLink = (linkWrapper: any): DownloadCandidate | null => {
  const link = linkWrapper.CI_OnlineResource || linkWrapper
  const url = getText(link.linkage?.URL)
  const protocol = getText(link.protocol).toLowerCase()
  const name = getText(link.name)

  if (!url) return null

  const u = url.toLowerCase()
  const p = protocol
  const n = name.toLowerCase()

  if (u.includes('service=wfs') && u.includes('outputformat=')) {
    let format = 'wfs_service'
    const score = 50
    if (u.includes('geojson') || u.includes('json')) format = 'geojson'
    else if (u.includes('csv')) format = 'csv'
    else if (u.includes('zip') || u.includes('shape')) format = 'shapefile'
    return { url, format, score }
  }

  if (u.includes('/api/data/') || u.includes('/api/records/')) {
    return { url, format: 'shapefile', score: 11 }
  }

  // 2. Shapefile (Zip)
  if (u.includes('shape-zip') || u.endsWith('.zip') || n.includes('shapefile')) {
    return { url, format: 'shapefile', score: 8 }
  }

  // 3. GeoJSON
  if (u.includes('geojson') || p.includes('geo+json') || n.includes('geojson')) {
    return { url, format: 'geojson', score: 10 }
  }

  // 4. CSV
  if (u.includes('/csv') || u.includes('.csv') || p.includes('text/csv') || name === 'csv') {
    return { url, format: 'csv', score: 6 }
  }

  // 5. KML
  if (u.endsWith('.kml') || p.includes('kml')) {
    return { url, format: 'kml', score: 4 }
  }

  // 6. JSON
  if ((u.includes('/json') || u.includes('.json') || p.includes('application/json') || n === 'json') && !u.includes('geojson')) {
    return { url, format: 'json', score: 5 }
  }

  // 7. WFS Brut
  if (p.includes('ogc:wfs') || p.includes('wfs') || u.includes('service=wfs')) {
    return { url, format: 'wfs_service', score: 2, layerName: name }
  }

  return { url, format: 'unknown', score: 1 }
}

/**
 * Parses the CSW metadata to find the best download URL based on heuristics and validation.
 * @param metadata The parsed ISO 19139 metadata object
 * @param resourceId The resource ID (used for logging and WFS typeName fallback)
 * @param log The logger object for logging progress and warnings
 * @returns An object containing the best URL and its format, or null if no valid link is found
 */
export const findBestDownloadUrl = async (metadata: any, resourceId: string, log: any): Promise<{ url: string, format: string } | null> => {
  const root = metadata.MD_Metadata || metadata
  const distributionInfo = root?.distributionInfo?.MD_Distribution
  if (!distributionInfo) return null

  const declaredFormats: string[] = []
  if (distributionInfo.distributionFormat) {
    const rawFormats = asArray(distributionInfo.distributionFormat)
    for (const f of rawFormats) {
      const formatName = getText(f.MD_Format?.name).toLowerCase()
      if (formatName) declaredFormats.push(formatName)
    }
  }

  const transferOptions = asArray(distributionInfo.transferOptions)

  const allLinks: any[] = []
  for (const transfer of transferOptions) {
    const digitalTransfer = transfer.MD_DigitalTransferOptions || transfer
    if (digitalTransfer && digitalTransfer.onLine) {
      allLinks.push(...asArray(digitalTransfer.onLine))
    }
  }

  if (allLinks.length === 0) return null

  const candidates: DownloadCandidate[] = []
  for (const link of allLinks) {
    const candidate = analyzeLink(link)
    if (candidate) candidates.push(candidate)
  }

  candidates.sort((a, b) => b.score - a.score)

  if (candidates.length === 0) return null

  log.info(`${candidates.length} liens candidats trouvés. Vérification...`)

  let bestCandidate: DownloadCandidate | null = null
  for (const candidate of candidates) {
    if (candidate.format === 'wfs_service') {
      log.info('lien WFS detecté, validation en cours...')
      if (await isUrlValid(candidate.url, log)) {
        bestCandidate = candidate
        log.info(`Lien WFS validé : ${candidate.url}`)
        break
      } else {
        const wfsResult = await negotiateWfsFormat(candidate.url, resourceId, candidate.layerName, log)
        if (wfsResult) {
          return wfsResult
        }
      }
    } else if (candidate.format !== 'wfs_service') {
      if (await isUrlValid(candidate.url, log)) {
        bestCandidate = candidate
        log.info(`Lien direct validé : ${candidate.url}`)
        break
      }
    } else if (!bestCandidate) {
      bestCandidate = candidate
    }
  }

  if (!bestCandidate) {
    log.warning('Aucun lien n\'a passé le test de validation.')
    return null
  }

  let { url, format } = bestCandidate

  if (url.includes('/api/data/')) {
    const urlObj = new URL(url)
    urlObj.searchParams.delete('format')
    const cleanUrl = urlObj.toString()
    const detectedFormat = await detectFormatFromHeaders(cleanUrl, log)
    if (detectedFormat) {
      url = cleanUrl
      format = detectedFormat
    } else {
      url = cleanUrl
      format = 'shapefile'
    }
  }

  if (format === 'unknown') {
    const detectedFormat = await detectFormatFromHeaders(url, log)
    if (detectedFormat) {
      format = detectedFormat
      log.info(`Format détecté à partir des headers : ${format}`)
    } else {
      log.warning('Aucun format supporté par DataFair (GeoJSON, Shapefile, KML, CSV, XLSX, XLS, ODS, GPX, KMZ)')
      return null
    }
  }

  return { url, format }
}
