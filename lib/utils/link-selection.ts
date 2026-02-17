import axios from '@data-fair/lib-node/axios.js'
import { getText, asArray } from './common.ts'

interface DownloadCandidate {
  url: string
  format: string
  score: number
}

export const isUrlValid = async (url: string, log: any): Promise<boolean> => {
  try {
    await axios.head(url, {
      timeout: 5000,
      validateStatus: (status) => status >= 200 && status < 400
    })
    return true
  } catch (err: any) {
    log.warning(`Lien testé inaccessible : ${url}`)
    return false
  }
}

const analyzeLink = (linkWrapper: any): DownloadCandidate | null => {
  const link = linkWrapper.CI_OnlineResource || linkWrapper
  const url = getText(link.linkage?.URL)
  const protocol = getText(link.protocol).toLowerCase()
  const name = getText(link.name).toLowerCase()

  if (!url) return null

  const u = url.toLowerCase()
  const p = protocol
  const n = name

  if (u.includes('service=wfs') && u.includes('outputformat=')) {
    let format = 'wfs_service'
    const score = 50
    if (u.includes('geojson') || u.includes('json')) format = 'geojson'
    else if (u.includes('csv')) format = 'csv'
    else if (u.includes('zip') || u.includes('shape')) format = 'shapefile'
    return { url, format, score }
  }

  // 2. Shapefile (Zip)
  if (u.includes('shape-zip') || u.endsWith('.zip') || n.includes('shapefile')) {
    return { url, format: 'shapefile', score: 10 }
  }

  // 3. GeoJSON
  if (u.includes('geojson') || p.includes('geo+json') || n.includes('geojson')) {
    return { url, format: 'geojson', score: 8 }
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

  // 7. WFS Brut (à construire)
  if (p.includes('ogc:wfs') || p.includes('wfs') || u.includes('service=wfs')) {
    return { url, format: 'wfs_service', score: 2 }
  }

  // 8. Download générique
  if (p.includes('download') || p.includes('file')) {
    return { url, format: 'file', score: 1 }
  }

  return null
}

export const findBestDownloadUrl = async (metadata: any, resourceId: string, log: any): Promise<{ url: string, format: string } | null> => {
  const root = metadata.MD_Metadata || metadata
  const distributionInfo = root?.distributionInfo?.MD_Distribution
  if (!distributionInfo) return null

  const transferOptions = asArray(distributionInfo.transferOptions)

  const allLinks: any[] = []
  for (const transfer of transferOptions) {
    const digitalTransfer = transfer.MD_DigitalTransferOptions || transfer
    if (digitalTransfer && digitalTransfer.onLine) {
      allLinks.push(...asArray(digitalTransfer.onLine))
    }
  }

  if (allLinks.length === 0) return null

  // Collecte et tri
  const candidates: DownloadCandidate[] = []
  for (const link of allLinks) {
    const candidate = analyzeLink(link)
    if (candidate) candidates.push(candidate)
  }

  // Tri décroissant par score
  candidates.sort((a, b) => b.score - a.score)

  if (candidates.length === 0) return null

  log.info(`${candidates.length} liens candidats trouvés. Vérification...`)

  // Sélection du meilleur lien valide (Failover)
  let bestCandidate: DownloadCandidate | null = null

  for (const candidate of candidates) {
    const isValid = await isUrlValid(candidate.url, log)
    if (isValid) {
      bestCandidate = candidate
      log.info(`Lien validé : ${candidate.url}`)
      break
    }
  }

  if (!bestCandidate) {
    log.warning('Aucun lien n\'a passé le test de validation.')
    return null
  }

  let { url, format } = bestCandidate

  if (format === 'wfs_service' && !url.toLowerCase().includes('outputformat=')) {
    log.info(`Construction URL WFS pour ${url}`)
    try {
      const urlObj = new URL(url)
      urlObj.searchParams.set('SERVICE', 'WFS')
      urlObj.searchParams.set('VERSION', '2.0.0')
      urlObj.searchParams.set('REQUEST', 'GetFeature')
      if (!urlObj.searchParams.get('TYPENAME') && !urlObj.searchParams.get('typeName')) {
        urlObj.searchParams.set('typeName', resourceId)
      }
      urlObj.searchParams.set('OUTPUTFORMAT', 'application/json; subtype=geojson')
      url = urlObj.toString()
      format = 'geojson'
    } catch (e) {
      log.warning('Echec construction URL WFS, utilisation brute')
    }
  }

  return { url, format }
}
