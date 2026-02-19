import type { CatalogPlugin, ListContext, Folder } from '@data-fair/types-catalogs'
import type { CSWConfig } from '#types'
import type { CswRecord } from './utils/types.ts'
import { XMLParser } from 'fast-xml-parser'
import axios from '@data-fair/lib-node/axios.js'
import capabilities from './capabilities.ts'
import { asArray, getText } from './utils/common.ts'

type ResourceList = Awaited<ReturnType<CatalogPlugin['list']>>['results']

const parser = new XMLParser({
  ignoreAttributes: false,
  attributeNamePrefix: '',
  removeNSPrefix: true
})

/**
 * Performs a CSW GetRecords request to list resources based on the provided query and pagination parameters.
 * @param config The context object containing catalog configuration, query parameters, and logger
 * @returns An object containing the total count of matched records, an array of resource summaries, and the path for pagination
 */
export const list = async (config: ListContext<CSWConfig, typeof capabilities>): ReturnType<CatalogPlugin<CSWConfig>['list']> => {
  const { catalogConfig, params } = config
  const currentFolderId = params?.currentFolderId

  if (!currentFolderId) {
    const cswBody = `
      <csw:GetRecords 
        xmlns:csw="http://www.opengis.net/cat/csw/2.0.2" 
        service="CSW" version="2.0.2" resultType="results" 
        startPosition="1" maxRecords="100" 
        outputSchema="http://www.opengis.net/cat/csw/2.0.2">
        <csw:Query typeNames="csw:Record">
          <csw:ElementSetName>brief</csw:ElementSetName>
        </csw:Query>
      </csw:GetRecords>`

    try {
      const response = await axios.post(catalogConfig.url, cswBody, {
        headers: { 'Content-Type': 'application/xml' }
      })

      const parsed = parser.parse(response.data)
      const root = parsed.GetRecordsResponse || parsed['csw:GetRecordsResponse']
      const searchResults = root?.SearchResults || root?.['csw:SearchResults']
      const rawRecords = searchResults?.BriefRecord || searchResults?.SummaryRecord || searchResults?.Record || []
      const records = asArray(rawRecords)

      const typesSet = new Set<string>()
      records.forEach((record: any) => {
        const typeStr = getText(record.type || record['dc:type'])
        if (typeStr && typeStr !== 'unknown') {
          typesSet.add(typeStr.toLowerCase())
        }
      })
      const folders = Array.from(typesSet).map(type => ({
        id: type,
        title: type.toUpperCase(),
        type: 'folder'
      } as Folder))

      if (folders.length === 0) {
        folders.push({ id: 'all', title: 'TOUS LES DOCUMENTS', type: 'folder' as const })
      }
      return {
        count: folders.length,
        results: folders,
        path: []
      }
    } catch (error: any) {
      console.error('Erreur GetDomain:', error.message)
      return { count: 0, results: [], path: [] }
    }
  }
  const query = params?.q ? params.q.trim() : ''
  const page = Number(params?.page || 1)
  const size = Number(params?.size || 10)
  const startPosition = (page - 1) * size + 1

  const typeFilter = `
    <ogc:PropertyIsEqualTo>
      <ogc:PropertyName>dc:type</ogc:PropertyName> <ogc:Literal>${currentFolderId}</ogc:Literal>
    </ogc:PropertyIsEqualTo>`

  const filterContent = query
    ? `
      <ogc:And>
        ${typeFilter}
        <ogc:PropertyIsLike wildCard="%" singleChar="_" escapeChar="\\\\">
          <ogc:PropertyName>AnyText</ogc:PropertyName>
          <ogc:Literal>%${query}%</ogc:Literal>
        </ogc:PropertyIsLike>
      </ogc:And>`
    : typeFilter

  const constraintBlock = `
    <csw:Constraint version="1.1.0">
      <ogc:Filter>
        ${filterContent}
      </ogc:Filter>
    </csw:Constraint>`

  const cswBody = `
    <csw:GetRecords 
      xmlns:csw="http://www.opengis.net/cat/csw/2.0.2" 
      xmlns:ogc="http://www.opengis.net/ogc" 
      service="CSW" 
      version="2.0.2" 
      resultType="results" 
      startPosition="${startPosition}" 
      maxRecords="${size}" 
      outputSchema="http://www.opengis.net/cat/csw/2.0.2">
      <csw:Query typeNames="csw:Record">
        <csw:ElementSetName>summary</csw:ElementSetName>
        ${constraintBlock}
      </csw:Query>
    </csw:GetRecords>`

  try {
    const baseUrl = catalogConfig.url

    const response = await axios.post(baseUrl, cswBody, {
      headers: { 'Content-Type': 'application/xml' }
    })

    const parsed = parser.parse(response.data)
    const root = parsed.GetRecordsResponse || parsed['csw:GetRecordsResponse']
    if (!root) {
      console.error('Réponse XML invalide (pas de GetRecordsResponse)')
      return { count: 0, results: [], path: [] }
    }

    const searchResults = root.SearchResults || root['csw:SearchResults']
    if (!searchResults) {
      console.error('Pas de SearchResults')
      return { count: 0, results: [], path: [] }
    }

    const totalCount = parseInt(searchResults.numberOfRecordsMatched || searchResults['numberOfRecordsMatched'] || '0', 10)
    const rawRecords = searchResults.SummaryRecord || searchResults.Record || []
    const records = asArray(rawRecords) as CswRecord[]

    const listResults = records.map((record: any) => {
      const identifier = getText(record.identifier || record['dc:identifier'])
      const titleRecord = getText(record.title || record['dc:title']) || 'Sans titre'
      const rawDateObj = record.modified || record.date || record.dateStamp || record.RevisionDate
      const dateRaw = getText(rawDateObj)
      return {
        id: identifier,
        title: titleRecord,
        updatedAt: dateRaw || new Date().toISOString(),
        type: 'resource',
        format: currentFolderId
      }
    }) as ResourceList

    return {
      count: totalCount,
      results: listResults,
      path: [
        { id: currentFolderId, title: currentFolderId.toUpperCase(), type: 'folder' }
      ]
    }
  } catch (error: any) {
    console.error('ERREUR :', error.message)
    if (error.response) console.error('Data:', error.response.data)
    throw new Error('Erreur lors de la recherche CSW')
  }
}
