import type { CatalogPlugin, ListContext } from '@data-fair/types-catalogs'
import type { CSWConfig } from '#types'
import type { CswRecord } from './utils/types.ts'
import { XMLParser } from 'fast-xml-parser'
import axios from '@data-fair/lib-node/axios.js'
import capabilities from './capabilities.ts'

type ResourceList = Awaited<ReturnType<CatalogPlugin['list']>>['results']

const parser = new XMLParser({
  ignoreAttributes: false,
  attributeNamePrefix: '',
  removeNSPrefix: true
})

const asArray = (input: any): any[] => {
  if (!input) return []
  return Array.isArray(input) ? input : [input]
}

const getTextValue = (input: any): string => {
  if (!input) return ''
  if (Array.isArray(input)) return input[0] || ''
  return String(input)
}

export const list = async (config: ListContext<CSWConfig, typeof capabilities>): ReturnType<CatalogPlugin<CSWConfig>['list']> => {
  const { catalogConfig, params } = config
  const query = params?.q ? params.q.trim() : ''
  const page = Number(params?.page || 1)
  const size = Number(params?.size || 10)
  const startPosition = (page - 1) * size + 1

  const formatFilter = `
    <ogc:Or>
      <ogc:PropertyIsLike wildCard="%" singleChar="_" escapeChar="\\\\">
        <ogc:PropertyName>AnyText</ogc:PropertyName>
        <ogc:Literal>%SHAPE-ZIP%</ogc:Literal>
      </ogc:PropertyIsLike>
      <ogc:PropertyIsLike wildCard="%" singleChar="_" escapeChar="\\\\">
        <ogc:PropertyName>AnyText</ogc:PropertyName>
        <ogc:Literal>%csv%</ogc:Literal>
      </ogc:PropertyIsLike>
      <ogc:PropertyIsLike wildCard="%" singleChar="_" escapeChar="\\\\">
        <ogc:PropertyName>AnyText</ogc:PropertyName>
        <ogc:Literal>%json%</ogc:Literal>
      </ogc:PropertyIsLike>
      <ogc:PropertyIsLike wildCard="%" singleChar="_" escapeChar="\\\\">
        <ogc:PropertyName>AnyText</ogc:PropertyName>
        <ogc:Literal>%geojson%</ogc:Literal>
      </ogc:PropertyIsLike>
      <ogc:PropertyIsLike wildCard="%" singleChar="_" escapeChar="\\\\">
        <ogc:PropertyName>Protocol</ogc:PropertyName>
        <ogc:Literal>%OGC:WFS%</ogc:Literal>
      </ogc:PropertyIsLike>
    </ogc:Or>`

  const filterBlock = query
    ? `
    <ogc:And>
      <ogc:PropertyIsLike wildCard="%" singleChar="_" escapeChar="\\\\">
        <ogc:PropertyName>AnyText</ogc:PropertyName>
        <ogc:Literal>%${query}%</ogc:Literal>
      </ogc:PropertyIsLike>
      ${formatFilter}
    </ogc:And>`
    : formatFilter

  const constraintBlock = `
    <csw:Constraint version="1.1.0">
      <ogc:Filter>
        ${filterBlock}
      </ogc:Filter>
    </csw:Constraint>
    <ogc:SortBy xmlns:ogc="http://www.opengis.net/ogc">
      <ogc:SortProperty>
        <ogc:PropertyName>RevisionDate</ogc:PropertyName>
        <ogc:SortOrder>DESC</ogc:SortOrder>
      </ogc:SortProperty>
    </ogc:SortBy>`

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
      console.error('[CSW] ERREUR: Réponse XML invalide (pas de GetRecordsResponse)')
      return { count: 0, results: [], path: [] }
    }

    const searchResults = root.SearchResults || root['csw:SearchResults']
    if (!searchResults) {
      console.error('[CSW] ERREUR: Pas de SearchResults')
      return { count: 0, results: [], path: [] }
    }

    const totalCount = parseInt(searchResults.numberOfRecordsMatched || searchResults['numberOfRecordsMatched'] || '0', 10)
    const rawRecords = searchResults.SummaryRecord || searchResults.Record || []
    const records = asArray(rawRecords) as CswRecord[]

    const listResults = records.map((record: any) => {
      const identifier = getTextValue(record.identifier || record['dc:identifier'])
      const titleRecord = getTextValue(record.title || record['dc:title']) || 'Sans titre'
      const dateRaw = record.RevisionDate || record['dct:modified'] || record.dateStamp || record['dateStamp']
      const date = getTextValue(dateRaw)
      const type = getTextValue(record.type || record['dc:type'])
      return {
        id: identifier,
        title: titleRecord,
        updatedAt: date || new Date().toISOString(),
        type: 'resource',
        format: type || 'unknown'
      }
    }) as ResourceList

    return {
      count: totalCount,
      results: listResults,
      path: []
    }
  } catch (error: any) {
    console.error('[CSW] ERREUR :', error.message)
    if (error.response) console.error('Data:', error.response.data)
    throw new Error('Erreur lors de la recherche CSW')
  }
}
