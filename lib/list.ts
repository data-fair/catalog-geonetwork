import type { CatalogPlugin, ListContext } from '@data-fair/types-catalogs'
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
  const query = params?.q ? params.q.trim() : ''
  const page = Number(params?.page || 1)
  const size = Number(params?.size || 10)
  const startPosition = (page - 1) * size + 1

  // Bloc of XML filters to find records with relevant formats in their metadata (WFS, GeoJSON, JSON, CSV, ZIP)
  const formatFilter = `
    <ogc:Or>
      <ogc:PropertyIsLike matchCase="false" wildCard="%" singleChar="_" escapeChar="\\">
        <ogc:PropertyName>AnyText</ogc:PropertyName>
        <ogc:Literal>%WFS%</ogc:Literal>
      </ogc:PropertyIsLike>
      
      <ogc:PropertyIsLike matchCase="false" wildCard="%" singleChar="_" escapeChar="\\">
        <ogc:PropertyName>AnyText</ogc:PropertyName>
        <ogc:Literal>%JSON%</ogc:Literal>
      </ogc:PropertyIsLike>

      <ogc:PropertyIsLike matchCase="false" wildCard="%" singleChar="_" escapeChar="\\">
        <ogc:PropertyName>AnyText</ogc:PropertyName>
        <ogc:Literal>%CSV%</ogc:Literal>
      </ogc:PropertyIsLike>

      <ogc:PropertyIsLike matchCase="false" wildCard="%" singleChar="_" escapeChar="\\">
        <ogc:PropertyName>AnyText</ogc:PropertyName>
        <ogc:Literal>%ZIP%</ogc:Literal>
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
      const type = getText(record.type || record['dc:type'])
      return {
        id: identifier,
        title: titleRecord,
        updatedAt: dateRaw || new Date().toISOString(),
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
    console.error('ERREUR :', error.message)
    if (error.response) console.error('Data:', error.response.data)
    throw new Error('Erreur lors de la recherche CSW')
  }
}
