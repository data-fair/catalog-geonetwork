import dns from 'dns/promises'
import { URL } from 'url'
import type { CSWConfig } from '#types'
import type { CSWCapabilities } from './capabilities.ts'
import type { PrepareContext } from '@data-fair/types-catalogs'

export default async ({ catalogConfig }: PrepareContext<CSWConfig, CSWCapabilities>) => {
  if (!catalogConfig || !catalogConfig.url) {
    throw new Error("L'URL du catalogue est obligatoire.")
  }
  const urlString = catalogConfig.url.trim()
  let url: URL
  try {
    url = new URL(urlString)
  } catch (err) {
    throw new Error("L'URL fournie n'est pas valide.")
  }
  if (!['http:', 'https:'].includes(url.protocol)) {
    throw new Error('Seuls les protocoles HTTP et HTTPS sont autorisés.')
  }
  try {
    const { address } = await dns.lookup(url.hostname, { family: 4 })
    const isPrivateIp =
      /^127\./.test(address) ||
      /^10\./.test(address) ||
      /^192\.168\./.test(address) ||
      /^169\.254\./.test(address) ||
      /^0\./.test(address) ||
      /^172\.(1[6-9]|2\d|3[0-1])\./.test(address)

    if (isPrivateIp) {
      throw new Error(`L'URL est interdite car elle pointe vers un réseau interne (${address}).`)
    }
  } catch (err: any) {
    if (err.message && err.message.includes('interdite')) throw err
    throw new Error(`Impossible de résoudre l'adresse : ${url.hostname}`)
  }
  catalogConfig.url = url.toString()
  return {
    catalogConfig
  }
}
