type CswValue = string | string[] | undefined

export interface CswRecord {
  identifier?: CswValue
  title?: CswValue
  description?: CswValue
  modified?: CswValue
  format?: CswValue
  protocol?: CswValue
  references?: { scheme: string, value: string }[]
}

export interface DownloadCandidate {
  url: string
  format: string
  score: number
}
