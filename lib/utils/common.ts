export const asArray = (input: any): any[] => {
  if (!input) return []
  return Array.isArray(input) ? input : [input]
}

export const getText = (input: any): string => {
  if (!input) return ''
  if (typeof input === 'string') return input
  if (input.CharacterString) return input.CharacterString
  if (input['#text']) return input['#text']
  return ''
}
