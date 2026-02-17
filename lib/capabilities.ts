import type { Capability } from '@data-fair/types-catalogs'

/**
 * The list of capabilities of the plugin.
 * These capabilities define the actions that can be performed with the plugin.
 * The capabilities must satisfy the `Capability` type.
 */
export const capabilities = [
  'search',
  'pagination',

  'import',
] satisfies Capability[]

export type CSWCapabilities = typeof capabilities
export default capabilities
