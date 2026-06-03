
import { createHash } from 'node:crypto'

import { Hono } from 'hono'

import { readCaptureFile } from '../instances/capturePaths.js'
import type { InstanceInfo } from '../instances/types.js'
import type { MgbaSocketClient } from '../mgba/MgbaSocketClient.js'
import { formatMessage, SUCCESS_MARKER } from '../mgba/protocol.js'
import { type InputAction, type InputLogBus } from '../streaming/InputLog.js'

export interface InstanceEntry {
  info: InstanceInfo
  client: MgbaSocketClient
}

export type InstanceRegistry = Map<string, InstanceEntry>

interface ApiVariables {
  entry: InstanceEntry
  principalId: string
}

interface ApiEnv {
  Variables: ApiVariables
}

const CONTAINER_CAPTURE_PATH = '/capture/rest-capture.png'
const HOST_CAPTURE_FILE = 'rest-capture.png'
const TERMINATION_MARKER = '<|END|>'
const VALID_BUTTONS = new Set(['A', 'B', 'L', 'R', 'Start', 'Select', 'Up', 'Down', 'Left', 'Right'])

export type PrincipalPermission = 'view-stream' | 'view-input-logs' | 'send-key' | 'read-memory' | 'admin-lifecycle'
type PrincipalRole = 'owner' | 'viewer' | 'controller' | 'admin'

interface PrincipalGrant {
  readonly principalId: string
  readonly role: PrincipalRole
  readonly sessionId: string
}

export class PrincipalAccessControl {
  private readonly tokenHashToPrincipal = new Map<string, string>()
  private readonly grants = new Map<string, PrincipalGrant>()

  registerPrincipalToken(principalId: string, token: string): void {
    this.tokenHashToPrincipal.set(hashToken(token), principalId)
  }

  grant(principalId: string, sessionId: string, role: PrincipalRole): void {
    this.grants.set(grantKey(principalId, sessionId), { principalId, role, sessionId })
  }

  authorize(token: string, sessionId: string, permission: PrincipalPermission): string | undefined {
    const principalId = this.tokenHashToPrincipal.get(hashToken(token))
    if (principalId === undefined) {
      return undefined
    }
    const grant = this.grants.get(grantKey(principalId, sessionId))
    if (grant === undefined || !roleAllows(grant.role, permission)) {
      return undefined
    }

    return principalId
  }
}

export interface ApiRouterOptions {
  inputLog?: InputLogBus
  onInputCompleted?: (principalToken: string) => void
  principalAcl?: PrincipalAccessControl
}

export function createApiRouter(registry: InstanceRegistry, options: ApiRouterOptions = {}): Hono<ApiEnv> {
  const app = new Hono<ApiEnv>()

  app.use('*', async (c, next) => {
    const auth = resolvePrincipalTokenEntry(
      registry,
      c.req.param('sessionId'),
      c.req.header('X-Principal-Token'),
      c.req.header('Authorization'),
      routePermission(c.req.path),
      options.principalAcl,
    )
    if (auth === undefined) {
      return c.text('Unauthorized', 401)
    }

    c.set('entry', auth.entry)
    c.set('principalId', auth.principalId)
    await next()
  })

  app.get('/core/currentframe', async (c) => c.text(await send(c.get('entry'), 'core.currentFrame')))

  app.get('/core/read8', async (c) => {
    const address = protocolArg(c.req.query('address'), 'address')
    if (!address.ok) {
      return c.text(address.message, 400)
    }

    return c.text(await send(c.get('entry'), 'core.read8', address.value))
  })

  app.get('/core/read16', async (c) => {
    const address = protocolArg(c.req.query('address'), 'address')
    if (!address.ok) {
      return c.text(address.message, 400)
    }

    return c.text(await send(c.get('entry'), 'core.read16', address.value))
  })

  app.get('/core/readrange', async (c) => {
    const address = protocolArg(c.req.query('address'), 'address')
    if (!address.ok) {
      return c.text(address.message, 400)
    }

    const length = protocolArg(c.req.query('length'), 'length')
    if (!length.ok) {
      return c.text(length.message, 400)
    }

    return c.text(await send(c.get('entry'), 'core.readRange', address.value, length.value))
  })

  app.post('/mgba-http/button/tap', async (c) => {
    const button = buttonParam(c.req.query('button'))
    if (!button.ok) {
      return c.text(button.message, 400)
    }

    const result = await sendInput(c.get('entry'), c.get('principalId'), options, 'button.tap', button.value, undefined)
    return c.text(result.response, 200, controlEventHeaders(result.controlEventId))
  })

  app.post('/mgba-http/button/hold', async (c) => {
    const button = buttonParam(c.req.query('button'))
    if (!button.ok) {
      return c.text(button.message, 400)
    }

    const duration = durationParam(c.req.query('duration') ?? '15')
    if (!duration.ok) {
      return c.text(duration.message, 400)
    }
    const result = await sendInput(c.get('entry'), c.get('principalId'), options, 'button.hold', button.value, duration.value)
    return c.text(result.response, 200, controlEventHeaders(result.controlEventId))
  })

  app.post('/core/screenshot', async (c) => {
    const entry = c.get('entry')
    const response = await send(entry, 'core.screenshot', CONTAINER_CAPTURE_PATH)
    if (response !== SUCCESS_MARKER) {
      return c.text(response, 500)
    }

    try {
      const pngBytes = await readCaptureFile(entry.info.captureDirectory, HOST_CAPTURE_FILE)
      return c.body(new Uint8Array(pngBytes), 200, { 'content-type': 'image/png' })
    } catch {
      return c.text('Failed to read screenshot', 500)
    }
  })

  app.post('/core/savestateslot', async (c) => {
    const slot = slotParam(c.req.query('slot'))
    if (!slot.ok) {
      return c.text(slot.message, 400)
    }

    return c.text(await send(c.get('entry'), 'core.saveStateSlot', slot.value))
  })

  app.post('/core/loadstateslot', async (c) => {
    const slot = slotParam(c.req.query('slot'))
    if (!slot.ok) {
      return c.text(slot.message, 400)
    }

    return c.text(await send(c.get('entry'), 'core.loadStateSlot', slot.value))
  })

  return app
}

export function resolvePrincipalTokenEntry(
  registry: InstanceRegistry,
  sessionId: string | undefined,
  principalTokenHeader: string | undefined,
  authorizationHeader: string | undefined,
  permission: PrincipalPermission,
  principalAcl: PrincipalAccessControl | undefined,
): { entry: InstanceEntry; principalId: string } | undefined {
  if (sessionId === undefined) {
    return undefined
  }

  const principalToken = principalTokenHeader ?? bearerToken(authorizationHeader)
  if (principalToken === undefined || principalToken === '') {
    return undefined
  }

  const entry = Array.from(registry.values()).find((candidate) => candidate.info.id === sessionId)
  if (entry === undefined) {
    return undefined
  }

  const acl = principalAcl ?? defaultPrincipalAcl(registry)
  const principalId = acl.authorize(principalToken, sessionId, permission)
  return principalId === undefined ? undefined : { entry, principalId }
}

export function bearerToken(value: string | undefined): string | undefined {
  const [scheme, token, extra] = value?.split(' ') ?? []
  if (extra !== undefined || scheme?.toLowerCase() !== 'bearer' || token === undefined) {
    return undefined
  }

  return token
}

function send(entry: InstanceEntry, type: string, ...args: string[]): Promise<string> {
  return entry.client.send(formatMessage(type, ...args))
}

async function sendInput(
  entry: InstanceEntry,
  principalId: string,
  options: ApiRouterOptions,
  action: InputAction,
  button: string,
  duration: string | undefined,
): Promise<{ response: string; controlEventId?: string }> {
  const command = action === 'button.tap' ? 'mgba-http.button.tap' : 'mgba-http.button.hold'
  const inputEvent = options.inputLog?.beginInput({
    action,
    actorPrincipalId: principalId,
    button,
    duration,
    sessionId: entry.info.id,
    source: 'http',
  })

  try {
    const response = duration === undefined
      ? await send(entry, command, button)
      : await send(entry, command, button, duration)
    if (inputEvent) {
      if (response === SUCCESS_MARKER) {
        options.inputLog?.completeInput(inputEvent.eventId)
        options.onInputCompleted?.(entry.info.principalToken)
      } else {
        options.inputLog?.failInput(inputEvent.eventId, response)
      }
    }
    return { response, controlEventId: inputEvent?.eventId }
  } catch (error) {
    if (inputEvent) {
      options.inputLog?.failInput(inputEvent.eventId, error)
    }
    throw error
  }
}

function controlEventHeaders(controlEventId: string | undefined): Record<string, string> | undefined {
  return controlEventId === undefined ? undefined : { 'X-Control-Event-Id': controlEventId }
}

type QueryParamResult =
  | { ok: true; value: string }
  | { ok: false; message: string }

function queryParam(value: string | undefined, name: string): QueryParamResult {
  if (value === undefined) {
    return { ok: false, message: `Missing ${name}` }
  }

  return { ok: true, value }
}

function protocolArg(value: string | undefined, name: string): QueryParamResult {
  const parsed = queryParam(value, name)
  if (!parsed.ok) {
    return parsed
  }
  if (!isProtocolSafe(parsed.value)) {
    return { ok: false, message: `Invalid ${name}` }
  }

  return parsed
}

function buttonParam(value: string | undefined): QueryParamResult {
  const parsed = protocolArg(value, 'button')
  if (!parsed.ok) {
    return parsed
  }
  if (!VALID_BUTTONS.has(parsed.value)) {
    return { ok: false, message: 'Invalid button' }
  }

  return parsed
}

function durationParam(value: string | undefined): QueryParamResult {
  const parsed = protocolArg(value, 'duration')
  if (!parsed.ok) {
    return parsed
  }
  if (!/^\d+$/.test(parsed.value)) {
    return { ok: false, message: 'Invalid duration' }
  }

  return parsed
}

function slotParam(value: string | undefined): QueryParamResult {
  const parsed = protocolArg(value, 'slot')
  if (!parsed.ok) {
    return parsed
  }
  if (!/^\d+$/.test(parsed.value)) {
    return { ok: false, message: 'Invalid slot' }
  }

  return parsed
}

function isProtocolSafe(value: string): boolean {
  return value !== '' && !value.includes(',') && !value.includes(TERMINATION_MARKER)
}

function routePermission(path: string): PrincipalPermission {
  if (path.includes('/mgba-http/button/')) {
    return 'send-key'
  }
  if (path.includes('/core/read')) {
    return 'read-memory'
  }
  if (path.includes('/core/savestateslot') || path.includes('/core/loadstateslot')) {
    return 'send-key'
  }
  if (path.includes('/logs')) {
    return 'view-input-logs'
  }
  if (path.includes('/core/currentframe') || path.includes('/core/screenshot')) {
    return 'view-stream'
  }

  return 'admin-lifecycle'
}

export function defaultPrincipalAcl(registry: InstanceRegistry): PrincipalAccessControl {
  const acl = new PrincipalAccessControl()
  for (const entry of registry.values()) {
    const principalId = `session:${entry.info.id}`
    acl.registerPrincipalToken(principalId, entry.info.principalToken)
    acl.grant(principalId, entry.info.id, 'owner')
  }

  return acl
}

function roleAllows(role: PrincipalRole, permission: PrincipalPermission): boolean {
  if (role === 'admin') {
    return true
  }
  if (role === 'owner') {
    return permission !== 'admin-lifecycle'
  }
  if (role === 'controller') {
    return permission === 'view-stream' || permission === 'view-input-logs' || permission === 'send-key' || permission === 'read-memory'
  }

  return permission === 'view-stream' || permission === 'view-input-logs'
}

function grantKey(principalId: string, sessionId: string): string {
  return `${principalId}\u0000${sessionId}`
}

function hashToken(token: string): string {
  return createHash('sha256').update(token).digest('hex')
}
