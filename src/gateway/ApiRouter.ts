
import { Hono } from 'hono'

import { readCaptureFile } from '../instances/capturePaths.js'
import type { InstanceInfo } from '../instances/types.js'
import type { MgbaSocketClient } from '../mgba/MgbaSocketClient.js'
import { formatMessage, SUCCESS_MARKER } from '../mgba/protocol.js'
import { type InputAction, type InputLogBus } from '../streaming/InputLog.js'

interface InstanceEntry {
  info: InstanceInfo
  client: MgbaSocketClient
}

export type InstanceRegistry = Map<string, InstanceEntry>

interface ApiVariables {
  entry: InstanceEntry
}

interface ApiEnv {
  Variables: ApiVariables
}

const CONTAINER_CAPTURE_PATH = '/capture/rest-capture.png'
const HOST_CAPTURE_FILE = 'rest-capture.png'

interface ApiRouterOptions {
  authMode?: 'legacy-path-token' | 'principal-token'
  fallbackToSingleInstance?: boolean
  inputLog?: InputLogBus
  onInputCompleted?: (token: string) => void
}

export function createApiRouter(registry: InstanceRegistry, options: ApiRouterOptions = {}): Hono<ApiEnv> {
  const app = new Hono<ApiEnv>()
  const authMode = options.authMode ?? 'legacy-path-token'

  app.use('*', async (c, next) => {
    const entry = authMode === 'principal-token'
      ? resolvePrincipalTokenEntry(registry, c.req.param('sessionId'), c.req.header('X-Principal-Token'), c.req.header('Authorization'))
      : resolveLegacyEntry(registry, c.req.param('token'), options.fallbackToSingleInstance)
    if (entry === undefined) {
      return c.text('Unauthorized', 401)
    }

    c.set('entry', entry)
    await next()
  })

  app.get('/core/currentframe', async (c) => c.text(await send(c.get('entry'), 'core.currentFrame')))

  app.get('/core/read8', async (c) => {
    const address = queryParam(c.req.query('address'), 'address')
    if (!address.ok) {
      return c.text(address.message, 400)
    }

    return c.text(await send(c.get('entry'), 'core.read8', address.value))
  })

  app.get('/core/read16', async (c) => {
    const address = queryParam(c.req.query('address'), 'address')
    if (!address.ok) {
      return c.text(address.message, 400)
    }

    return c.text(await send(c.get('entry'), 'core.read16', address.value))
  })

  app.get('/core/readrange', async (c) => {
    const address = queryParam(c.req.query('address'), 'address')
    if (!address.ok) {
      return c.text(address.message, 400)
    }

    const length = queryParam(c.req.query('length'), 'length')
    if (!length.ok) {
      return c.text(length.message, 400)
    }

    return c.text(await send(c.get('entry'), 'core.readRange', address.value, length.value))
  })

  app.post('/mgba-http/button/tap', async (c) => {
    const button = queryParam(c.req.query('button'), 'button')
    if (!button.ok) {
      return c.text(button.message, 400)
    }

    return c.text(await sendInput(c.get('entry'), options, 'button.tap', button.value, undefined))
  })

  app.post('/mgba-http/button/hold', async (c) => {
    const button = queryParam(c.req.query('button'), 'button')
    if (!button.ok) {
      return c.text(button.message, 400)
    }

    const duration = c.req.query('duration') ?? '15'
    return c.text(await sendInput(c.get('entry'), options, 'button.hold', button.value, duration))
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
    const slot = queryParam(c.req.query('slot'), 'slot')
    if (!slot.ok) {
      return c.text(slot.message, 400)
    }

    return c.text(await send(c.get('entry'), 'core.saveStateSlot', slot.value))
  })

  app.post('/core/loadstateslot', async (c) => {
    const slot = queryParam(c.req.query('slot'), 'slot')
    if (!slot.ok) {
      return c.text(slot.message, 400)
    }

    return c.text(await send(c.get('entry'), 'core.loadStateSlot', slot.value))
  })

  return app
}

export function createV2ApiRouter(registry: InstanceRegistry, options: Omit<ApiRouterOptions, 'authMode' | 'fallbackToSingleInstance'> = {}): Hono<ApiEnv> {
  return createApiRouter(registry, { ...options, authMode: 'principal-token' })
}

function resolveLegacyEntry(
  registry: InstanceRegistry,
  token: string | undefined,
  fallbackToSingleInstance: boolean | undefined,
): InstanceEntry | undefined {
  return token === undefined && fallbackToSingleInstance && registry.size === 1
    ? Array.from(registry.values())[0]
    : token === undefined ? undefined : registry.get(token)
}

function resolvePrincipalTokenEntry(
  registry: InstanceRegistry,
  sessionId: string | undefined,
  principalTokenHeader: string | undefined,
  authorizationHeader: string | undefined,
): InstanceEntry | undefined {
  if (sessionId === undefined) {
    return undefined
  }

  const principalToken = principalTokenHeader ?? bearerToken(authorizationHeader)
  if (principalToken === undefined || principalToken === '') {
    return undefined
  }

  const entry = Array.from(registry.values()).find((candidate) => candidate.info.id === sessionId)
  if (entry?.info.token !== principalToken) {
    return undefined
  }

  return entry
}

function bearerToken(value: string | undefined): string | undefined {
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
  options: ApiRouterOptions,
  action: InputAction,
  button: string,
  duration: string | undefined,
): Promise<string> {
  const command = action === 'button.tap' ? 'mgba-http.button.tap' : 'mgba-http.button.hold'
  const inputEvent = options.inputLog?.beginInput({
    action,
    actorPrincipalId: `token:${entry.info.token}`,
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
        options.onInputCompleted?.(entry.info.token)
      } else {
        options.inputLog?.failInput(inputEvent.eventId, response)
      }
    }
    return response
  } catch (error) {
    if (inputEvent) {
      options.inputLog?.failInput(inputEvent.eventId, error)
    }
    throw error
  }
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
