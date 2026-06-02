import { Hono } from 'hono'
import { beforeEach, describe, expect, it, vi } from 'vitest'

import {
  PrincipalAccessControl,
  createApiRouter,
  createV2ApiRouter,
  type InstanceRegistry,
} from '../src/gateway/ApiRouter.js'
import { MgbaSocketClient } from '../src/mgba/MgbaSocketClient.js'
import { formatMessage, SUCCESS_MARKER } from '../src/mgba/protocol.js'
import { InputLogBus } from '../src/streaming/InputLog.js'

const REST_CAPTURE_OPEN_CALL = /^open:\/tmp\/grokemon-captures-test\/instance-1\/rest-capture\.png:\d+$/

const readFileMock = vi.hoisted(() => {
  const directoryStats = { isDirectory: () => true }
  const fileStats = { dev: 1, ino: 2, isFile: () => true }
  let response: Buffer<ArrayBufferLike> = Buffer.from([])
  let failure: Error | undefined
  const calls: string[] = []

  return {
    calls,
    lstat(path: string) {
      calls.push(`lstat:${path}`)
      if (failure) {
        return Promise.reject(failure)
      }
      return Promise.resolve(path.endsWith('/instance-1') ? directoryStats : fileStats)
    },
    open(path: string, flags?: number) {
      calls.push(`open:${path}:${flags ?? ''}`)
      if (failure) {
        return Promise.reject(failure)
      }
      return Promise.resolve({
        close: () => Promise.resolve(),
        readFile: () => Promise.resolve(response),
        stat: () => Promise.resolve(fileStats),
      })
    },
    reset() {
      calls.length = 0
      response = Buffer.from([])
      failure = undefined
    },
    setFailure(error: Error) {
      failure = error
    },
    setResponse(next: Buffer<ArrayBufferLike>) {
      response = next
    },
  }
})

vi.mock('node:fs/promises', () => ({
  lstat: readFileMock.lstat,
  open: readFileMock.open,
}))

const TOKEN = '0123456789abcdef0123456789abcdef'
const CONTAINER_ID = 'container-123'

type HttpMethod = 'GET' | 'POST'

interface TextEndpointCase {
  name: string
  method: HttpMethod
  path: string
  socketMessage: string
  socketResponse: string
  body: string
}

interface Fixture {
  app: Hono
  messages: string[]
}

describe('createApiRouter', () => {
  beforeEach(() => {
    readFileMock.reset()
  })

  const textCases: TextEndpointCase[] = [
    {
      name: 'GET /core/currentframe',
      method: 'GET',
      path: '/api/v1/0123456789abcdef0123456789abcdef/core/currentframe',
      socketMessage: formatMessage('core.currentFrame'),
      socketResponse: '12345',
      body: '12345',
    },
    {
      name: 'GET /core/read8',
      method: 'GET',
      path: '/api/v1/0123456789abcdef0123456789abcdef/core/read8?address=0xD35E',
      socketMessage: formatMessage('core.read8', '0xD35E'),
      socketResponse: '12',
      body: '12',
    },
    {
      name: 'GET /core/read16',
      method: 'GET',
      path: '/api/v1/0123456789abcdef0123456789abcdef/core/read16?address=0xD35E',
      socketMessage: formatMessage('core.read16', '0xD35E'),
      socketResponse: '3456',
      body: '3456',
    },
    {
      name: 'GET /core/readrange',
      method: 'GET',
      path: '/api/v1/0123456789abcdef0123456789abcdef/core/readrange?address=0xD35E&length=3',
      socketMessage: formatMessage('core.readRange', '0xD35E', '3'),
      socketResponse: '0a,1b,2c',
      body: '0a,1b,2c',
    },
    {
      name: 'POST /mgba-http/button/tap',
      method: 'POST',
      path: '/api/v1/0123456789abcdef0123456789abcdef/mgba-http/button/tap?button=A',
      socketMessage: formatMessage('mgba-http.button.tap', 'A'),
      socketResponse: SUCCESS_MARKER,
      body: SUCCESS_MARKER,
    },
    {
      name: 'POST /mgba-http/button/hold',
      method: 'POST',
      path: '/api/v1/0123456789abcdef0123456789abcdef/mgba-http/button/hold?button=B&duration=15',
      socketMessage: formatMessage('mgba-http.button.hold', 'B', '15'),
      socketResponse: SUCCESS_MARKER,
      body: SUCCESS_MARKER,
    },
    {
      name: 'POST /mgba-http/button/hold default duration',
      method: 'POST',
      path: '/api/v1/0123456789abcdef0123456789abcdef/mgba-http/button/hold?button=Start',
      socketMessage: formatMessage('mgba-http.button.hold', 'Start', '15'),
      socketResponse: SUCCESS_MARKER,
      body: SUCCESS_MARKER,
    },
    {
      name: 'POST /core/savestateslot',
      method: 'POST',
      path: '/api/v1/0123456789abcdef0123456789abcdef/core/savestateslot?slot=2',
      socketMessage: formatMessage('core.saveStateSlot', '2'),
      socketResponse: SUCCESS_MARKER,
      body: SUCCESS_MARKER,
    },
    {
      name: 'POST /core/loadstateslot',
      method: 'POST',
      path: '/api/v1/0123456789abcdef0123456789abcdef/core/loadstateslot?slot=2',
      socketMessage: formatMessage('core.loadStateSlot', '2'),
      socketResponse: SUCCESS_MARKER,
      body: SUCCESS_MARKER,
    },
  ]

  for (const endpoint of textCases) {
    it(`${endpoint.name} sends the Lua command and returns mGBA-http text`, async () => {
      const fixture = createFixture(new Map([[endpoint.socketMessage, endpoint.socketResponse]]))

      const response = await fixture.app.request(endpoint.path, { method: endpoint.method })

      expect(response.status).toBe(200)
      expect(response.headers.get('content-type')).toContain('text/plain')
      expect(await response.text()).toBe(endpoint.body)
      expect(fixture.messages).toEqual([endpoint.socketMessage])
    })
  }

  it('returns 401 for an unknown token before route parameters are read', async () => {
    const fixture = createFixture(new Map([[formatMessage('core.read8', '0xD35E'), '12']]))

    const response = await fixture.app.request('/api/v1/deadbeefdeadbeefdeadbeefdeadbeef/core/read8')

    expect(response.status).toBe(401)
    expect(await response.text()).toBe('Unauthorized')
    expect(fixture.messages).toEqual([])
  })

  it('can route tokenless root requests to the only registered instance when enabled', async () => {
    const socketMessage = formatMessage('core.currentFrame')
    const fixture = createFixture(new Map([[socketMessage, '12345']]), { fallbackToSingleInstance: true })

    const response = await fixture.app.request('/core/currentframe')

    expect(response.status).toBe(200)
    expect(await response.text()).toBe('12345')
    expect(fixture.messages).toEqual([socketMessage])
  })

  it('routes v2 session requests with the principal token header as the primary contract', async () => {
    const socketMessage = formatMessage('core.currentFrame')
    const fixture = createFixture(new Map([[socketMessage, '12345']]), { includeV2: true })

    const response = await fixture.app.request('/api/v2/sessions/instance-1/core/currentframe', {
      headers: { 'X-Principal-Token': TOKEN },
    })

    expect(response.status).toBe(200)
    expect(await response.text()).toBe('12345')
    expect(fixture.messages).toEqual([socketMessage])
  })

  it('accepts bearer principal tokens on v2 session requests', async () => {
    const socketMessage = formatMessage('core.currentFrame')
    const fixture = createFixture(new Map([[socketMessage, '12345']]), { includeV2: true })

    const response = await fixture.app.request('/api/v2/sessions/instance-1/core/currentframe', {
      headers: { Authorization: `Bearer ${TOKEN}` },
    })

    expect(response.status).toBe(200)
    expect(await response.text()).toBe('12345')
    expect(fixture.messages).toEqual([socketMessage])
  })

  it('keeps v2 principal tokens scoped to the requested session', async () => {
    const socketMessage = formatMessage('core.currentFrame')
    const fixture = createFixture(new Map([[socketMessage, '12345']]), { includeV2: true })

    const response = await fixture.app.request('/api/v2/sessions/other-instance/core/currentframe', {
      headers: { 'X-Principal-Token': TOKEN },
    })

    expect(response.status).toBe(401)
    expect(await response.text()).toBe('Unauthorized')
    expect(fixture.messages).toEqual([])
  })

  it('keeps legacy path token routing as a compatibility adapter', async () => {
    const socketMessage = formatMessage('core.currentFrame')
    const fixture = createFixture(new Map([[socketMessage, '12345']]), { includeV2: true })

    const response = await fixture.app.request(
      '/api/v1/0123456789abcdef0123456789abcdef/core/currentframe',
    )

    expect(response.status).toBe(200)
    expect(await response.text()).toBe('12345')
    expect(fixture.messages).toEqual([socketMessage])
  })

  it('logs v2 input actors by principal id without leaking bearer tokens', async () => {
    const inputLog = new InputLogBus()
    const socketMessage = formatMessage('mgba-http.button.tap', 'A')
    const fixture = createFixture(new Map([[socketMessage, SUCCESS_MARKER]]), {
      includeV2: true,
      inputLog,
    })

    const response = await fixture.app.request('/api/v2/sessions/instance-1/mgba-http/button/tap?button=A', {
      method: 'POST',
      headers: { Authorization: `Bearer ${TOKEN}` },
    })

    expect(response.status).toBe(200)
    const controlEventId = response.headers.get('X-Control-Event-Id')
    expect(controlEventId).toBeTruthy()
    const events = inputLog.recent('instance-1')
    expect(events).toHaveLength(2)
    expect(events[1]?.eventId).toBe(controlEventId)
    expect(events[1]?.actorPrincipalId).toBe('session:instance-1')
    expect(JSON.stringify(events)).not.toContain(TOKEN)
  })

  it('enforces v2 grants before sending memory or key commands', async () => {
    const acl = new PrincipalAccessControl()
    acl.registerPrincipalToken('viewer-a', TOKEN)
    acl.grant('viewer-a', 'instance-1', 'viewer')
    const socketMessage = formatMessage('core.read8', '0xD35E')
    const fixture = createFixture(new Map([[socketMessage, '12']]), {
      includeV2: true,
      principalAcl: acl,
    })

    const response = await fixture.app.request('/api/v2/sessions/instance-1/core/read8?address=0xD35E', {
      headers: { 'X-Principal-Token': TOKEN },
    })

    expect(response.status).toBe(401)
    expect(fixture.messages).toEqual([])
  })

  it('does not let v2 viewer grants mutate save-state slots', async () => {
    const acl = new PrincipalAccessControl()
    acl.registerPrincipalToken('viewer-a', TOKEN)
    acl.grant('viewer-a', 'instance-1', 'viewer')
    const fixture = createFixture(new Map([[formatMessage('core.saveStateSlot', '2'), SUCCESS_MARKER]]), {
      includeV2: true,
      principalAcl: acl,
    })

    const response = await fixture.app.request('/api/v2/sessions/instance-1/core/savestateslot?slot=2', {
      method: 'POST',
      headers: { 'X-Principal-Token': TOKEN },
    })

    expect(response.status).toBe(401)
    expect(fixture.messages).toEqual([])
  })

  it('rejects protocol delimiter injection in query arguments before socket send', async () => {
    const fixture = createFixture(new Map(), { includeV2: true })

    const badButton = await fixture.app.request(
      '/api/v2/sessions/instance-1/mgba-http/button/tap?button=A%2Ccore.read8',
      { method: 'POST', headers: { 'X-Principal-Token': TOKEN } },
    )
    const badAddress = await fixture.app.request(
      '/api/v2/sessions/instance-1/core/read8?address=0xD35E%3C%7CEND%7C%3E',
      { headers: { 'X-Principal-Token': TOKEN } },
    )
    const badSlot = await fixture.app.request(
      '/api/v1/0123456789abcdef0123456789abcdef/core/savestateslot?slot=1%2Ccore.read8',
      { method: 'POST' },
    )

    expect(badButton.status).toBe(400)
    expect(badAddress.status).toBe(400)
    expect(badSlot.status).toBe(400)
    expect(fixture.messages).toEqual([])
  })

  it('returns PNG bytes for /core/screenshot after reading the bind-mounted capture file', async () => {
    const pngBytes = Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a])
    readFileMock.setResponse(pngBytes)

    const socketMessage = formatMessage('core.screenshot', '/capture/rest-capture.png')
    const fixture = createFixture(new Map([[socketMessage, SUCCESS_MARKER]]))

    const response = await fixture.app.request(
      '/api/v1/0123456789abcdef0123456789abcdef/core/screenshot',
      { method: 'POST' },
    )

    expect(response.status).toBe(200)
    expect(response.headers.get('content-type')).toBe('image/png')
    expect(Buffer.from(await response.arrayBuffer())).toEqual(pngBytes)
    expect(fixture.messages).toEqual([socketMessage])
    expect(readFileMock.calls).toEqual([
      'lstat:/tmp/grokemon-captures-test/instance-1',
      'lstat:/tmp/grokemon-captures-test/instance-1/rest-capture.png',
      expect.stringMatching(REST_CAPTURE_OPEN_CALL),
    ])
  })

  it('returns 500 when the bind-mounted capture file cannot be read', async () => {
    readFileMock.setFailure(new Error('read failed'))

    const socketMessage = formatMessage('core.screenshot', '/capture/rest-capture.png')
    const fixture = createFixture(new Map([[socketMessage, SUCCESS_MARKER]]))

    const response = await fixture.app.request(
      '/api/v1/0123456789abcdef0123456789abcdef/core/screenshot',
      { method: 'POST' },
    )

    expect(response.status).toBe(500)
    expect(await response.text()).toBe('Failed to read screenshot')
    expect(fixture.messages).toEqual([socketMessage])
  })
})

function createFixture(
  responses: Map<string, string>,
  options: {
    readonly fallbackToSingleInstance?: boolean
    readonly includeV2?: boolean
    readonly inputLog?: InputLogBus
    readonly principalAcl?: PrincipalAccessControl
  } = {},
): Fixture {
  const messages: string[] = []
  const client = new MgbaSocketClient()
  const send = vi.fn<(message: string) => Promise<string>>((message) => {
    messages.push(message)
    const response = responses.get(message)
    if (response === undefined) {
      return Promise.reject(new Error(`Unexpected message: ${message}`))
    }

    return Promise.resolve(response)
  })
  client.send = send

  const registry: InstanceRegistry = new Map([
    [
      TOKEN,
      {
        info: {
          id: 'instance-1',
          token: TOKEN,
          containerId: CONTAINER_ID,
          containerHost: '127.0.0.1',
          captureDirectory: '/tmp/grokemon-captures-test/instance-1',
          status: 'running',
          createdAt: new Date('2026-05-25T00:00:00Z'),
        },
        client,
      },
    ],
  ])

  const app = new Hono()
  if (options.includeV2) {
    app.route('/api/v2/sessions/:sessionId', createV2ApiRouter(registry, {
      inputLog: options.inputLog,
      principalAcl: options.principalAcl,
    }))
  }
  app.route('/api/v1/:token', createApiRouter(registry, { inputLog: options.inputLog }))
  if (options.fallbackToSingleInstance) {
    app.route('/', createApiRouter(registry, { fallbackToSingleInstance: true }))
  }

  return { app, messages }
}
