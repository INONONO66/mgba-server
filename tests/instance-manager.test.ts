import { beforeEach, describe, expect, it, vi } from 'vitest'

import type { Config } from '../src/config.js'
import type { InstanceRegistry } from '../src/gateway/ApiRouter.js'
import { InstanceManager } from '../src/instances/InstanceManager.js'

const fsMock = vi.hoisted(() => ({
  lstat: vi.fn((value: string) => Promise.resolve({
    isDirectory: () => value.startsWith('/tmp/grokemon-captures-test/') && value !== '/tmp/grokemon-captures-test/',
  })),
  realpath: vi.fn((value: string) => Promise.resolve(value)),
  rm: vi.fn(() => Promise.resolve()),
}))

vi.mock('node:fs/promises', () => ({
  lstat: fsMock.lstat,
  realpath: fsMock.realpath,
  rm: fsMock.rm,
}))

interface CreateContainerOptions {
  image: string
  instanceId: string
  romPath?: string
  networkName: string
  emulatorPort: number
  emulatorMemoryBytes: number
  captureRoot: string
}

interface ContainerInfo {
  id: string
  host: string
  port: number
  captureDirectory: string
}

interface ManagedContainer {
  id: string
  instanceId: string
  host: string
  captureDirectory?: string
}

interface MgbaClientMock {
  connectCalls: Array<{ host: string; port: number }>
  disconnectCalls: number
  connected: boolean
  pingResponses: boolean[]
  connect(host: string, port: number): Promise<void>
  ping(): Promise<boolean>
  disconnect(): void
  isConnected(): boolean
}

const dockerMock = vi.hoisted(() => {
  const createdOptions: CreateContainerOptions[] = []
  const stoppedContainers: string[] = []
  const listedContainers: ManagedContainer[] = []
  const runningContainers = new Map<string, boolean>()
  const createResponses: ContainerInfo[] = []
  let stopError: Error | undefined

  class DockerDriver {
    createContainer(opts: CreateContainerOptions): Promise<ContainerInfo> {
      createdOptions.push(opts)
      return Promise.resolve(
        createResponses.shift() ?? {
          id: `container-${opts.instanceId}`,
          host: `grokemon-${opts.instanceId}`,
          port: opts.emulatorPort,
          captureDirectory: `${opts.captureRoot}/${opts.instanceId}`,
        },
      )
    }

    stopContainer(containerId: string): Promise<void> {
      stoppedContainers.push(containerId)
      if (stopError) {
        return Promise.reject(stopError)
      }
      return Promise.resolve()
    }

    listManagedContainers(): Promise<ManagedContainer[]> {
      return Promise.resolve([...listedContainers])
    }

    inspectContainer(containerId: string): Promise<{ running: boolean }> {
      return Promise.resolve({ running: runningContainers.get(containerId) ?? false })
    }
  }

  return {
    DockerDriver,
    createdOptions,
    stoppedContainers,
    listedContainers,
    runningContainers,
    createResponses,
    reset() {
      createdOptions.length = 0
      stoppedContainers.length = 0
      listedContainers.length = 0
      runningContainers.clear()
      createResponses.length = 0
      stopError = undefined
    },
    setStopError(error: Error) {
      stopError = error
    },
  }
})

const mgbaMock = vi.hoisted(() => {
  const clients: MgbaClientMock[] = []
  const defaultPingResponses: boolean[] = [true]

  class MgbaSocketClient implements MgbaClientMock {
    connectCalls: Array<{ host: string; port: number }> = []
    disconnectCalls = 0
    connected = false
    pingResponses: boolean[] = [...defaultPingResponses]

    constructor() {
      clients.push(this)
    }

    connect(host: string, port: number): Promise<void> {
      this.connectCalls.push({ host, port })
      this.connected = true
      return Promise.resolve()
    }

    ping(): Promise<boolean> {
      return Promise.resolve(this.pingResponses.shift() ?? true)
    }

    disconnect(): void {
      this.disconnectCalls += 1
      this.connected = false
    }

    isConnected(): boolean {
      return this.connected
    }
  }

  return {
    MgbaSocketClient,
    clients,
    defaultPingResponses,
    reset() {
      clients.length = 0
      defaultPingResponses.length = 0
      defaultPingResponses.push(true)
    },
  }
})

vi.mock('../src/instances/DockerDriver.js', () => ({
  DockerDriver: dockerMock.DockerDriver,
}))

vi.mock('../src/mgba/MgbaSocketClient.js', () => ({
  MgbaSocketClient: mgbaMock.MgbaSocketClient,
}))

describe('InstanceManager', () => {
  beforeEach(() => {
    dockerMock.reset()
    fsMock.lstat.mockClear()
    fsMock.realpath.mockClear()
    fsMock.rm.mockClear()
    mgbaMock.reset()
    vi.useRealTimers()
  })

  it('create() starts a Docker container, waits for mGBA, and adds the instance to the registry', async () => {
    const registry: InstanceRegistry = new Map()
    const manager = new InstanceManager(createConfig(), registry)

    const info = await manager.create('/rom/custom.gb')

    expect(info.status).toBe('running')
    expect(info.containerId).toBe(`container-${info.id}`)
    expect(dockerMock.createdOptions).toEqual([
      {
        image: 'grokemon-emulator',
        instanceId: info.id,
        romPath: '/rom/custom.gb',
        networkName: 'grokemon-net',
        emulatorPort: 8888,
        emulatorMemoryBytes: 805_306_368,
        captureRoot: '/tmp/grokemon-captures-test',
      },
    ])
    expect(mgbaMock.clients[0]?.connectCalls).toEqual([{ host: info.containerHost, port: 8888 }])
    expect(registry.get(info.principalToken)?.info).toBe(info)
    expect(manager.getByPrincipalToken(info.principalToken)).toBe(info)
  })

  it('cleans up the container and capture directory when socket readiness fails', async () => {
    vi.useFakeTimers()
    mgbaMock.defaultPingResponses.length = 0
    mgbaMock.defaultPingResponses.push(...Array.from({ length: 100 }, () => false))
    const registry: InstanceRegistry = new Map()
    const manager = new InstanceManager(createConfig(), registry)

    const createPromise = manager.create()
    const rejection = expect(createPromise).rejects.toThrow('Lua socket not ready')
    await vi.advanceTimersByTimeAsync(31_000)

    await rejection
    expect(dockerMock.stoppedContainers).toHaveLength(1)
    expect(fsMock.rm).toHaveBeenCalledWith(expect.stringContaining('/tmp/grokemon-captures-test/'), { force: true, recursive: true })
    expect(registry.size).toBe(0)
  })

  it('destroy() stops the container, disconnects the client, and removes the registry entry', async () => {
    const registry: InstanceRegistry = new Map()
    const manager = new InstanceManager(createConfig(), registry)
    const info = await manager.create()

    await manager.destroy(info.id)

    expect(dockerMock.stoppedContainers).toEqual([info.containerId])
    expect(mgbaMock.clients[0]?.disconnectCalls).toBe(1)
    expect(registry.has(info.principalToken)).toBe(false)
    expect(fsMock.rm).toHaveBeenCalledWith(info.captureDirectory, { force: true, recursive: true })
    expect(manager.get(info.id)).toBeUndefined()
  })

  it('keeps a failing destroy tracked and visible as an error', async () => {
    const registry: InstanceRegistry = new Map()
    const manager = new InstanceManager(createConfig(), registry)
    const info = await manager.create()
    dockerMock.setStopError(new Error('remove failed'))

    await expect(manager.destroy(info.id)).rejects.toThrow('remove failed')

    expect(manager.get(info.id)?.status).toBe('error')
    expect(registry.has(info.principalToken)).toBe(true)
    expect(mgbaMock.clients[0]?.disconnectCalls).toBe(0)
  })

  it('enforces the configured maximum instance count', async () => {
    const manager = new InstanceManager(createConfig(), new Map())

    for (let count = 0; count < 20; count += 1) {
      await manager.create()
    }

    await expect(manager.create()).rejects.toThrow('MAX_INSTANCES_REACHED')
  })

  it('counts pending creates when enforcing the maximum instance count', async () => {
    const registry: InstanceRegistry = new Map()
    const manager = new InstanceManager({ ...createConfig(), maxInstances: 1 }, registry)

    const [first, second] = await Promise.allSettled([manager.create(), manager.create()])

    expect(first.status).toBe('fulfilled')
    expect(second.status).toBe('rejected')
    expect((second as PromiseRejectedResult).reason).toMatchObject({ message: 'MAX_INSTANCES_REACHED' })
    expect(dockerMock.createdOptions).toHaveLength(1)
    expect(registry.size).toBe(1)
  })

  it('marks an instance as error when the health check ping fails', async () => {
    vi.useFakeTimers()
    const manager = new InstanceManager(createConfig(), new Map())
    const info = await manager.create()
    const client = mgbaMock.clients[0]
    if (!client) {
      throw new Error('expected mGBA client')
    }
    client.pingResponses.push(false)

    manager.startHealthChecks()
    await vi.advanceTimersByTimeAsync(10_000)

    expect(manager.get(info.id)?.status).toBe('error')
    manager.stopHealthChecks()
  })

  it('skips reconstructed containers with capture directories outside captureRoot', async () => {
    dockerMock.listedContainers.push({
      id: 'container-escaped',
      instanceId: 'instance-escaped',
      host: 'grokemon-instance-escaped',
      captureDirectory: '/tmp/outside/instance-escaped',
    })
    dockerMock.runningContainers.set('container-escaped', true)
    const registry: InstanceRegistry = new Map()
    const manager = new InstanceManager(createConfig(), registry)

    await manager.reconstruct()

    expect(manager.get('instance-escaped')).toBeUndefined()
    expect(dockerMock.stoppedContainers).toEqual(['container-escaped'])
    expect(registry.size).toBe(0)
    expect(mgbaMock.clients).toHaveLength(0)
  })


  it('stops managed containers that do not have a capture directory label', async () => {
    dockerMock.listedContainers.push({
      id: 'container-unlabeled',
      instanceId: 'instance-unlabeled',
      host: 'grokemon-instance-unlabeled',
    })
    dockerMock.runningContainers.set('container-unlabeled', true)
    const registry: InstanceRegistry = new Map()
    const manager = new InstanceManager(createConfig(), registry)

    await manager.reconstruct()

    expect(manager.get('instance-unlabeled')).toBeUndefined()
    expect(dockerMock.stoppedContainers).toEqual(['container-unlabeled'])
    expect(registry.size).toBe(0)
  })

  it('reconstructs running managed containers from Docker on startup', async () => {
    dockerMock.listedContainers.push({
      id: 'container-existing',
      instanceId: 'instance-existing',
      host: 'grokemon-instance-existing',
      captureDirectory: '/tmp/grokemon-captures-test/instance-existing',
    })
    dockerMock.runningContainers.set('container-existing', true)
    const registry: InstanceRegistry = new Map()
    const manager = new InstanceManager(createConfig(), registry)

    await manager.reconstruct()

    const info = manager.get('instance-existing')
    expect(info?.containerId).toBe('container-existing')
    expect(info?.principalToken).toEqual(expect.any(String))
    expect(info?.principalToken).not.toBe('')
    expect(info?.containerHost).toBe('grokemon-instance-existing')
    expect(info?.captureDirectory).toBe('/tmp/grokemon-captures-test/instance-existing')
    expect(info?.status).toBe('running')
    expect(registry.get(info?.principalToken ?? '')?.info).toBe(info)
    expect(mgbaMock.clients[0]?.connectCalls).toEqual([{ host: 'grokemon-instance-existing', port: 8888 }])
  })
})

function createConfig(): Config {
  return {
    port: 8787,
    adminToken: 'admin-token',
    maxInstances: 20,
    emulatorImage: 'grokemon-emulator',
    emulatorPort: 8888,
    emulatorMemoryBytes: 805_306_368,
    captureIntervalMs: 16,
    sourceCaptureIntervalMs: 250,
    captureRoot: '/tmp/grokemon-captures-test',
    streamKeyframeInterval: 60,
    streamTileSize: 16,
    wsBackpressureLimit: 262_144,
    networkName: 'grokemon-net',
    romPath: '/rom/default.gb',
  }
}
