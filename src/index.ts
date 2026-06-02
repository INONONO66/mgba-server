import 'dotenv/config'
import { loadConfig } from './config.js'
import type { InstanceRegistry } from './gateway/ApiRouter.js'
import { createGatewayServer } from './gateway/GatewayServer.js'
import { InstanceManager } from './instances/InstanceManager.js'

const config = loadConfig()
if (config.adminToken === 'dev-admin-token') {
  console.warn('WARNING: Using default admin token. Set ADMIN_TOKEN env var for production.')
}

const registry: InstanceRegistry = new Map()
const instanceManager = new InstanceManager(config, registry)

const gateway = createGatewayServer(config, registry, instanceManager)
gateway.start()

instanceManager.reconstruct().catch((err: unknown) => {
  console.warn('Could not reconstruct instances from Docker (Docker may be unavailable):', err instanceof Error ? err.message : String(err))
}).then(() => {
  instanceManager.startHealthChecks()
})

process.on('SIGTERM', async () => {
  instanceManager.stopHealthChecks()
  await gateway.stop().catch((err: unknown) => {
    console.error('Error during shutdown:', err instanceof Error ? err.message : String(err))
  })
  process.exit(0)
})

export { gateway, instanceManager }
