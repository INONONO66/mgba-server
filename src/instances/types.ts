type InstanceStatus = 'starting' | 'running' | 'stopped' | 'error'

export interface InstanceInfo {
  id: string
  principalToken: string
  containerId: string
  containerHost: string
  captureDirectory: string
  status: InstanceStatus
  createdAt: Date
}
